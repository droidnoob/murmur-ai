# Migrating from raw asyncio

Most agent codebases start as ad-hoc `asyncio.gather` over LLM calls with
a hand-rolled retry / timeout / cost layer bolted on. Murmur replaces the
scaffolding with primitives that compose cleanly — same `asyncio` under
the hood, no new runtime to learn.

## Why migrate

Hand-rolled patterns that Murmur's pipeline replaces:

| Hand-rolled pattern | Murmur primitive |
|---|---|
| `asyncio.gather(*coros)` with manual error squashing | `runtime.gather(...)` returning `list[AgentResult]` — partial failures aggregate cleanly |
| `try / except` retry loops | `RuntimeOptions(retry_max_attempts=N)` + `RetryMiddleware` |
| `async with asyncio.timeout(N): ...` per call | `RuntimeOptions(timeout_seconds=N)` + `TimeoutMiddleware` |
| Token bookkeeping in a global dict | `RuntimeOptions(token_budget=TokenBudget(limit=N))` + `CostTrackingMiddleware` |
| Custom log lines for every spawn | `LogEventEmitter` (always-on default) emitting typed `RuntimeEvent`s |
| Manual fan-out coordination | `AgentGroup` + `Edge` topology |
| "Don't recurse beyond N levels" comments | `RuntimeOptions(max_spawn_depth=N)` + `DepthLimitMiddleware` |

You're not adopting a new runtime — you're consolidating scaffolding you
already have, with the bonus that the same code now also runs distributed
when you point it at a broker URL.

## Cookbook

### Replace `asyncio.gather` with `runtime.gather`

Before:

```python
import asyncio


async def call_one(agent_fn, q):
    try:
        return {"ok": True, "result": await agent_fn(q)}
    except Exception as e:
        return {"ok": False, "error": e}


results = await asyncio.gather(
    *(call_one(my_agent, q) for q in questions),
    return_exceptions=False,
)

ok = [r["result"] for r in results if r["ok"]]
failed = [r["error"] for r in results if not r["ok"]]
```

After:

```python
from murmur import AgentRuntime, TaskSpec

runtime = AgentRuntime()

results = await runtime.gather(
    my_agent,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=20,
)

ok = [r.output for r in results if r.is_ok()]
failed = [r.error for r in results if not r.is_ok()]
```

`AgentResult.is_ok()` does the discrimination; per-slot exceptions stay
out of `asyncio.gather`'s `return_exceptions` semantics. `max_concurrency`
caps fan-out without `asyncio.Semaphore` plumbing.

### Replace retry / timeout boilerplate with middleware

Before:

```python
async def with_retry(coro_fn, *args, attempts=3, backoff=1.5):
    for i in range(attempts):
        try:
            async with asyncio.timeout(60):
                return await coro_fn(*args)
        except (TimeoutError, ConnectionError):
            if i + 1 == attempts:
                raise
            await asyncio.sleep(backoff ** i)


result = await with_retry(my_agent, question, attempts=3)
```

After:

```python
from murmur import AgentRuntime, RuntimeOptions

runtime = AgentRuntime(
    options=RuntimeOptions(
        timeout_seconds=60.0,
        retry_max_attempts=3,
        retry_backoff_factor=1.5,
    ),
)

result = await runtime.run(my_agent, TaskSpec(input=question))
```

Both `RetryMiddleware` and `TimeoutMiddleware` are pipeline `Stage`s; the
options are exposed as `RuntimeOptions` knobs so you don't construct
middleware directly.

### Replace global token bookkeeping with TokenBudget

Before:

```python
TOTAL_TOKENS = 0


async def call_with_tracking(agent_fn, q):
    global TOTAL_TOKENS
    if TOTAL_TOKENS >= TOKEN_LIMIT:
        raise BudgetExhausted()
    res = await agent_fn(q)
    TOTAL_TOKENS += res.usage.total_tokens
    return res
```

After:

```python
from murmur import AgentRuntime, RuntimeOptions
from murmur.middleware.cost_tracking import TokenBudget

runtime = AgentRuntime(
    options=RuntimeOptions(token_budget=TokenBudget(limit=TOKEN_LIMIT)),
)

# CostTrackingMiddleware does pre-check (raises BudgetExceededError) +
# post-charge (deducts from the budget) automatically. A BUDGET_EXCEEDED
# RuntimeEvent fires before the error is raised.
```

### Replace ad-hoc DAG runners with AgentGroup

Before:

```python
async def research_pipeline(question):
    finding = await researcher(question)
    review = await reviewer(finding)
    summary = await summariser(review)
    return summary
```

After:

```python
from murmur import AgentGroup, Edge

crew = AgentGroup(
    name="research",
    topology={
        researcher: Edge(to=(reviewer,)),
        reviewer:   Edge(to=(summariser,)),
        summariser: Edge.terminal(),
    },
)

result = await runtime.run_group(crew, TaskSpec(input=question))
```

You get cycle detection, fan-out via `FanOut`-annotated output fields,
conditional edges with predicates, and the same observability as the
single-agent path.

### Going distributed

The biggest win. Once your code is on `runtime.run` / `runtime.gather`,
swapping the constructor is the only change to fan out across machines:

```python
runtime = AgentRuntime(broker="kafka://kafka.prod:9092")
```

Then start a worker process:

```bash
murmur worker start --agents researcher --broker kafka://kafka.prod:9092 --concurrency 20
```

No agent code changes. The agent doesn't know it moved.

## What does *not* change

- **Your model code.** Whether you're using PydanticAI directly,
  Anthropic's SDK, OpenAI's SDK, or your own client — Murmur dispatches
  via PydanticAI under the hood and accepts the same model strings.
- **Your async style.** Murmur is `asyncio` end-to-end; `await runtime.run(...)`
  composes with everything you already do.
- **Your existing logging / metrics.** `LogEventEmitter` writes through
  `structlog`; if you've already configured structlog, events appear in
  the same sink.
- **Your secrets.** Provider auth resolves via env vars the same way as
  PydanticAI / your existing SDK.

## Incremental adoption path

1. **Wrap one agent.** Use `from_pydantic_ai` (if you already have a
   `pydantic_ai.Agent`) or build a fresh `murmur.Agent`. Run it with
   `runtime.run` against a single task.
2. **Replace `asyncio.gather`** with `runtime.gather` for one fan-out
   site. Confirm partial-failure aggregation matches what you had.
3. **Add `RuntimeOptions`** for retry, timeout, depth limit. Delete the
   hand-rolled scaffolding around the agent.
4. **Add `TokenBudget`.** Delete the global counter.
5. **Wire `MultiEventEmitter([LogEventEmitter(), SSEEventEmitter(...)])`**
   for observability.
6. **Convert one DAG** to `AgentGroup` + `Edge` topology.
7. **Swap to broker mode** by changing `AgentRuntime()` to
   `AgentRuntime(broker="…")`. Start a `Worker`.

## See also

- [Migrating from PydanticAI](migration-pydantic-ai.md)
- [Migrating from FastStream](migration-faststream.md)
- [Architecture](../concepts/architecture.md)
- [Runtime API](../api/runtime.md)
