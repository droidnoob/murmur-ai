# Migrating from PydanticAI

Wrappers in [`murmur.interop`](../api/interop.md) let existing PydanticAI
agents move into Murmur incrementally — you don't have to rewrite your
fleet to start using Murmur orchestration.

## Why migrate

PydanticAI handles single-agent execution well. What it doesn't give you:

- **Trust enforcement.** PydanticAI's tool model is "if registered, the
  agent can call it." Murmur adds `TrustLevel` (HIGH / MEDIUM / LOW /
  SANDBOX), MCP allow-listing, and runtime-side enforcement.
- **Observable lifecycle.** Every spawn, tool call, and completion in
  Murmur emits a typed `RuntimeEvent`. PydanticAI logs through its own
  channels but doesn't ship a swappable emitter Protocol.
- **Distributed dispatch.** `AgentRuntime(broker="kafka://…")` swaps
  AsyncBackend for JobBackend with no code changes to the agent.
  PydanticAI is in-process only.
- **Structured fan-out.** `runtime.gather`, `AgentGroup`, `Edge` topology
  with mappers and `FanOut` typing — primitives PydanticAI doesn't ship.
- **Cost ceilings.** `RuntimeOptions(token_budget=…)` enforces hard caps
  with pre-check + post-charge semantics.

Interop is **permanent**, not a one-way trip. You can keep
`pydantic_ai.Agent` instances in your codebase forever and wrap them at
the dispatch boundary.

## Cookbook

### Wrap an existing PydanticAI agent

```python
from pydantic_ai import Agent as PaAgent
from murmur.interop import from_pydantic_ai

pa_agent = PaAgent(
    "anthropic:claude-sonnet-4-6",
    system_prompt="You are a research minion ...",
    output_type=ResearchFinding,
)

agent = from_pydantic_ai(pa_agent, name="research-minion")
```

The returned `murmur.Agent` is a fully-fledged Murmur agent — you can add
`mcp_servers=`, `trust_level=`, hand it to `AgentRuntime.run`, ship it to
a worker, etc. The PydanticAI agent is the source of truth for model +
instructions + output_type; Murmur owns orchestration.

> **`murmur.interop` is the only place in Murmur allowed to import
> `pydantic_ai`.** Everywhere else, the public API rule applies — users
> never import from `pydantic_ai`.

### Add the trust gate

```python
from murmur.types import TrustLevel

agent = from_pydantic_ai(pa_agent, name="research-minion").model_copy(
    update={"trust_level": TrustLevel.LOW},
)
```

Once `trust_level` is set, every tool call flows through `ToolExecutor`'s
gate. For `LOW`, MCP servers must declare `allow=[…]` per server — see
[MCP](../concepts/mcp.md#trust-matrix).

### Add observability

```python
from murmur import AgentRuntime
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

sse = SSEEventEmitter(heartbeat_interval=15.0)
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)

result = await runtime.run(agent, TaskSpec(input="..."))
# Hand sse.subscribe() to a FastAPI EventSourceResponse for live frames.
```

`LogEventEmitter` is on by default (forwards to `structlog`); adding the
SSE emitter gives you a streamable event source without touching the
agent.

### Add cost tracking

```python
from murmur import AgentRuntime, RuntimeOptions
from murmur.middleware.cost_tracking import TokenBudget

runtime = AgentRuntime(
    options=RuntimeOptions(token_budget=TokenBudget(limit=1_000_000)),
)
```

Every `runtime.run` charges tokens against the budget; `BudgetExceededError`
fires before dispatch once exhausted, with a `BUDGET_EXCEEDED`
`RuntimeEvent` emitted first.

## What does *not* change

- Model strings (`"anthropic:claude-sonnet-4-6"`) — Murmur passes them
  through to PydanticAI.
- Output schemas — Murmur reuses your Pydantic models verbatim.
- Tool definitions — wrap any `async def` and register it; PydanticAI's
  schema introspection still works because Murmur preserves
  `__signature__` via `functools.wraps`.
- Provider auth — `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, etc. resolved
  the same way as PydanticAI.
- `agent.run_stream` semantics — once `runtime.run_stream` ships, the
  underlying PydanticAI streaming primitives carry through.

## Incremental adoption path

1. **Wrap one agent.** Use `from_pydantic_ai`, run with `AgentRuntime()`
   locally, confirm output schemas + tool calls behave identically.
2. **Add trust + observability.** Set `trust_level=` and wire a
   `MultiEventEmitter`.
3. **Add cost tracking** with `RuntimeOptions(token_budget=…)`.
4. **Move to broker mode.** Swap `AgentRuntime()` for
   `AgentRuntime(broker="kafka://…")` and run a worker process.
5. **Coordinate.** Replace hand-rolled fan-out / sequencing with
   `AgentGroup` + `Edge` topology.

Each step is independently revertable; nothing is all-or-nothing.

## See also

- [Migrating from FastStream](migration-faststream.md)
- [Migrating from raw asyncio](migration-asyncio.md)
- [`murmur.interop` API reference](../api/interop.md)
