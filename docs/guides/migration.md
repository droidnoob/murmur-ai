# Migration

Wrappers in `murmur.interop` let existing PydanticAI agents and
FastStream subscribers move into Murmur incrementally — you don't have
to rewrite your fleet to start using Murmur orchestration.

## From PydanticAI

If you already have a `pydantic_ai.Agent`, wrap it:

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

The returned `murmur.Agent` is a fully-fledged Murmur agent — you can
add `mcp_servers=`, `trust_level=`, hand it to `AgentRuntime.run`, ship
it to a worker, etc. The PydanticAI agent is the source of truth for
model + instructions + output_type; Murmur owns orchestration.

> **`murmur.interop` is the only place in Murmur allowed to import
> `pydantic_ai`.** Everywhere else, the public API rule applies — users
> never import from `pydantic_ai`. See [Architecture](../concepts/architecture.md#public-api-rule).

## From FastStream

If you already have a `faststream` subscriber and want to expose it as a
Murmur-runnable agent:

```python
from murmur.interop import as_faststream_handler

handler = as_faststream_handler(agent, runtime=runtime)
broker.subscriber("research-tasks")(handler)
```

Or, more often: replace the FastStream subscriber with a Murmur
`Worker`. The Worker is FastStream under the hood — same delivery
guarantees, same broker semantics — but with the policy gate, lifecycle
hooks, and registry integration Murmur provides.

```python
from murmur.worker import Worker

worker = Worker(
    runtime=runtime,
    broker="kafka://localhost:9092",
    agents=("research-minion",),
    concurrency=20,
)
await worker.start()
```

## Migrating broker imports

```python
# Before — direct FastStream
from faststream.kafka import KafkaBroker
broker = KafkaBroker("localhost:9092")

# After — Murmur runtime parses the URL internally
from murmur import AgentRuntime
runtime = AgentRuntime(broker="kafka://localhost:9092")
```

## What does NOT change

- Model strings (`"anthropic:claude-sonnet-4-6"`) — Murmur passes them
  through to PydanticAI.
- Output schemas — Murmur reuses your Pydantic models verbatim.
- Tool definitions — wrap any `async def` and register it; PydanticAI's
  schema introspection still works because Murmur preserves
  `__signature__` via `functools.wraps` (decision D2).
- Broker semantics — at-least-once, ordering guarantees, partition keys
  — are FastStream's; Murmur doesn't change them.
- Provider auth — `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` etc. resolved
  the same way as PydanticAI.

## Incremental adoption

A reasonable migration path:

1. **Wrap a single agent** with `from_pydantic_ai`. Run it with
   `AgentRuntime()` locally. Confirm output schemas and tool calls
   behave identically.
2. **Add the trust gate.** Set `trust_level=` on the wrapped agent;
   audit which tools fire under each level.
3. **Move to broker mode.** Swap `AgentRuntime()` for
   `AgentRuntime(broker="…")`. Run a single worker process. Confirm
   the wire envelope behaves correctly under failure.
4. **Add observability.** Wire `LogEventEmitter` (default — already on)
   plus `SSEEventEmitter` if you want a live dashboard.
5. **Add cost tracking.** Set `RuntimeOptions(token_budget=…)` once you
   have a target spend ceiling.
6. **Rewrite hand-rolled coordination** as `AgentGroup` topology if it
   already lives in your codebase. The Phase 3 workflow engine adds the
   YAML form.

Each step is independently revertable; nothing is all-or-nothing.
