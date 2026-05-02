# Runtime

`AgentRuntime` is the entry point for executing agents. It composes the
pipeline, owns the backend, and routes events.

## Construction

```python
from murmur import AgentRuntime

# Local — uses ThreadBackend (asyncio)
runtime = AgentRuntime()

# Distributed — uses JobBackend over the parsed broker
runtime = AgentRuntime(broker="kafka://localhost:9092")
runtime = AgentRuntime(broker="nats://localhost:4222")
runtime = AgentRuntime(broker="amqp://localhost:5672")     # RabbitMQ
runtime = AgentRuntime(broker="redis://localhost:6379")
runtime = AgentRuntime(broker="memory://")                  # in-process, for tests
```

## `run` — single agent

```python
result = await runtime.run(agent, TaskSpec(input="..."))

if result.is_ok():
    print(result.output)        # Pydantic model — agent.output_type
else:
    print(result.error)         # SpawnError, ToolExecutionError, BudgetExceededError, ...
```

`agent` accepts an `Agent` instance or a registry name (string). The
registry is consulted when a string is passed.

## `gather` — fan-out

```python
results = await runtime.gather(
    agent,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)
```

Bounded concurrency. Partial failures don't take the batch down — every
slot returns its own `AgentResult`. Pre-spawn validation errors raise
synchronously; post-spawn errors are caught into the per-slot result.

`gather` deliberately bypasses the middleware pipeline (per-slot path
calls `backend.gather` directly) — see CLAUDE.md decision D12.

## `run_group` — DAG

```python
from murmur import AgentGroup, Edge

crew = AgentGroup(
    name="research-crew",
    topology={
        researcher: Edge(to=(reviewer,)),
        reviewer:   Edge(to=(summariser,)),
    },
)

result = await runtime.run_group(crew, TaskSpec(input="..."))
```

The runner walks the topology, resolves mappers or `FanOut`, and reuses
the same `runtime.run` and `runtime.gather` for each node. There is no
separate code path. Conditional edges (`Edge(condition=lambda out: ...)`)
and multi-input aggregation are supported. Cycles are rejected at
group-construction time.

## `RuntimeOptions`

Conservative defaults; tweak via the constructor:

```python
from murmur import AgentRuntime, RuntimeOptions
from murmur.middleware.cost_tracking import TokenBudget

runtime = AgentRuntime(
    options=RuntimeOptions(
        timeout_seconds=300,
        retry_attempts=0,
        depth_limit=4,
        token_budget=TokenBudget(limit=1_000_000),
        mcp_eager_start=False,
    ),
)
```

| Field | Default | Effect |
|---|---|---|
| `timeout_seconds` | `300` | Per-call wall clock cap. |
| `retry_attempts` | `0` | Retries on `SpawnError`. Off by default. |
| `depth_limit` | `4` | Max cascading spawn depth. |
| `token_budget` | `None` | If set, wires `CostTrackingMiddleware`. See [Cost](cost.md). |
| `mcp_eager_start` | `False` | If set, holds MCP subprocesses warm across runs. See [MCP](mcp.md). |

## `publish_events` — distributed observability

```python
runtime = AgentRuntime(
    broker="kafka://localhost:9092",
    publish_events=True,
)
```

When set, the runtime subscribes to `murmur.events.{runtime_id}` and
relays worker-side per-agent / per-tool events back to its local
emitter. Useful for centralised dashboards. Without it, the publisher
sees only `BATCH_*` / `GROUP_*` / `AGENT_DISPATCHED`. See
[Events](events.md#distributed-event-bridge).

## `shutdown`

Releases MCP subprocesses (when `mcp_eager_start=True`) and broker
connections. `AgentRouter` and `AgentServer` lifespans call this
automatically; for plain runtimes, call it on shutdown.

```python
try:
    await runtime.run(agent, task)
finally:
    await runtime.shutdown()
```

## `run_sync` and friends

For notebooks, scripts, and the REPL, sync entry points wrap each async
method with `asyncio.run`:

```python
result = runtime.run_sync(agent, TaskSpec(input="..."))
```

`run_sync` raises if called from inside a running event loop — use the
async form there.
