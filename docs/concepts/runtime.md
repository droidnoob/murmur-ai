# Runtime

`AgentRuntime` is the entry point for executing agents. It composes the
pipeline, owns the backend, and routes events.

## Construction

```python
from murmur import AgentRuntime

# Local — uses AsyncBackend (asyncio)
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
calls `backend.gather` directly).

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

## Optional: uvloop for the asyncio loop

The CLI commands that own the loop (`murmur serve`, `murmur worker
start`) accept `--uvloop` to swap stdlib's asyncio for
[`uvloop`](https://github.com/MagicStack/uvloop) — a Cython wrapper
around libuv that's typically 2-4× faster on the scheduler hot path:

```bash
pip install 'murmur-ai[uvloop]'    # POSIX only — no Windows wheels
murmur serve --uvloop --port 8420
murmur worker start --agents researcher --broker redis://… --uvloop
# or fleet-wide via env:
MURMUR_USE_UVLOOP=1 murmur serve
```

Falls back to the default loop with a stderr warning when the extra
isn't installed or the platform is Windows. Real-world speedup is
workload-dependent: single-digit % for typical agent runs (httpx +
provider call dominate), larger only with high-concurrency `gather`
workloads where the asyncio scheduler is actually a bottleneck.

For your own programs that own `asyncio.run`, the canonical pattern
sets the policy before the loop starts. Murmur deliberately doesn't
touch the policy on `import murmur` — the runtime is a library, not
a process owner.

```python
import asyncio

try:
    import uvloop
except ImportError:
    pass
else:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

asyncio.run(main())
```

## Worked example — middleware order in practice

A run flows through `Pipeline` stages in this order, outside-in:

```
Timeout → DepthLimit → CostTracking? → Retry? → dispatch_stage(backend.spawn)
```

`Timeout` wraps the whole chain so a stuck child cancels regardless of
which inner stage is mid-flight. `DepthLimit` rejects recursive spawns
that would push past `RuntimeOptions.depth_limit`. `CostTracking` only
appears when `RuntimeOptions.token_budget` is set; `Retry` only when
`RuntimeOptions.retry_attempts > 1`. `dispatch_stage` is terminal — it
calls `backend.spawn` and returns the result.

`gather` deliberately bypasses this pipeline (the per-slot path calls
`backend.gather` directly). Per-slot retries / timeouts aren't applied
— if you need them, build a single-task helper that goes through
`runtime.run` and parallelise that.

## Worked example — lifecycle observation

Wire an emitter and watch the events fire as a run progresses:

```python
from murmur import Agent, AgentRuntime, TaskSpec
from murmur.events import (
    LogEventEmitter,
    MultiEventEmitter,
    RuntimeEvent,
    SSEEventEmitter,
)

sse = SSEEventEmitter(heartbeat_interval=15.0)
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)

# In one task, drive the agent:
async def driver():
    await runtime.run(agent, TaskSpec(input="..."))

# In another, consume the SSE stream (e.g. inside a Starlette endpoint):
async def watch():
    async for event in sse.subscribe():
        print(event.event_type.value, event.payload)
```

Per single `runtime.run`, you'll see roughly:

```
agent_spawned    {agent_name: 'researcher', backend: 'AsyncBackend', ...}
tool_call_started   {tool_name: 'web_search', ...}
tool_call_completed {tool_name: 'web_search', duration_ms: 412, ...}
agent_completed  {agent_name: 'researcher', tokens_used: 1842, ...}
```

`gather` wraps the slots in `BATCH_STARTED` / `BATCH_COMPLETED`, and
`run_group` adds `GROUP_STARTED` / `GROUP_COMPLETED`. See
[Events](events.md) for the full type list and emitter wiring.
