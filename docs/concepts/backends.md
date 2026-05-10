# Backends

A backend is the unit of execution: the thing that actually runs an
agent. Murmur ships two from MVP and treats them both as first-class.

```python
class Backend(Protocol):
    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> AgentHandle: ...

    async def kill(self, handle: AgentHandle) -> None: ...
    async def status(self, handle: AgentHandle) -> BackendStatus: ...
    async def result(self, handle: AgentHandle) -> AgentResult: ...
```

Both backends pass the same `BackendContract` test suite.

## AsyncBackend

The default. Uses `asyncio.create_task`. Lightweight, zero-config, no
external services required.

```python
from murmur import AgentRuntime

runtime = AgentRuntime()                  # AsyncBackend
```

Use it for:

- Local development.
- Single-host workloads where one process is enough.
- Embedded mode — mounting Murmur inside a user-supplied FastAPI app.
- The publisher side of distributed mode (the publisher's runtime is
  in-process; only the *worker* uses `JobBackend` semantics on the wire).

## JobBackend

Broker-backed. Uses FastStream subscribers + publishers under the hood.
Activates automatically when you pass a broker URL to `AgentRuntime`:

```python
runtime = AgentRuntime(broker="kafka://localhost:9092")
```

Supported URL schemes:

| Scheme | Broker | Extra |
|---|---|---|
| `kafka://` | Apache Kafka | `murmur-ai[kafka]` |
| `nats://` | NATS | `murmur-ai[nats]` |
| `amqp://` | RabbitMQ | `murmur-ai[rabbitmq]` |
| `redis://` | Redis Streams | `murmur-ai[redis]` |
| `memory://` | In-process broker | bundled — for tests |

Mental model: **`JobBackend` is a transport for `AsyncBackend`
invocations across machines.** A `Worker` consumes `TaskMessage`
envelopes off the broker, dispatches them through its own internal
in-process runtime, and publishes the `ResultMessage` envelope back on
the agent's results topic. The publisher correlates the response by
`batch_id` via `ResultCollector`.

See the [Distributed deployments guide](../guides/distributed.md) for
production patterns.

## Wire envelope

`ResultMessage` carries primitive fields (`success: bool`,
`output_payload: dict`, `error_message: str`) — **not** a nested
`AgentResult[BaseModel]`. Generic `BaseModel` can't be Pydantic-deserialised
on the wire; `JobBackend._msg_to_result` rehydrates against the agent's
declared `output_type` after receipt.

## Failure semantics

- Pre-spawn errors (registry miss, validation failure) raise synchronously.
- Spawn errors (timeout, broker connection failure) wrap in `SpawnError`.
- Tool errors during the run wrap in `ToolExecutionError`.
- Budget exhaustion wraps in `BudgetExceededError`.
- All errors derive from `MurmurError`.

`gather` catches per-slot errors into the slot's `AgentResult` so a
partial batch doesn't fail the whole call.

## `spawn_agents` and the runtime-binding gotcha

The [`spawn_agents`](agents.md#llm-driven-fan-out-with-spawn_agents)
tool dispatches children via **whichever runtime you bound at factory
time**. This matters in distributed mode:

| Bound runtime | Child dispatch path |
|---|---|
| `AgentRuntime()` (in-process) | Children run in-process via `asyncio.create_task` — no broker hop. |
| `AgentRuntime(broker="kafka://...")` (`JobBackend`) | Each child publishes a `TaskMessage` to the broker; some worker in the fleet consumes it; result correlates back via `ResultCollector`. Fleet-load-balanced. |

A `Worker` consuming an orchestrator task uses a in-process runtime
internally — deliberately, to avoid republish loops on every native
`runtime.run` call. So if you register `spawn_agents` against the
worker's internal runtime, **children run in-process inside that
worker** — they share its CPU, they don't fan out across the fleet.

To get one-broker-job-per-child from inside a worker, construct a
**second**, broker-backed `AgentRuntime` in the worker process and bind
the spawn_agents factory to *that*. Children round-trip through the
broker like any top-level call. There is a real loop risk if the
children's template surface also includes `spawn_agents` — keep the
tool on the orchestrator's per-agent set only, not on the template.

Cascading-spawn cycle detection is queued; until it lands, this is the
operator's responsibility.

## Backends not yet shipped

- **`ProcessBackend`** — `ProcessPoolExecutor`. CPU isolation. Backlog;
  ship when a real workload requires it.

For untrusted-context concerns (sub-agents processing potentially
hostile external data), the planned mitigation is per-tool sandboxing
plus a `DenylistToolProvider` rather than per-agent container isolation
— sandbox the *tool* (e.g. a code-interpreter tool wired to a hosted
sandbox), not the whole agent loop.
