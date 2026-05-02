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

## ThreadBackend

The default. Uses `asyncio.create_task`. Lightweight, zero-config, no
external services required.

```python
from murmur import AgentRuntime

runtime = AgentRuntime()                  # ThreadBackend
```

Use it for:

- Local development.
- Single-host workloads where one process is enough.
- Embedded mode — mounting Murmur inside a user-supplied FastAPI app.
- The publisher side of distributed mode (the publisher's runtime is
  thread-mode; only the *worker* uses `JobBackend` semantics on the wire).

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

Mental model: **`JobBackend` is a transport for `ThreadBackend`
invocations across machines.** A `Worker` consumes `TaskMessage`
envelopes off the broker, dispatches them through its own internal
thread-mode runtime, and publishes the `ResultMessage` envelope back on
the agent's results topic. The publisher correlates the response by
`batch_id` via `ResultCollector`.

See the [Distributed deployments guide](../guides/distributed.md) for
production patterns.

## Wire envelope

`ResultMessage` carries primitive fields (`success: bool`,
`output_payload: dict`, `error_message: str`) — **not** a nested
`AgentResult[BaseModel]`. Generic `BaseModel` can't be Pydantic-deserialised
on the wire; `JobBackend._msg_to_result` rehydrates against the agent's
declared `output_type` after receipt. Decision D15.

## Failure semantics

- Pre-spawn errors (registry miss, validation failure) raise synchronously.
- Spawn errors (timeout, broker connection failure) wrap in `SpawnError`.
- Tool errors during the run wrap in `ToolExecutionError`.
- Budget exhaustion wraps in `BudgetExceededError`.
- All errors derive from `MurmurError`.

`gather` catches per-slot errors into the slot's `AgentResult` so a
partial batch doesn't fail the whole call.

## Backends not yet shipped

- **`ProcessBackend`** — `ProcessPoolExecutor`. CPU isolation. Backlog;
  ship when a real workload requires it.
- **`ContainerBackend`** — Docker SDK. Full isolation for untrusted
  context. Phase 4 (issue `murmur-ai-09g`).
