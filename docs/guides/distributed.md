# Distributed deployments

When one machine isn't enough, swap `AgentRuntime()` for
`AgentRuntime(broker="…")` and run a fleet of workers. The agent
definition doesn't change. The workflow doesn't change. Only the
runtime constructor does.

## Bootstrap

```bash
uv init my-murmur-app
cd my-murmur-app
uv add 'murmur-ai[kafka]'        # or [nats] / [rabbitmq] / [redis]
export ANTHROPIC_API_KEY=...
```

A self-contained run that uses an in-process broker (no external
services) lives at [`examples/distributed.py`](https://github.com/murmur-ai/murmur/blob/main/examples/distributed.py)
— same agent code, same wire envelope as a real Kafka deployment, just
the broker swapped out. Drop it into your project and run with
`uv run python distributed.py`.

## Wire shape

```
┌─────────────┐    publish     ┌──────────┐   subscribe    ┌─────────────┐
│  Publisher  │ ─────────────→ │  Broker  │ ─────────────→ │   Worker    │
│  (your app) │                │ (Kafka,  │                │ (`murmur     │
│             │ ←───────────── │  NATS,   │ ←───────────── │  worker     │
│             │     reply      │  …)      │     reply      │  start`)    │
└─────────────┘                └──────────┘                └─────────────┘
```

Per-agent task topics carry `TaskMessage` envelopes. A single
`{agent}.results` reply topic carries `ResultMessage` envelopes back,
correlated to the publisher's outstanding spawns by `batch_id`.

## Publisher

```python
from murmur import AgentRuntime, TaskSpec

runtime = AgentRuntime(broker="kafka://kafka.prod:9092")
results = await runtime.gather(researcher, tasks=tasks, max_concurrency=200)
```

The publisher's runtime is in-process internally — `JobBackend` handles
the publish + collect. Worker republish loops are avoided by routing
through `ResultCollector`.

## Workers

Programmatic:

```python
from murmur import AgentRuntime
from murmur.worker import Worker

runtime = AgentRuntime()                      # in-process, intentionally
worker = Worker(
    runtime=runtime,
    broker="kafka://kafka.prod:9092",
    agents=("researcher",),
    concurrency=20,
    prefetch=5,
)

await worker.start()
```

CLI:

```bash
murmur worker start \
    --agents researcher,reviewer \
    --broker kafka://kafka.prod:9092 \
    --concurrency 20

# or auto-discover from a registry directory:
murmur worker start \
    --all-from ./specs \
    --broker kafka://kafka.prod:9092
```

For local development, add `--reload` to auto-restart the worker on file
changes (uses `watchfiles`, same library as FastStream + uvicorn —
install via `uv add 'murmur-ai[reload]'`):

```bash
murmur worker start \
    --agents researcher \
    --broker memory:// \
    --reload \
    --reload-dir ./specs --reload-dir ./src
```

Defaults watch `*.py`, `*.yaml`, `*.yml` under the listed paths.
**Don't use `--reload` in production** — restarting workers on file
changes mid-task causes message redelivery.

`Worker.start()` prints a multi-line Murmur banner to stderr with
broker, runtime id, agents, per-agent topics, and concurrency. The
banner uses `sys.stderr` directly so the layout survives structlog
rendering.

## Scaling out — multiple workers compete for tasks

Pointing several `Worker` processes at the same broker URL gives you
horizontal fan-out: each `TaskMessage` is delivered to **exactly one**
worker in the fleet, not broadcast to all of them. Concretely, each
worker subscribes through its broker's competing-consumer primitive —
Redis Streams consumer group, Kafka consumer `group_id`, NATS queue
group, or a shared RabbitMQ queue — keyed by the agent's task topic.
The key is identical across every Worker serving the same agent, so the
broker pools them automatically; spinning up another process is the
only knob you need to turn.

```python
# Three workers, same broker, same agents — the broker load-balances.
for _ in range(3):
    Worker(
        broker=broker,
        agents={"researcher": researcher},
        concurrency=4,
    ).start()
```

Per-Worker `concurrency=` still caps how many tasks one process pulls
in parallel; effective fleet parallelism is `num_workers × concurrency`.
A worker process that dies mid-task is recovered by the broker's
redelivery semantics — another worker eventually picks the task up
(the agent author owns idempotency).

## Lifecycle hooks

```python
@worker.on_task_start
async def on_start(task_id: str, agent_name: str) -> None:
    metrics.task_started.inc(agent=agent_name)


@worker.on_task_complete
async def on_complete(task_id: str, agent_name: str, duration_ms: int) -> None:
    metrics.task_completed.observe(agent=agent_name, duration=duration_ms)


@worker.on_task_error
async def on_error(task_id: str, agent_name: str, error: Exception) -> None:
    metrics.task_failed.inc(agent=agent_name, error_type=type(error).__name__)
```

## Centralised observability

By default, the publisher sees only `BATCH_*` / `GROUP_*` /
`AGENT_DISPATCHED` events. Per-agent and per-tool events fire on the
worker process — typically the right model for log-aggregation pipelines
where both processes ship to the same sink.

For a centralised dashboard, opt in:

```python
runtime = AgentRuntime(
    broker="kafka://kafka.prod:9092",
    publish_events=True,
)
```

The publisher subscribes to `murmur.events.{runtime_id}`; the worker
relays each event through `BrokerEventBridge`. Doubles broker load
(every event becomes a broker message) — opt-in. See
[Events](../concepts/events.md#distributed-event-bridge).

## `murmur serve` as the dashboard for a fleet

```bash
murmur serve --broker kafka://kafka.prod:9092 --publish-events --port 8420
```

One `serve` process becomes the SSE dashboard for the entire worker
fleet. `GET /events/stream` delivers live frames.

## Persistence

Default `InMemoryRunStore` loses runs on restart. For production, pick
one of the persistent concretes:

```python
from murmur.runs import RedisRunStore, SQLiteRunStore

store = RedisRunStore(url="redis://redis.prod:6379")
# or
store = SQLiteRunStore(path="/var/lib/murmur/runs.db")

server = AgentServer(runtime=runtime, run_store=store)
```

All four (`InMemoryRunStore`, `SQLiteRunStore`, `RocksDBRunStore`,
`RedisRunStore`) implement the same `RunStore` Protocol and pass the
same `RunStoreContract` test suite.

## Failure modes

| Failure | Outcome |
|---|---|
| Worker process dies mid-task | Broker re-delivers; another worker picks it up. (Idempotency is the agent author's concern.) |
| Broker partition unavailable | `gather` slot returns `SpawnError`; partial-batch survives. |
| Result topic backed up | Per-spawn future resolves on first matching `batch_id`; backlog drains naturally. |
| Worker can't deserialise output | `ResultMessage.success=False` with `error_message`; publisher's `AgentResult.is_ok()` returns `False`. |

## Where to next

- **LLM-driven dynamic spawning across the fleet** — careful runtime
  binding for `spawn_agents`: [Backends — runtime-binding gotcha](../concepts/backends.md#spawn_agents-and-the-runtime-binding-gotcha).
- **Mount agent endpoints in your existing app** — [Embedded mode](embedded.md).
- **Centralised SSE dashboard for the fleet** — [Events — distributed bridge](../concepts/events.md#distributed-event-bridge).
- **Cap fleet-wide cost** — [`TokenBudget` semantics in distributed mode](../concepts/cost.md#distributed-mode).
