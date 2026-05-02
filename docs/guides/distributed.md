# Distributed deployments

When one machine isn't enough, swap `AgentRuntime()` for
`AgentRuntime(broker="…")` and run a fleet of workers. The agent
definition doesn't change. The workflow doesn't change. Only the
runtime constructor does.

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
correlated to the publisher's outstanding spawns by `batch_id`. Decision
D5.

## Publisher

```python
from murmur import AgentRuntime, TaskSpec

runtime = AgentRuntime(broker="kafka://kafka.prod:9092")
results = await runtime.gather(researcher, tasks=tasks, max_concurrency=200)
```

The publisher's runtime is thread-mode internally — `JobBackend` handles
the publish + collect. Worker republish loops are avoided by routing
through `ResultCollector`.

## Workers

Programmatic:

```python
from murmur import AgentRuntime
from murmur.worker import Worker

runtime = AgentRuntime()                      # thread-mode, intentionally
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

`Worker.start()` prints a multi-line Murmur banner to stderr with
broker, runtime id, agents, per-agent topics, and concurrency. The
banner uses `sys.stderr` directly so the layout survives structlog
rendering. Decision D18.

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
