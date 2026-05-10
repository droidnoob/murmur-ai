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
services) lives at [`examples/distributed.py`](https://github.com/droidnoob/murmur-ai/blob/main/examples/distributed.py)
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

### Stable consumer identity (Redis)

`Worker(consumer_id=...)` pins the broker-side consumer name. On Redis
Streams this is the value passed to the consumer group as `consumer=`.
Setting it to a deployment-stable identifier — a Kubernetes pod name,
a container id, anything that survives a Worker restart — gives you
two guarantees:

1. **Restart reclaims its own pending entries.** Redis remembers which
   stream entries each consumer is holding (the per-consumer PEL).
   When a Worker restarts under the same id, its next `XREADGROUP`
   sees those entries again — no operator intervention.
2. **`XINFO GROUPS` consumer count stays bounded by fleet size.** Every
   restart of a Worker named `pod-3` reuses one slot; without a stable
   id, every restart creates a fresh `<uuid>` consumer that never gets
   reaped, and the roster grows without limit.

Default: `consumer_id` falls back to the worker runtime's `runtime_id`.
If you set a stable `runtime_id` already (the production pattern), you
get stable consumer names for free. Override only when you want the
binding to track something else (e.g. a pod name distinct from the
runtime id).

```python
worker = Worker(
    broker=broker,
    agents={"researcher": researcher},
    consumer_id=os.environ["HOSTNAME"],  # k8s pod name
)
```

CLI: `murmur worker start --consumer-id pod-abc-3 …`.

The other broker schemes ignore `consumer_id` today — Kafka identifies
consumers via `group_id` + partition assignment, NATS by queue group
membership, RabbitMQ at the channel level. Redis is the one where
operator-supplied consumer names are load-bearing.

### Reclaiming abandoned PEL entries

If a Worker dies after claiming a task but before `XACK`, the entry
sits on its PEL forever — unless something reclaims it. Same-`consumer_id`
restart picks up its own PEL automatically (above), but a *different*
replacement (different `consumer_id`, e.g. a fresh k8s pod) won't.

Set `Worker(reclaim_min_idle_ms=...)` to enable
`XAUTOCLAIM`-driven recovery. Default `30_000` (30 s); pass `0` or
`None` to disable.

```python
worker = Worker(
    broker=broker,
    agents={"researcher": researcher},
    consumer_id=os.environ["HOSTNAME"],
    reclaim_min_idle_ms=30_000,  # 30s — claim entries idle longer than this
)
```

CLI: `murmur worker start --reclaim-min-idle-ms 30000 …`.

Implementation: the wrapper registers a sidecar Redis Streams subscriber
in addition to the normal one. The primary subscriber runs
`XREADGROUP > ...` to pick up new entries; the sidecar runs
`XAUTOCLAIM` at the configured idle threshold to inherit any peer's
abandoned entries. Both share the worker's `consumer_id` so reclaimed
ownership is durable across the live worker's own restarts. The same
handler dispatches both paths — abandoned entries arrive a few
seconds later than fresh ones, otherwise indistinguishable.

Other broker schemes ignore `reclaim_min_idle_ms` today — abandoned-
message recovery is broker-specific (Kafka offsets vs. NATS pending vs.
Rabbit unacked-message redelivery on channel close all behave differently).

`prefetch=` (default `5`) is the fan-out fairness knob, but its
effective semantics depend on the broker:

- **Redis** — true per-poll batch cap (Redis Streams `max_records`).
  `prefetch=1` gives the most uniform distribution across the fleet
  at the cost of an extra round-trip per task; higher values favour
  throughput when one worker greedily draining a burst is fine.
- **NATS** — bounds the in-flight backpressure window
  (`pending_msgs_limit`), not per-poll batch size. One busy subscriber
  can still hold all slots while peers idle.
- **Kafka, RabbitMQ** — currently a no-op. FastStream's Kafka
  `DefaultSubscriber` ignores `max_records`, and AMQP channel QoS
  lives on a different call than the wrapper currently exposes.
  Track and tune throughput at the broker for now; a future change
  will switch Kafka to batch mode and wire `channel.set_qos` for Rabbit.

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
