# Migrating from FastStream

If you already have a FastStream-driven service consuming task messages
and producing results, Murmur slots in at the dispatch boundary —
keeping your broker semantics untouched while adding the Murmur policy
gate, lifecycle hooks, and registry.

## Why migrate

FastStream gives you broker abstraction across Kafka / NATS / RabbitMQ /
Redis. What it doesn't give you for an *agent* fleet:

- **An Agent contract.** FastStream subscribers receive raw messages; you
  build the validation, retry, and tool-execution scaffolding yourself.
  Murmur ships that.
- **Trust enforcement.** Same as PydanticAI — FastStream has no notion of
  per-call policy. Murmur layers `ToolExecutor` over every dispatch.
- **A registry.** Murmur's `YamlRegistry` + `InMemoryRegistry` resolve
  agents by name. With FastStream alone you wire each subscriber by
  hand.
- **Embedded mode.** `AgentRouter` mounts on any FastAPI app — share auth,
  middleware, and lifespan with your existing service.
- **Observability events.** Typed `RuntimeEvent` emitters fan out to
  log / SSE / broker bridge sinks without touching the agent code.

You keep FastStream where it earns its keep — the actual broker
transport — and add Murmur on top.

## Cookbook

### Replace a FastStream subscriber with a Murmur Worker

The most common migration path. Before:

```python
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")


@broker.subscriber("research-tasks")
async def handle_task(message: TaskPayload) -> ResultPayload:
    ...
```

After:

```python
from murmur import Agent, AgentRuntime
from murmur.worker import Worker

researcher = Agent(
    name="research-minion",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=ResearchFinding,
)

runtime = AgentRuntime()                               # thread-mode internally
worker = Worker(
    runtime=runtime,
    broker="kafka://localhost:9092",
    agents=("research-minion",),
    concurrency=20,
)

await worker.start()
```

Murmur generates the per-agent task topic + `{agent}.results` reply topic
automatically. The wire envelope (`TaskMessage` / `ResultMessage`) is
defined in `murmur.messages` — primitive fields (`success: bool`,
`output_payload: dict`, `error_message: str`) so generic `BaseModel`
serialisation isn't a problem (decision D15).

### Expose a Murmur agent as a FastStream handler

If you have an existing FastStream broker and want to plug a Murmur agent
into it without running a `Worker`:

```python
from murmur.interop import as_faststream_handler

handler = as_faststream_handler(agent, runtime=runtime)
broker.subscriber("research-tasks")(handler)
```

This is the inverse of `from_pydantic_ai` — `murmur.interop` is the only
place allowed to import `faststream` directly.

### Migrate broker imports

```python
# before — direct FastStream
from faststream.kafka import KafkaBroker
broker = KafkaBroker("localhost:9092")

# after — Murmur runtime parses the URL internally
from murmur import AgentRuntime
runtime = AgentRuntime(broker="kafka://localhost:9092")
```

The runtime constructs the right FastStream broker (`KafkaBroker`,
`NatsBroker`, `RabbitBroker`, `RedisBroker`) from the URL scheme. You
never import the concrete broker class.

### Lifecycle hooks

FastStream has subscriber lifecycle via decorators; Murmur's `Worker`
offers the same shape:

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

### Auto-discover agents from a registry

```bash
murmur worker start \
    --all-from ./specs \
    --broker kafka://localhost:9092
```

Every YAML spec under `./specs` becomes a subscriber. With raw FastStream
you'd wire each one manually.

## What does *not* change

- **Broker semantics** — at-least-once, ordering guarantees, partition
  keys — are FastStream's, not Murmur's. Murmur doesn't change them.
- **Authentication** to the broker — TLS, SASL, etc. — passes through
  via the URL or the broker arguments.
- **Topic naming** can be customised (Murmur defaults to
  `murmur.{agent_name}.tasks` and `murmur.{agent_name}.results`).
- **Existing FastStream middleware** continues to work for transport
  concerns; Murmur's pipeline middleware sits at a different layer (see
  decision D23 — "two distinct middleware systems exist; don't conflate
  them").

## Incremental adoption path

1. **Wrap one agent** in a `Worker` and run it against your existing
   broker. Confirm the wire envelope behaves correctly.
2. **Add the policy gate.** Set `trust_level=` on the agent and audit
   which tools fire under each level.
3. **Add observability.** Wire `LogEventEmitter` (default — already on)
   plus `SSEEventEmitter` for a live event stream, or
   `BrokerEventBridge` for centralised dashboards.
4. **Move from one-off subscribers to a registry-backed fleet.** Use
   `--all-from` to auto-discover agents in YAML.

## See also

- [Migrating from PydanticAI](migration-pydantic-ai.md)
- [Migrating from raw asyncio](migration-asyncio.md)
- [Distributed deployments](distributed.md)
- [`murmur.interop` API reference](../api/interop.md)
