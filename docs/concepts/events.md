# Events & observability

Every spawn, tool call, and completion in Murmur flows through a typed
`RuntimeEvent` envelope. Emitters are swappable and composable —
`LogEventEmitter` is the always-on default, and you layer on
`SSEEventEmitter`, `MultiEventEmitter`, or `BrokerEventBridge` as the
deployment requires.

## `RuntimeEvent`

```python
from murmur.events import RuntimeEvent, EventType


class RuntimeEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_type: EventType
    timestamp: datetime
    agent_name: str
    task_id: str | None
    trace_id: str
    parent_trace_id: str | None
    payload: Mapping[str, object]
```

`trace_id` is the same value `TaskSpec.request_id` carries — Murmur
doesn't introduce a separate ID for events. `parent_trace_id` stays
`None` until cascading-spawn machinery ships.

This affects the [`spawn_agents`](agents.md#llm-driven-fan-out-with-spawn_agents)
tool: each child fired by the orchestrator's LLM emits the standard
`AGENT_SPAWNED` / `AGENT_COMPLETED` (or `_FAILED`) pair, but children
appear as **independent top-level runs** in the event stream — there's
no parent-pointer back to the orchestrator's `trace_id` yet. Observers
correlate by timing + `agent_name` until the cascading-spawn graph
surfaces.

## `EventType`

```python
class EventType(StrEnum):
    AGENT_SPAWNED = "agent_spawned"
    AGENT_DISPATCHED = "agent_dispatched"
    AGENT_COMPLETED = "agent_completed"
    AGENT_FAILED = "agent_failed"
    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    TOOL_CALL_FAILED = "tool_call_failed"
    BUDGET_EXCEEDED = "budget_exceeded"
    DEPTH_LIMIT_EXCEEDED = "depth_limit_exceeded"
    BATCH_STARTED = "batch_started"
    BATCH_COMPLETED = "batch_completed"
    GROUP_STARTED = "group_started"
    GROUP_COMPLETED = "group_completed"
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"
    WORKER_HEARTBEAT = "worker_heartbeat"
```

### Per-event payload contract

| `EventType` | Payload |
|---|---|
| `AGENT_DISPATCHED` | `{backend, broker, trust_level}` |
| `AGENT_SPAWNED` | `{backend, trust_level}` |
| `AGENT_COMPLETED` | `{duration_ms, tokens_used, backend, model}` |
| `AGENT_FAILED` | `{duration_ms, error, backend}` (typed reason in `reason` when present) |
| `TOOL_CALL_STARTED` | `{tool_name, trust_level}` |
| `TOOL_CALL_COMPLETED` | `{tool_name, duration_ms, tokens_used}` |
| `TOOL_CALL_FAILED` | `{tool_name, error, duration_ms}` |
| `BATCH_STARTED` | `{task_count, max_concurrency}` |
| `BATCH_COMPLETED` | `{task_count, success_count, failure_count}` |
| `GROUP_STARTED` | `{group_name, node_count}` |
| `GROUP_COMPLETED` | `{group_name, duration_ms}` |
| `BUDGET_EXCEEDED` | `{limit, used, scope}` |
| `DEPTH_LIMIT_EXCEEDED` | `{limit, depth}` |
| `WORKER_STARTED` | `{runtime_id, agents, broker_scheme, concurrency, prefetch, consumer_id, heartbeat_seconds}` |
| `WORKER_STOPPED` | `{runtime_id, agents, broker_scheme}` |
| `WORKER_HEARTBEAT` | `{runtime_id, agent_subscriptions, in_flight, concurrency_cap, broker_scheme}` |

`AGENT_COMPLETED.model` is the resolved identifier — either the
user-supplied `"provider:name"` string or `"{system}:{model_name}"` when
a `Model` instance was passed. `TOOL_CALL_COMPLETED.tokens_used` is
best-effort LLM cost attribution to the tool call; until the agent loop
reports a per-call delta it is `0`. Worker-lifecycle events use the
worker's `runtime_id` as both `agent_name` and `trace_id` (a worker isn't
tied to a single agent — the runtime id is the closest stable handle).

## `EventEmitter` Protocol

```python
@runtime_checkable
class EventEmitter(Protocol):
    async def emit(self, event: RuntimeEvent) -> None: ...
```

Every concrete passes the shared `EventEmitterContract` test suite:
Protocol shape, `emit` returns `None`, never raises, burst-no-block,
covers every `EventType`, concurrent-no-deadlock.

## Emitters shipped

### `LogEventEmitter`

Default. Forwards every event to `structlog` with the event's
`EventType.value` as the event name. Failure event types
(`agent_failed`, `tool_call_failed`, `budget_exceeded`,
`depth_limit_exceeded`) route to `aerror`; everything else goes to
`ainfo`.

### `SSEEventEmitter`

Per-subscriber bounded queues with idle heartbeats. Overflow drops
events instead of blocking — observability never takes a run down.
Default queue size 1024.

```python
from murmur.events import SSEEventEmitter

sse = SSEEventEmitter(heartbeat_interval=15.0)

async for event in sse.subscribe():
    print(event.event_type, event.payload)
```

`subscribe()` returns an `AsyncGenerator` (not `AsyncIterator`) so
callers can `aclose()` on connection drop.

### `MultiEventEmitter`

Fan-out. Sibling failures are contained — a custom emitter that raises
won't take the others down. Wrap your custom emitter directly (without
`Multi`) to surface the raise during debugging.

```python
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)
```

### `OTelMetricsEmitter`

OpenTelemetry GenAI metrics adapter. Drops in alongside `LogEventEmitter`
inside a `MultiEventEmitter`; every `RuntimeEvent` that carries
quantitative information is recorded as an OTel histogram or counter.

Behind the optional `murmur-runtime[otel]` extra:

```bash
pip install "murmur-runtime[otel]"
```

Importing the emitter without the extra raises a clear `ImportError`
that names the missing extra.

#### What gets recorded

| Instrument | Type | Unit | When |
|---|---|---|---|
| `gen_ai.client.token.usage` | histogram | `{token}` | `AGENT_COMPLETED` with `tokens_used > 0` |
| `gen_ai.client.operation.duration` | histogram | `s` | `AGENT_COMPLETED` and `AGENT_FAILED` |
| `murmur.tool.calls` | counter | `{call}` | `TOOL_CALL_COMPLETED` and `TOOL_CALL_FAILED` |
| `murmur.tool.duration_ms` | histogram | `ms` | `TOOL_CALL_COMPLETED` and `TOOL_CALL_FAILED` |
| `murmur.rejections` | counter | `{rejection}` | `BUDGET_EXCEEDED`, `DEPTH_LIMIT_EXCEEDED`, `AGENT_FAILED` |

The `gen_ai.*` instruments follow the OpenTelemetry [GenAI semantic
conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/).
Attributes match the spec: `gen_ai.operation.name` (`"invoke_agent"`),
`gen_ai.provider.name` and `gen_ai.request.model` parsed out of Murmur's
resolved model identifier (`"provider:name"`), `gen_ai.token.type`
(`"total"` — Murmur sums input/output at the runtime level), and
`error.type` on failed runs (the typed error class, e.g.
`BudgetExceededError`, `DepthLimitError`).

#### Wiring

Murmur deliberately stays out of exporter / endpoint configuration. Set
the global `MeterProvider` before constructing the emitter, or pass an
explicit one in:

```python
from murmur import AgentRuntime
from murmur.events import LogEventEmitter, MultiEventEmitter, OTelMetricsEmitter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

reader = PeriodicExportingMetricReader(OTLPMetricExporter())
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), OTelMetricsEmitter()]),
)
```

The same `MeterProvider` is what Datadog (v1.37+), Grafana, Logfire, and
Phoenix natively ingest. Murmur emits — your provider decides where the
metrics land.

#### Cardinality discipline

Attributes used for labels are bounded by design: `agent`, `tool`,
`gen_ai.provider.name`, `gen_ai.request.model`. Murmur deliberately does
**not** label by `trace_id`, `task_id`, prompt content, or tool argument
blobs — those are the cardinality bombs that fall over OTel backends in
production. If a custom emission point needs a high-cardinality label,
log the event instead of metric-ing it.

### `BrokerEventBridge`

Distributed-mode emitter. Publishes events to a per-runtime broker
topic (`murmur.events.{runtime_id}`) so a publisher can subscribe to its
worker fleet's events. Contextvar-driven — when the topic is unbound,
`emit` is a no-op, so the bridge is safe to install always. Workers bind
the contextvar per-task. See [Distributed event bridge](#distributed-event-bridge).

## `murmur serve`

Standalone HTTP server with a built-in SSE endpoint:

```bash
murmur serve --port 8420 [--broker URL] [--publish-events]
```

`GET /events/stream` delivers live `RuntimeEvent` frames as SSE. Use
`--broker URL --publish-events` to make one `serve` process the SSE
dashboard for an entire worker fleet via `BrokerEventBridge`.

When an `EventStore` is wired into the runtime's emitter chain (the
`StoreEventEmitter` adapter does this — see
`murmur.events.store`), the server also exposes a small set of
read-only rollups computed on demand from the store:

| Endpoint | Returns | Notes |
|---|---|---|
| `GET /events?limit=&since=&until=&trace_id=&event_type=` | recent `RuntimeEvent` rows | up to 100k per request |
| `GET /usage?group_by=agent\|trace\|model\|none` | tokens grouped by key | aggregates `AGENT_COMPLETED.tokens_used`; `model` keys on the resolved identifier with `unknown` fallback |
| `GET /tools?group_by=tool\|agent_tool` | per-tool latency rows | `{calls, failures, p50_ms, p95_ms, p99_ms, avg_ms}`; counts both `TOOL_CALL_COMPLETED` and `TOOL_CALL_FAILED` toward `calls`, only failures toward `failures` |
| `GET /runtime/stats` | composite dashboard rollup | header meters, burn rate, rejection breakdown, error groups, MCP servers, **and** worker fleet — workers are derived from the latest `WORKER_HEARTBEAT` per `runtime_id`, with status `healthy` (<90s), `stale` (<300s), or `down` (≥300s or after a `WORKER_STOPPED`) |
| `GET /runs/{trace_id}/tree` | event tree for one run | feeds the run inspector |

For local development, add `--reload` to auto-restart on file changes
(uses `watchfiles`, same library as FastStream + uvicorn — install via
`uv add 'murmur-runtime[reload]'`):

```bash
murmur serve --port 8420 --reload --reload-dir ./specs --reload-dir ./src
```

Default include set is `*.py`, `*.yaml`, `*.yml`. Override with
`--reload-include` / `--reload-exclude`.

## `murmur status` — terminal SSE consumer

Tail-style live view of the same `/events/stream` endpoint, useful in
CI logs or over SSH when no browser is available:

```bash
murmur status                                         # 127.0.0.1:8420 by default
murmur status --url http://prod-host:8420/events/stream
murmur status --filter-event-type agent_failed --filter-event-type tool_call_failed
murmur status --filter-agent researcher
```

Each `RuntimeEvent` renders as one line: `event_type agent=… task=…
trace=… [payload-key=value, …]`. Reconnects on dropped connection
(`--no-reconnect` to fail-fast instead). Ctrl-C exits cleanly.

## Distributed event bridge

Without `publish_events=True`, the publisher's emitter sees only
`BATCH_*` / `GROUP_*` / `AGENT_DISPATCHED` (those fire publisher-side).
Per-agent and per-tool events fire on the worker process — the right
model for log-aggregation pipelines (Datadog, Loki) where both processes
ship logs to the same sink.

For centralised dashboards, opt the publisher into the bridge:

```python
runtime = AgentRuntime(
    broker="kafka://localhost:9092",
    publish_events=True,
)
```

The publisher subscribes to `murmur.events.{runtime_id}`, the worker
relays each `RuntimeEvent` through `BrokerEventBridge`, and the
publisher's local emitter sees the full stream. This doubles broker
load (every event becomes a broker message), so it's opt-in.

`AGENT_DISPATCHED` fires publisher-side regardless of `publish_events`
— gives callers immediate "task accepted by broker" visibility even
when the worker is seconds away from picking it up.
