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

`trace_id` **is** `request_id` from Phase 1 — Phase 2 didn't introduce a
new ID concept. `parent_trace_id` stays `None` until Phase 4 cascading
spawn lands. Decision D19.

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
```

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
`ainfo`. Decisions D20, D21.

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
callers can `aclose()` on connection drop. Decision D22, D28.

### `MultiEventEmitter`

Fan-out. Sibling failures are contained — a custom emitter that raises
won't take the others down. Wrap your custom emitter directly (without
`Multi`) to surface the raise during debugging. Decision D29.

```python
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)
```

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

For local development, add `--reload` to auto-restart on file changes
(uses `watchfiles`, same library as FastStream + uvicorn — install via
`uv add 'murmur-ai[reload]'`):

```bash
murmur serve --port 8420 --reload --reload-dir ./specs --reload-dir ./src
```

Default include set is `*.py`, `*.yaml`, `*.yml`. Override with
`--reload-include` / `--reload-exclude`.

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
load (every event becomes a broker message), so it's opt-in. Decision D30.

`AGENT_DISPATCHED` fires publisher-side regardless of `publish_events`
— gives callers immediate "task accepted by broker" visibility even
when the worker is seconds away from picking it up. Decision D31.
