# Protocols

The abstract ports Murmur is wired against. Every pluggable component
is a `typing.Protocol` here first; concretes in sibling packages match
**structurally** — no inheritance, no registration. Tests are keyed on
the Protocol so every concrete passes the same shared contract suite.

```python
from murmur.core.protocols import (
    Backend,
    BackendStatus,
    Broker,
    ContextPasser,
    EventEmitter,
    MessageHandler,
    Middleware,
    NextStage,
    OnComplete,
    OnError,
    OnStart,
    Pipeline,
    Registry,
    RouteDecision,
    Router,
    Stage,
    ToolDescriptor,
    ToolExecutor,
    ToolProvider,
    ToolsetProvider,
    Worker,
)
```

A handful of Protocols carry `@runtime_checkable` so they can be used as
Pydantic field types or in `isinstance()` checks — `EventEmitter` and
`ToolsetProvider` today.

## Execution

### `Backend`

The unit of execution. Concretes:
[`AsyncBackend`](../concepts/backends.md#asyncbackend),
[`JobBackend`](../concepts/backends.md#jobbackend), and a future
`ContainerBackend`. All pass the shared `BackendContract` test suite.

::: murmur.core.protocols.Backend
    options:
      heading_level: 4
      show_bases: false

### `BackendStatus`

::: murmur.core.protocols.BackendStatus
    options:
      heading_level: 4
      show_bases: false

## Context passing

### `ContextPasser`

Decides what flows into a spawn. Concretes today:
[`FullContextPasser`](context.md#fullcontextpasser),
[`NullContextPasser`](context.md#nullcontextpasser).
`SummaryContextPasser` and `SelectiveContextPasser` are queued.

::: murmur.core.protocols.ContextPasser
    options:
      heading_level: 4
      show_bases: false

## Tools

### `ToolProvider`

Resolves an agent's allowed tools at dispatch time. Concrete today:
[`StaticToolProvider`](tools.md#statictoolprovider).
`RoleBasedToolProvider` (role → tool-set map) and `DenylistToolProvider`
(base set minus denied) are queued.

::: murmur.core.protocols.ToolProvider
    options:
      heading_level: 4
      show_bases: false

### `ToolExecutor`

The runtime-side executor that gates and proxies every tool call.
Concrete: [`murmur.tools.ToolExecutor`](tools.md#toolexecutor) — same
name, different module (Protocol in `core.protocols`, concrete in
`tools`).

::: murmur.core.protocols.ToolExecutor
    options:
      heading_level: 4
      show_bases: false

### `ToolsetProvider`

Dynamic, runtime-discovered tool sources — primarily MCP. Concrete:
`MCPToolsetProvider`, constructed via the
[`mcp_stdio` / `mcp_http` / `mcp_sse`](tools.md#mcp-factories)
factories. Marked `@runtime_checkable` so Pydantic accepts it as a
field type on `Agent.mcp_servers`.

::: murmur.core.protocols.ToolsetProvider
    options:
      heading_level: 4
      show_bases: false

### `ToolDescriptor`

::: murmur.core.protocols.ToolDescriptor
    options:
      heading_level: 4
      show_bases: false

## Routing

### `Router`

Classifies a task into a single-agent vs multi-agent path. Concrete
today: `AlwaysSingleRouter` in `murmur.routing`. An LLM-based router is
queued.

::: murmur.core.protocols.Router
    options:
      heading_level: 4
      show_bases: false

### `RouteDecision`

::: murmur.core.protocols.RouteDecision
    options:
      heading_level: 4
      show_bases: false

## Events

### `EventEmitter`

The instrumentation sink. Concretes:
[`LogEventEmitter`](events.md#logeventemitter),
[`SSEEventEmitter`](events.md#sseeventemitter),
[`MultiEventEmitter`](events.md#multieventemitter),
[`BrokerEventBridge`](events.md#brokereventbridge). All pass the shared
`EventEmitterContract` suite. Marked `@runtime_checkable`.

A `WebSocketEventEmitter` (push live events to connected dashboards) and
`FastStreamEventEmitter` (publish events onto a configurable broker
topic) are queued.

::: murmur.core.protocols.EventEmitter
    options:
      heading_level: 4
      show_bases: false

## Persistence

### `Registry`

Resolves agent / group names to spec instances. Concretes:
`InMemoryRegistry`, `YamlRegistry` in `murmur.registry`.

::: murmur.core.protocols.Registry
    options:
      heading_level: 4
      show_bases: false

### Run store

`RunStore` is documented under [Runs](runs.md#runstore-protocol) since
it lives in `murmur.runs` rather than `murmur.core.protocols` — the
single exception to the Protocols-first layout, because the value
types it operates on (`RunStatus`, `RunProgress`, `RunEvent`) live in
the same package.

## Pipeline

### `Pipeline`

The composer that wires `Stage` and `Middleware` instances around the
backend dispatch. Concrete: the unnamed pipeline returned by
`murmur.core.pipeline.build_pipeline`.

::: murmur.core.protocols.Pipeline
    options:
      heading_level: 4
      show_bases: false

### `Stage`

A single hop in the pipeline. Receives `PipelineContext` and a reference
to the next stage; can mutate context, transform the result, or
short-circuit.

::: murmur.core.protocols.Stage
    options:
      heading_level: 4
      show_bases: false

### `Middleware`

Identical shape to `Stage`. Distinct name to clarify intent — middleware
is for cross-cutting concerns ([Retry](middleware.md#retrymiddleware),
[Timeout](middleware.md#timeoutmiddleware),
[DepthLimit](middleware.md#depthlimitmiddleware),
[CostTracking](middleware.md#costtrackingmiddleware)) where stages are
domain operations (route, resolve context, dispatch).

::: murmur.core.protocols.Middleware
    options:
      heading_level: 4
      show_bases: false

### `NextStage`

::: murmur.core.protocols.NextStage
    options:
      heading_level: 4

## Distributed

### `Broker`

The message-bus abstraction backing `JobBackend`. Concretes:

- `FastStreamRedisBroker` — Redis Streams; first-class `consumer_id`,
  `prefetch`, and `group` support.
- `FastStreamKafkaBroker` — Kafka with consumer `group_id`.
- `FastStreamNatsBroker` — NATS queue groups.
- `FastStreamRabbitBroker` — RabbitMQ named queues (competing-consumer
  by default).
- `InMemoryBroker` — in-process round-robin, `memory://` URLs, used in
  tests.

Production code routes through the URL-keyed factory at
`AgentRuntime(broker="redis://…")` and stays scheme-agnostic; the per-
scheme classes are importable directly when an integration test needs
the explicit type.

::: murmur.core.protocols.Broker
    options:
      heading_level: 4
      show_bases: false

### `MessageHandler`

::: murmur.core.protocols.MessageHandler
    options:
      heading_level: 4

### `Worker`

The distributed consumer. Concrete:
[`murmur.worker.Worker`](worker.md#worker).

::: murmur.core.protocols.Worker
    options:
      heading_level: 4
      show_bases: false

### Worker hooks

Type aliases for the lifecycle callbacks attached via `@worker.on_task_start`
etc.

::: murmur.core.protocols.OnStart
    options:
      heading_level: 4

::: murmur.core.protocols.OnComplete
    options:
      heading_level: 4

::: murmur.core.protocols.OnError
    options:
      heading_level: 4
