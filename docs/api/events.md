# Events

Typed runtime instrumentation. Every emitter satisfies the
`EventEmitter` Protocol (defined in `murmur.core.protocols.events`) and
passes the shared `EventEmitterContract` test suite.

```python
from murmur.events import (
    BrokerEventBridge,
    EventType,
    LogEventEmitter,
    MultiEventEmitter,
    OTelMetricsEmitter,
    RuntimeEvent,
    SSEEventEmitter,
)
```

## Value types

### `RuntimeEvent`

::: murmur.events.RuntimeEvent
    options:
      heading_level: 4
      show_bases: false

### `EventType`

::: murmur.events.EventType
    options:
      heading_level: 4
      show_bases: false

## Emitters

### `LogEventEmitter`

::: murmur.events.LogEventEmitter
    options:
      heading_level: 4
      show_bases: false

### `SSEEventEmitter`

::: murmur.events.SSEEventEmitter
    options:
      heading_level: 4
      show_bases: false

### `MultiEventEmitter`

::: murmur.events.MultiEventEmitter
    options:
      heading_level: 4
      show_bases: false

### `BrokerEventBridge`

::: murmur.events.BrokerEventBridge
    options:
      heading_level: 4
      show_bases: false

### `OTelMetricsEmitter`

::: murmur.events.otel.OTelMetricsEmitter
    options:
      heading_level: 4
      show_bases: false
