# Worker

Distributed broker consumer. One process subscribes to per-agent task
topics, dispatches each `TaskMessage` through an internal in-process
`AgentRuntime`, and publishes `ResultMessage` envelopes back on the
agent's results topic.

```python
from murmur.worker import Worker
```

See the [Distributed deployments guide](../guides/distributed.md) for
the wire shape and production patterns.

## `Worker`

::: murmur.worker.Worker
    options:
      heading_level: 3
      show_bases: false
