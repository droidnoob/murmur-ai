# Models

Concurrency-limit primitives for capping provider-side HTTP requests.
Re-exported from PydanticAI under `murmur.models` so user code never
imports `pydantic_ai` directly (Public API Rule).

```python
from murmur.models import (
    AbstractConcurrencyLimiter,
    ConcurrencyLimit,
    ConcurrencyLimiter,
)
```

See the [Agents concept guide](../concepts/agents.md#capping-provider-http-concurrency)
for usage. The two `Agent` knobs that consume these are:

- `Agent.max_concurrent_requests: int | None` — convenience int knob,
  builds a fresh limiter per agent.
- `Agent.model_concurrency_limiter: AbstractConcurrencyLimiter | None`
  — pre-built limiter shared across agents.

The two are mutually exclusive. The runtime wraps the resolved model in
`pydantic_ai.models.concurrency.ConcurrencyLimitedModel` *outside* any
`FallbackModel`, so one slot covers the whole run regardless of which
fallback served the request.

## Classes

| Class | Purpose |
|---|---|
| `ConcurrencyLimiter` | The default in-process limiter. Constructor: `ConcurrencyLimiter(max_running, *, max_queued=None, name=None, tracer=None)`. Tracks waiting count + emits OpenTelemetry spans while awaiting a slot. |
| `ConcurrencyLimit` | Frozen dataclass: `ConcurrencyLimit(max_running, max_queued=None)`. Pass to `ConcurrencyLimiter.from_limit(...)` for backpressure (`ConcurrencyLimitExceeded` when queue depth exceeds `max_queued`). |
| `AbstractConcurrencyLimiter` | Base class for custom limiters. Subclass + implement `acquire(source)` / `release()` for cross-process backends (e.g. Redis-backed for fleet-wide RPM caps). |

```python
from murmur import Agent
from murmur.models import ConcurrencyLimit, ConcurrencyLimiter

# Per-agent cap (one limiter per agent):
solo = Agent(name="solo", model="openai:gpt-5.2", max_concurrent_requests=5, …)

# Shared cap across agents:
pool = ConcurrencyLimiter(max_running=10, name="openai-pool")
head   = Agent(name="head",   model="openai:gpt-5.2", model_concurrency_limiter=pool, …)
minion = Agent(name="minion", model="openai:gpt-5.2", model_concurrency_limiter=pool, …)

# Backpressure (raises ConcurrencyLimitExceeded when queue exceeds max_queued):
bp = ConcurrencyLimiter.from_limit(
    ConcurrencyLimit(max_running=5, max_queued=20),
    name="openai-bp",
)
strict = Agent(name="strict", model="openai:gpt-5.2", model_concurrency_limiter=bp, …)
```

For the full primitive reference, see PydanticAI's
[concurrency module docs](https://ai.pydantic.dev/concurrency/).
