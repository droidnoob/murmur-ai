# Middleware

Pipeline stages wrapping `AgentRuntime.run` per agent call. Each
implements the `Stage` Protocol — `__call__(context, next_stage)` —
and can be composed via `RuntimeOptions`.

```python
from murmur.middleware import (
    DepthLimitMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
)
from murmur.middleware.cost_tracking import CostTrackingMiddleware, TokenBudget
```

## `RetryMiddleware`

::: murmur.middleware.RetryMiddleware
    options:
      heading_level: 3
      show_bases: false

## `TimeoutMiddleware`

::: murmur.middleware.TimeoutMiddleware
    options:
      heading_level: 3
      show_bases: false

## `DepthLimitMiddleware`

::: murmur.middleware.DepthLimitMiddleware
    options:
      heading_level: 3
      show_bases: false

## `CostTrackingMiddleware`

Pre-check + post-charge token enforcement. See
[Cost tracking](../concepts/cost.md) for semantics.

::: murmur.middleware.cost_tracking.CostTrackingMiddleware
    options:
      heading_level: 4
      show_bases: false

## `TokenBudget`

::: murmur.middleware.cost_tracking.TokenBudget
    options:
      heading_level: 3
      show_bases: false
