# Cost tracking

Murmur enforces token-cost ceilings via `CostTrackingMiddleware` with
pre-check + post-charge semantics. A `BUDGET_EXCEEDED` event fires before
the error is raised, so observers see the saturation point.

## `TokenBudget`

```python
from murmur.middleware.cost_tracking import TokenBudget

budget = TokenBudget(limit=1_000_000)
```

`TokenBudget` carries an `asyncio.Lock` and a mutable `_remaining`
counter — incompatible with `frozen=True` Pydantic semantics, so
`RuntimeOptions` enables `arbitrary_types_allowed` to accommodate it.
Decision D23b.

| Attribute | Type | Notes |
|---|---|---|
| `limit` | `int` | Hard cap. |
| `remaining` | `int` | Live counter. May go negative on a single over-spend. |
| `used` | `int` | `limit - remaining`. |

## Wiring

```python
from murmur import AgentRuntime, RuntimeOptions
from murmur.middleware.cost_tracking import TokenBudget

runtime = AgentRuntime(
    options=RuntimeOptions(token_budget=TokenBudget(limit=100_000)),
)

# Each runtime.run / runtime.gather charges tokens against the budget.
# Once exhausted, subsequent calls raise BudgetExceededError before dispatch
# and emit BUDGET_EXCEEDED through whatever emitter is wired.
```

## Semantics

The middleware is a pipeline `Stage`:

1. **Pre-check.** Before forwarding to the next stage, reject if
   `remaining <= 0`. Emits `BUDGET_EXCEEDED` then raises
   `BudgetExceededError`.
2. **Post-charge.** After `next_stage(ctx)` returns, deduct
   `result.metadata.tokens_used`. The driver here is
   `_extract_tokens(pa_result.usage())` from PydanticAI — covers both
   model-call tokens and provider-side built-in tool tokens.

This is **post-charge, not preempt.** It can only act around the
agent's run boundary — by the time the next stage returns, the agent
has already burned tokens. The next call's pre-check then raises.

> One over-spend per saturation event is the documented semantic.

Per-call enforcement (cancel mid-run on token-cap hit) requires
PydanticAI's `WrapperModel` and is queued — see decision D23c.

## Distributed mode

In broker mode, multiple cross-process consumers race the same
publisher-side counter. The middleware is **best-effort** there — soft
cap, not hard contract. For hard caps in a distributed deployment,
shape the upstream task supply instead (`gather(max_concurrency=N)` plus
external rate limiting).

## Per-call provider concurrency

A separate, complementary mechanism: `ConcurrencyLimitedModel` (issue
`murmur-ai-dxm`) caps provider-side HTTP requests at the model layer.
Useful when one API key is shared across a Murmur fleet — distinct from
`gather(max_concurrency=)` which caps Murmur's task fan-out.

## Budget propagation across the DAG

Cascading sub-spawns where a parent budget needs to inherit down a tree
is **deferred** to Phase 4 timing — see issue `murmur-ai-zxn.2.3`.
Today's runtime-wide budget is acceptable for non-cascading workloads;
the real value of propagation lands once Phase 4 sub-spawn machinery
exists.
