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
PydanticAI's `WrapperModel` and is queued.

## Distributed mode

In broker mode, multiple cross-process consumers race the same
publisher-side counter. The middleware is **best-effort** there — soft
cap, not hard contract. For hard caps in a distributed deployment,
shape the upstream task supply instead (`gather(max_concurrency=N)` plus
external rate limiting).

## Per-call provider concurrency

A separate, complementary mechanism: `ConcurrencyLimitedModel` caps
provider-side HTTP requests at the model layer. Useful when one API key
is shared across a Murmur fleet — distinct from
`gather(max_concurrency=)` which caps Murmur's task fan-out. See
[Capping provider HTTP concurrency](agents.md#capping-provider-http-concurrency).

## Budget propagation across the DAG

Cascading sub-spawns where a parent budget needs to inherit down a tree
is **deferred** until cascading-spawn machinery lands. Today's
runtime-wide budget is acceptable for non-cascading workloads; the real
value of propagation lands alongside that future work.

## Worked example — tight budget hits the cap

The full runnable script is at
[`examples/cost_budget.py`](https://github.com/murmur-ai/murmur/blob/main/examples/cost_budget.py).
The shape:

```python
from murmur import AgentRuntime, RuntimeOptions
from murmur.core.errors import BudgetExceededError
from murmur.middleware.cost_tracking import TokenBudget

budget = TokenBudget(limit=200)            # tight cap so we hit it quickly
runtime = AgentRuntime(options=RuntimeOptions(token_budget=budget))

for topic in ["winter rain", "city lights", "old library", "morning coffee"]:
    try:
        result = await runtime.run(poet, TaskSpec(input=topic))
    except BudgetExceededError as exc:
        print(f"  budget exceeded: {exc}")
        break
    print(f"  charged {result.metadata.tokens_used} tokens, "
          f"remaining={budget.remaining}")
```

Typical run with a 200-token cap and a haiku-writing agent:

```
--- topic: winter rain (remaining=200) ---
  charged 168 tokens, remaining=32
--- topic: city lights (remaining=32) ---
  charged 162 tokens, remaining=-130        # one over-spend tolerated
--- topic: old library (remaining=-130) ---
  budget exceeded: token budget of 200 exhausted (used 330)
```

Three observations:

1. The first call charges normally — pre-check sees `remaining=200`.
2. The second call is admitted (remaining was still positive on entry),
   then post-charges into the negative — that's the documented
   "one over-spend per saturation event" semantic.
3. The third call's pre-check sees `remaining<0` and raises
   `BudgetExceededError` before dispatch. A `BUDGET_EXCEEDED`
   `RuntimeEvent` fires first, so an `SSEEventEmitter` consumer sees
   the saturation point in the stream.

If you're routing budget alerts elsewhere (Datadog, PagerDuty), wire a
`MultiEventEmitter` with a custom emitter that pages on
`EventType.BUDGET_EXCEEDED`. See [Events](events.md).
