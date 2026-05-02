"""Cost-tracking middleware + ``TokenBudget`` value type.

:class:`TokenBudget` is a mutable counter — ordinarily one per
:class:`AgentRuntime`. The middleware decrements ``remaining`` by
``AgentResult.metadata.tokens_used`` after every agent run, and
short-circuits with :class:`BudgetExceededError` once the budget is
exhausted. Out-of-budget runs emit a :data:`EventType.BUDGET_EXCEEDED`
event before raising so dashboards see them.

Pre-check vs post-charge semantics
----------------------------------

The middleware can only **post-charge**: by the time ``next_stage``
returns, the agent has already burned its tokens. We can't preempt
mid-call from the pipeline level — that's :class:`pydantic_ai`'s
``WrapperModel`` territory and is left for a future iteration.

What this means in practice:

- A run that *starts* with ``remaining > 0`` always completes. Its
  tokens get charged. If the burst exceeded what was left,
  ``remaining`` goes negative.
- The *next* run's pre-check sees ``remaining <= 0`` and raises
  :class:`BudgetExceededError` *before* dispatch.

Net effect: the budget is enforced lazily — one over-spend per
saturation event. For most workloads this is fine; the precise
semantic is documented in the :class:`TokenBudget` docstring.

Distributed mode
----------------

When :class:`JobBackend` is in play, multiple workers may charge the
same publisher-side :class:`TokenBudget` concurrently. The
:class:`asyncio.Lock` only protects same-loop concurrent access; cross-
process budget consistency would need a centralised counter (Redis
``INCRBY`` / etc.). Document this as accepted slop — the budget is a
soft cap, not a hard contract.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, TypeVar

from murmur.core.errors import BudgetExceededError
from murmur.events.types import EventType, RuntimeEvent

if TYPE_CHECKING:
    from murmur.core.pipeline import NextStage, PipelineContext
    from murmur.core.protocols.events import EventEmitter

T = TypeVar("T")


class TokenBudget:
    """Mutable token-cost ceiling.

    Construct with a positive limit. Wire via
    :class:`murmur.RuntimeOptions(token_budget=...)`. The runtime's
    :class:`CostTrackingMiddleware` decrements ``remaining`` after each
    agent run; once it hits ``0`` or below, subsequent runs raise
    :class:`BudgetExceededError` before dispatch.

    Concurrency: an :class:`asyncio.Lock` guards :meth:`consume` against
    same-loop interleavings. Cross-process workers sharing one publisher-
    side budget will race; budget is a soft cap in distributed mode.
    """

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self._limit = limit
        self._remaining = limit
        self._lock = asyncio.Lock()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def remaining(self) -> int:
        """Tokens remaining. Negative when the last run over-spent."""
        return self._remaining

    @property
    def used(self) -> int:
        """``limit - remaining``. Reads cleanly even when remaining is negative."""
        return self._limit - self._remaining

    async def consume(self, n: int) -> None:
        """Decrement ``remaining`` by ``n``. ``n=0`` is a cheap no-op."""
        if n <= 0:
            return
        async with self._lock:
            self._remaining -= n

    def reset(self) -> None:
        """Restore ``remaining`` to ``limit``. Useful in tests + for callers
        running periodic windows (e.g. per-minute budgets)."""
        self._remaining = self._limit


class CostTrackingMiddleware:
    """Pipeline :class:`Stage` that gates and charges a :class:`TokenBudget`.

    Built fresh per spawn by :meth:`AgentRuntime.run` — the per-spawn
    instance closes over the runtime's :class:`EventEmitter` so a
    ``BUDGET_EXCEEDED`` emission flows through the same sink as every
    other runtime event.
    """

    def __init__(
        self,
        budget: TokenBudget,
        *,
        event_emitter: EventEmitter | None = None,
    ) -> None:
        self._budget = budget
        self._emitter = event_emitter

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T:
        if self._budget.remaining <= 0:
            await self._emit_exceeded(context)
            raise BudgetExceededError(
                f"token budget exhausted before agent={context.agent_name!r} "
                f"(limit={self._budget.limit}, used={self._budget.used})"
            )
        result = await next_stage(context)
        tokens = _tokens_used(result)
        if tokens > 0:
            await self._budget.consume(tokens)
        return result

    async def _emit_exceeded(self, context: PipelineContext) -> None:
        if self._emitter is None:
            return
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.BUDGET_EXCEEDED,
                agent_name=context.agent_name,
                task_id=context.task.id,
                trace_id=context.task.request_id,
                payload={
                    "limit": self._budget.limit,
                    "used": self._budget.used,
                    "scope": "runtime",
                },
            )
        )


def _tokens_used(result: Any) -> int:
    """Read ``tokens_used`` off an ``AgentResult.metadata``.

    Defensive ``getattr`` lookups so a non-``AgentResult`` next-stage
    return (rare; the pipeline is typed) gracefully reports zero rather
    than crashing the budget gate.
    """
    metadata = getattr(result, "metadata", None)
    if metadata is None:
        return 0
    return int(getattr(metadata, "tokens_used", 0) or 0)


__all__ = ["CostTrackingMiddleware", "TokenBudget"]
