"""Tests for ``TokenBudget`` + ``CostTrackingMiddleware`` (zxn.2.1 + zxn.2.2)."""

from __future__ import annotations

import asyncio
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import BudgetExceededError
from murmur.events import EventType, RuntimeEvent
from murmur.middleware.cost_tracking import CostTrackingMiddleware, TokenBudget
from murmur.runtime import AgentRuntime, RuntimeOptions
from murmur.types import (
    AgentContext,
    AgentResult,
    ResultMetadata,
    TaskSpec,
    TrustLevel,
)

# ---------------------------------------------------------------------------
# TokenBudget — value type
# ---------------------------------------------------------------------------


class TestTokenBudget:
    def test_construction_rejects_zero_or_negative_limit(self) -> None:
        with pytest.raises(ValueError, match="limit must be"):
            TokenBudget(limit=0)
        with pytest.raises(ValueError, match="limit must be"):
            TokenBudget(limit=-5)

    def test_initial_state_full(self) -> None:
        b = TokenBudget(limit=100)
        assert b.limit == 100
        assert b.remaining == 100
        assert b.used == 0

    async def test_consume_decrements_remaining(self) -> None:
        b = TokenBudget(limit=100)
        await b.consume(30)
        assert b.remaining == 70
        assert b.used == 30

    async def test_consume_zero_or_negative_is_noop(self) -> None:
        b = TokenBudget(limit=100)
        await b.consume(0)
        await b.consume(-5)
        assert b.remaining == 100

    async def test_consume_can_drive_remaining_negative(self) -> None:
        """Last burst over-spends — remaining goes negative; next pre-check raises."""
        b = TokenBudget(limit=100)
        await b.consume(150)
        assert b.remaining == -50
        assert b.used == 150

    async def test_concurrent_consume_is_safe(self) -> None:
        b = TokenBudget(limit=10_000)
        # 100 coroutines each charging 50 tokens = 5000 total.
        await asyncio.gather(*(b.consume(50) for _ in range(100)))
        assert b.used == 5_000
        assert b.remaining == 5_000

    def test_reset_restores_full_limit(self) -> None:
        b = TokenBudget(limit=100)
        # Bypass consume's lock requirement — simulate post-consume state.
        b._remaining = -30  # noqa: SLF001
        b.reset()
        assert b.remaining == 100
        assert b.used == 0


# ---------------------------------------------------------------------------
# CostTrackingMiddleware — pipeline behavior
# ---------------------------------------------------------------------------


class _CollectingEmitter:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def emit(self, event: RuntimeEvent) -> None:
        self.events.append(event)


def _result(tokens: int, agent_name: str = "r") -> AgentResult[BaseModel]:
    class _Out(BaseModel):
        text: str

    return AgentResult[BaseModel](
        output=_Out(text="ok"),
        metadata=ResultMetadata(tokens_used=tokens, backend="thread"),
        agent_name=agent_name,
        task_id="t-1",
    )


def _ctx(agent_name: str = "r", task_id: str = "t-1") -> Any:
    """Minimal stand-in for PipelineContext — middleware reads two attrs."""
    from murmur.core.pipeline import PipelineContext

    return PipelineContext(
        task=TaskSpec(input="hi", id=task_id, request_id="req-1"),
        agent_name=agent_name,
        agent_context=AgentContext(),
    )


async def test_passes_through_when_budget_ok() -> None:
    budget = TokenBudget(limit=1_000)
    mw = CostTrackingMiddleware(budget)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=100)

    out = await mw(_ctx(), next_stage)
    assert out.is_ok()
    assert budget.used == 100


async def test_charges_after_dispatch() -> None:
    budget = TokenBudget(limit=1_000)
    mw = CostTrackingMiddleware(budget)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=42)

    await mw(_ctx(), next_stage)
    await mw(_ctx(), next_stage)
    await mw(_ctx(), next_stage)
    assert budget.used == 126


async def test_zero_tokens_used_is_noop() -> None:
    budget = TokenBudget(limit=1_000)
    mw = CostTrackingMiddleware(budget)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=0)

    await mw(_ctx(), next_stage)
    assert budget.used == 0


async def test_pre_check_raises_when_remaining_zero() -> None:
    budget = TokenBudget(limit=100)
    await budget.consume(100)
    mw = CostTrackingMiddleware(budget)

    called = False

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        nonlocal called
        called = True
        return _result(tokens=10)

    with pytest.raises(BudgetExceededError, match="exhausted"):
        await mw(_ctx(), next_stage)
    assert not called  # short-circuited before dispatch


async def test_pre_check_raises_when_remaining_negative() -> None:
    """A previous over-spend leaves remaining < 0 — next call still rejects."""
    budget = TokenBudget(limit=100)
    await budget.consume(150)  # over-spend by 50
    mw = CostTrackingMiddleware(budget)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=10)

    with pytest.raises(BudgetExceededError):
        await mw(_ctx(), next_stage)


async def test_emits_budget_exceeded_event() -> None:
    budget = TokenBudget(limit=100)
    await budget.consume(100)
    emitter = _CollectingEmitter()
    mw = CostTrackingMiddleware(budget, event_emitter=emitter)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=10)

    with pytest.raises(BudgetExceededError):
        await mw(_ctx(), next_stage)

    [event] = emitter.events
    assert event.event_type is EventType.BUDGET_EXCEEDED
    assert event.payload["limit"] == 100
    assert event.payload["used"] == 100
    assert event.payload["scope"] == "runtime"
    assert event.agent_name == "r"
    assert event.trace_id == "req-1"


async def test_no_emitter_still_raises() -> None:
    """Emitter is optional — None means just raise without an event."""
    budget = TokenBudget(limit=100)
    await budget.consume(100)
    mw = CostTrackingMiddleware(budget, event_emitter=None)

    async def next_stage(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=10)

    with pytest.raises(BudgetExceededError):
        await mw(_ctx(), next_stage)


async def test_first_run_completes_then_next_is_blocked() -> None:
    """Saturation semantic: one over-spend, then hard-stop."""
    budget = TokenBudget(limit=100)
    mw = CostTrackingMiddleware(budget)

    async def big(_ctx: Any) -> AgentResult[BaseModel]:
        return _result(tokens=200)  # over-spend

    # First call: budget had 100 remaining → call goes through.
    out = await mw(_ctx(), big)
    assert out.is_ok()
    assert budget.remaining == -100  # over-spent

    # Second call: pre-check sees remaining <= 0 → raises.
    with pytest.raises(BudgetExceededError):
        await mw(_ctx(), big)


# ---------------------------------------------------------------------------
# Runtime integration — RuntimeOptions(token_budget=...)
# ---------------------------------------------------------------------------


class _Out(BaseModel):
    text: str


async def _stub_pa_agent_with_tokens(
    tokens: int,
) -> Any:
    """Build a stub PA agent that reports ``tokens`` of usage on each run."""

    async def factory(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return factory


def _agent() -> Agent:
    return Agent(
        name="r",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


async def test_runtime_options_accepts_token_budget() -> None:
    """RuntimeOptions has arbitrary_types_allowed — TokenBudget instance is fine."""
    budget = TokenBudget(limit=1_000)
    opts = RuntimeOptions(token_budget=budget)
    assert opts.token_budget is budget


async def test_runtime_with_budget_blocks_after_exhaustion() -> None:
    """End-to-end: AgentRuntime(options=RuntimeOptions(token_budget=...)) enforces.

    The middleware raises *before dispatch*; that bubbles up through the
    pipeline as a real exception (not an ``AgentResult`` with ``.error``
    set — that path is for failures *inside* dispatch). Mirrors how
    :class:`DepthLimitMiddleware` propagates :class:`DepthLimitError`.
    """
    budget = TokenBudget(limit=1)  # tiny — already-exhausted state
    await budget.consume(1)

    backend = AsyncBackend()
    backend._build_pa_agent = await _stub_pa_agent_with_tokens(0)
    runtime = AgentRuntime(
        backend=backend,
        options=RuntimeOptions(token_budget=budget),
    )

    with pytest.raises(BudgetExceededError, match="exhausted"):
        await runtime.run(_agent(), TaskSpec(input="hi"))


async def test_runtime_without_budget_skips_middleware() -> None:
    """RuntimeOptions.token_budget=None (default) — no middleware overhead."""
    backend = AsyncBackend()
    backend._build_pa_agent = await _stub_pa_agent_with_tokens(0)
    runtime = AgentRuntime(backend=backend)  # default options, no budget

    result = await runtime.run(_agent(), TaskSpec(input="hi"))
    assert result.is_ok()
