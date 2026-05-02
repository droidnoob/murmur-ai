"""Tests for ``Agent.max_concurrent_requests`` / ``model_concurrency_limiter``.

Verifies the fields round-trip, default to None, are mutually exclusive, and
gate the construction of ``pydantic_ai.models.concurrency.ConcurrencyLimitedModel``
at dispatch. The limiter semantics themselves (queue depth, span emission,
backpressure) are upstream PydanticAI concerns; we only assert the wiring.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel, ValidationError
from pydantic_ai.models.concurrency import ConcurrencyLimitedModel
from pydantic_ai.models.fallback import FallbackModel

from murmur._dispatch import build_pydantic_ai_agent
from murmur.agent import Agent
from murmur.models import ConcurrencyLimit, ConcurrencyLimiter
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry


class _Out(BaseModel):
    text: str


def _make_agent(**kwargs: Any) -> Agent:
    defaults: dict[str, Any] = {
        "name": "r",
        "model": "test",
        "instructions": "...",
        "output_type": _Out,
    }
    defaults.update(kwargs)
    return Agent(**defaults)


def _capture_pa_init() -> tuple[dict[str, Any], Any]:
    """Test seam — patch ``pydantic_ai.Agent.__init__`` to capture kwargs."""
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    return captured, original_init


def _restore_pa_init(original_init: Any) -> None:
    pydantic_ai.Agent.__init__ = original_init  # test seam restore


# ---------------------------------------------------------------------------
# Field shape
# ---------------------------------------------------------------------------


def test_defaults_are_none() -> None:
    a = _make_agent()
    assert a.max_concurrent_requests is None
    assert a.model_concurrency_limiter is None


def test_max_concurrent_requests_stores_int() -> None:
    a = _make_agent(max_concurrent_requests=5)
    assert a.max_concurrent_requests == 5


def test_model_concurrency_limiter_stores_limiter() -> None:
    pool = ConcurrencyLimiter(max_running=10, name="pool")
    a = _make_agent(model_concurrency_limiter=pool)
    assert a.model_concurrency_limiter is pool


def test_fields_are_frozen() -> None:
    a = _make_agent(max_concurrent_requests=5)
    with pytest.raises(ValidationError):
        a.max_concurrent_requests = 10


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_mutual_exclusivity_rejected() -> None:
    """Setting both knobs at once raises a clear validation error."""
    pool = ConcurrencyLimiter(max_running=10)
    with pytest.raises(ValidationError, match="mutually exclusive"):
        _make_agent(max_concurrent_requests=5, model_concurrency_limiter=pool)


def test_zero_max_concurrent_requests_rejected() -> None:
    with pytest.raises(ValidationError, match="positive integer"):
        _make_agent(max_concurrent_requests=0)


def test_negative_max_concurrent_requests_rejected() -> None:
    with pytest.raises(ValidationError, match="positive integer"):
        _make_agent(max_concurrent_requests=-3)


# ---------------------------------------------------------------------------
# Dispatch — ConcurrencyLimitedModel wrap is opt-in
# ---------------------------------------------------------------------------


async def test_dispatch_passes_string_when_neither_set() -> None:
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    assert captured["model"] == "test"
    assert not isinstance(captured["model"], ConcurrencyLimitedModel)


async def test_dispatch_wraps_with_int_knob() -> None:
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(max_concurrent_requests=3),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    forwarded = captured["model"]
    assert isinstance(forwarded, ConcurrencyLimitedModel)
    # Internal limiter should reflect the int we asked for.
    assert forwarded._limiter.max_running == 3  # type: ignore[attr-defined]


async def test_dispatch_wraps_with_shared_limiter() -> None:
    pool = ConcurrencyLimiter(max_running=7, name="pool")
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(model_concurrency_limiter=pool),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    forwarded = captured["model"]
    assert isinstance(forwarded, ConcurrencyLimitedModel)
    # The exact limiter instance must thread through — that's how shared caps
    # across agents work.
    assert forwarded._limiter is pool  # type: ignore[attr-defined]


async def test_shared_limiter_across_two_agents_uses_same_instance() -> None:
    """Two agents pointing at one limiter must end up wrapped with the same
    AbstractConcurrencyLimiter object — that's what gives them a *shared* cap.
    """
    pool = ConcurrencyLimiter(max_running=2, name="shared")
    captured_a: dict[str, Any] = {}
    captured_b: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__
    counter = {"n": 0}

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        target = captured_a if counter["n"] == 0 else captured_b
        target.update(kwargs)
        counter["n"] += 1
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(name="a", model_concurrency_limiter=pool),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-a",
        )
        await build_pydantic_ai_agent(
            agent=_make_agent(name="b", model_concurrency_limiter=pool),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-b",
        )
    finally:
        pydantic_ai.Agent.__init__ = original_init  # ty: ignore[invalid-assignment]  # test seam restore

    a_lim = captured_a["model"]._limiter
    b_lim = captured_b["model"]._limiter
    assert a_lim is b_lim is pool


async def test_concurrency_wraps_outside_fallback() -> None:
    """When both fallback_models and a concurrency cap are set, the wrap order
    is FallbackModel inside, ConcurrencyLimitedModel outside — one limiter slot
    per agent run regardless of which fallback ultimately serves the request.
    """
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(
                model="test",
                fallback_models=("test",),
                max_concurrent_requests=4,
            ),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    forwarded = captured["model"]
    assert isinstance(forwarded, ConcurrencyLimitedModel)
    # The wrapped model is the FallbackModel — concurrency is the outer ring.
    assert isinstance(forwarded.wrapped, FallbackModel)


async def test_concurrency_limit_value_object_passes_through() -> None:
    """``ConcurrencyLimit`` (the dataclass with max_queued backpressure) is also
    a valid limiter input. Passing it via the limiter field threads through and
    materialises a ConcurrencyLimiter inside ConcurrencyLimitedModel."""
    limit = ConcurrencyLimit(max_running=5, max_queued=20)
    # ConcurrencyLimit isn't itself an AbstractConcurrencyLimiter, so the
    # public field type rejects it. Users who want backpressure pass it via a
    # ConcurrencyLimiter.from_limit(...) instead.
    with pytest.raises(ValidationError):
        _make_agent(model_concurrency_limiter=limit)  # type: ignore[arg-type]
    # The supported escape hatch:
    pool = ConcurrencyLimiter.from_limit(limit, name="bp")
    a = _make_agent(model_concurrency_limiter=pool)
    assert a.model_concurrency_limiter is pool


# ---------------------------------------------------------------------------
# Re-export surface
# ---------------------------------------------------------------------------


def test_models_module_reexports_limiter_classes() -> None:
    from murmur import models

    assert models.ConcurrencyLimiter is ConcurrencyLimiter
    assert "ConcurrencyLimit" in models.__all__
    assert "ConcurrencyLimiter" in models.__all__
    assert "AbstractConcurrencyLimiter" in models.__all__
