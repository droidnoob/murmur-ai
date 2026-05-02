"""Tests for ``Agent.fallback_models`` (d3l).

Verifies the field round-trips, defaults to empty tuple, and gates the
construction of ``pydantic_ai.models.fallback.FallbackModel`` at dispatch.
The fallback semantics themselves (which exceptions trigger, ordering,
``FallbackExceptionGroup``) are upstream PydanticAI concerns; we only
assert the wiring here.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.fallback import FallbackModel

from murmur._dispatch import build_pydantic_ai_agent
from murmur.agent import Agent
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


# ---------------------------------------------------------------------------
# Field shape
# ---------------------------------------------------------------------------


def test_default_fallback_models_is_empty_tuple() -> None:
    a = _make_agent()
    assert a.fallback_models == ()


def test_fallback_models_stores_tuple_of_strings() -> None:
    a = _make_agent(fallback_models=("anthropic:claude-sonnet-4-6", "openai:gpt-5.2"))
    assert a.fallback_models == ("anthropic:claude-sonnet-4-6", "openai:gpt-5.2")


def test_fallback_models_field_is_frozen() -> None:
    """Immutability: post-construction assignment raises ValidationError."""
    from pydantic import ValidationError

    a = _make_agent(fallback_models=("openai:gpt-5.2",))
    with pytest.raises(ValidationError):
        a.fallback_models = ("anthropic:claude-sonnet-4-6",)


# ---------------------------------------------------------------------------
# Dispatch — constructs FallbackModel only when fallback_models is non-empty
# ---------------------------------------------------------------------------


async def test_dispatch_passes_string_model_when_no_fallbacks() -> None:
    """Empty fallback_models means model= is forwarded as-is (string passthrough)."""
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        agent = _make_agent()
        await build_pydantic_ai_agent(
            agent=agent,
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        pydantic_ai.Agent.__init__ = original_init  # ty: ignore[invalid-assignment]  # test seam restore

    # The string we passed propagates verbatim — no FallbackModel wrapping.
    assert captured["model"] == "test"
    assert not isinstance(captured["model"], FallbackModel)


async def test_dispatch_constructs_fallback_model_when_fallbacks_set() -> None:
    """Non-empty fallback_models triggers FallbackModel(primary, *fallbacks).

    Uses PydanticAI's ``test`` pseudo-model identifier for primary +
    fallbacks so the constructor doesn't try to authenticate with a real
    provider — we're testing the dispatch wiring, not the upstream API.
    """
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        agent = _make_agent(
            model="test",
            fallback_models=("test", "test"),
        )
        await build_pydantic_ai_agent(
            agent=agent,
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        pydantic_ai.Agent.__init__ = original_init  # ty: ignore[invalid-assignment]  # test seam restore

    forwarded = captured["model"]
    assert isinstance(forwarded, FallbackModel)
    # FallbackModel exposes the wrapped models via .models — order is
    # (primary, *fallbacks). With three "test" entries we expect three
    # TestModel instances.
    assert len(forwarded.models) == 3


async def test_dispatch_single_fallback_still_wraps() -> None:
    """Even a single-entry fallback list triggers FallbackModel wrapping —
    keeps the dispatch path uniform."""
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        agent = _make_agent(
            model="test",
            fallback_models=("test",),
        )
        await build_pydantic_ai_agent(
            agent=agent,
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        pydantic_ai.Agent.__init__ = original_init  # ty: ignore[invalid-assignment]  # test seam restore

    assert isinstance(captured["model"], FallbackModel)
    assert len(captured["model"].models) == 2  # primary + 1 fallback
