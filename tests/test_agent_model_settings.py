"""Tests for ``Agent.model_settings`` (t16)."""

from __future__ import annotations

from typing import Any, cast

import pydantic_ai
import pytest
from pydantic import BaseModel, ValidationError

from murmur._dispatch import build_pydantic_ai_agent
from murmur.agent import Agent
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry


class _Out(BaseModel):
    text: str


def _make_agent(**kwargs: Any) -> Agent:
    return Agent(
        name="r",
        model="test",
        instructions="...",
        output_type=_Out,
        **kwargs,
    )


# ---- field shape ----------------------------------------------------------


def test_default_model_settings_is_none() -> None:
    a = _make_agent()
    assert a.model_settings is None


def test_model_settings_stores_dict() -> None:
    a = _make_agent(model_settings={"temperature": 0.0, "max_tokens": 1024})
    assert a.model_settings == {"temperature": 0.0, "max_tokens": 1024}


def test_model_settings_is_immutable_through_with_() -> None:
    a = _make_agent(model_settings={"temperature": 0.5})
    a2 = a.with_(model_settings={"temperature": 0.9})
    assert a.model_settings == {"temperature": 0.5}
    assert a2.model_settings == {"temperature": 0.9}


def test_model_settings_field_is_frozen() -> None:
    a = _make_agent(model_settings={"temperature": 0.0})
    with pytest.raises(ValidationError):
        a.model_settings = {"temperature": 1.0}  # type: ignore[misc]


# ---- forwarding to PydanticAI --------------------------------------------


async def test_build_forwards_model_settings_to_pydantic_ai() -> None:
    """Non-None model_settings reaches the constructed pa_agent."""
    agent = _make_agent(model_settings={"temperature": 0.0, "max_tokens": 256})
    registry = ToolRegistry()
    executor = ToolExecutor(registry)

    pa_agent = await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-1",
    )
    # PydanticAI keeps model_settings on the agent object. Cast through to
    # ``dict`` because PA's static type is ``ModelSettings | Callable[...]``
    # and ty can't narrow the TypedDict cleanly via isinstance.
    assert isinstance(pa_agent, pydantic_ai.Agent)
    settings_view = cast(dict[str, object], pa_agent.model_settings)
    assert settings_view.get("temperature") == 0.0
    assert settings_view.get("max_tokens") == 256


async def test_build_passes_none_when_unset() -> None:
    """Default path: agent without model_settings hits the existing PA default."""
    agent = _make_agent()
    registry = ToolRegistry()
    executor = ToolExecutor(registry)

    pa_agent = await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-1",
    )
    # PA's own default — None, not an empty dict.
    assert pa_agent.model_settings is None


async def test_build_copies_mapping_so_caller_cannot_mutate_via_agent() -> None:
    """The Mapping passed to PA is a fresh dict, not a shared reference.

    Defends against subtle bugs where a user reuses a settings dict and
    mutates it later, expecting the agent's frozen view to be unchanged.
    """
    settings: dict[str, object] = {"temperature": 0.0}
    agent = _make_agent(model_settings=settings)
    registry = ToolRegistry()
    executor = ToolExecutor(registry)

    pa_agent = await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-1",
    )
    settings["temperature"] = 1.0  # mutation post-build
    # PA's copy stays at 0.0 — we converted Mapping → dict at the boundary.
    settings_view = cast(dict[str, object], pa_agent.model_settings)
    assert settings_view.get("temperature") == 0.0
