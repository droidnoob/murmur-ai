"""Tests for ``Agent.builtin_tools`` (lqh).

Verifies the field round-trips through Agent construction, defaults to an
empty tuple, and is forwarded to ``pydantic_ai.Agent(builtin_tools=...)``
as a defensive list copy at the dispatch boundary.

The provider-side execution itself is tested upstream by PydanticAI; we
only assert the wiring on Murmur's side.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
from pydantic import BaseModel
from pydantic_ai.builtin_tools import AbstractBuiltinTool

from murmur._dispatch import build_pydantic_ai_agent
from murmur.agent import Agent
from murmur.tools import (
    CodeExecutionTool,
    FileSearchTool,
    ImageGenerationTool,
    MCPServerTool,
    MemoryTool,
    WebFetchTool,
    WebSearchTool,
    XSearchTool,
)
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


# ---------------------------------------------------------------------------
# Field shape
# ---------------------------------------------------------------------------


def test_default_builtin_tools_is_empty_tuple() -> None:
    a = _make_agent()
    assert a.builtin_tools == ()


def test_builtin_tools_stores_tuple_of_instances() -> None:
    a = _make_agent(builtin_tools=(WebSearchTool(max_uses=5), CodeExecutionTool()))
    assert len(a.builtin_tools) == 2
    assert isinstance(a.builtin_tools[0], WebSearchTool)
    assert isinstance(a.builtin_tools[1], CodeExecutionTool)
    assert a.builtin_tools[0].max_uses == 5


def test_agent_remains_frozen_with_builtin_tools_set() -> None:
    """The new field doesn't break the frozen-Pydantic guarantee."""
    import pytest
    from pydantic import ValidationError

    a = _make_agent(builtin_tools=(WebSearchTool(),))
    with pytest.raises(ValidationError):
        a.builtin_tools = (CodeExecutionTool(),)


# ---------------------------------------------------------------------------
# Re-exports under murmur.tools — users don't import from pydantic_ai
# ---------------------------------------------------------------------------


def test_all_concrete_builtin_tools_are_re_exported() -> None:
    """Every provider-side tool PydanticAI exports has a Murmur re-export.

    Locks in the API contract that ``from murmur.tools import <Tool>`` works
    so Public API Rule (no user-facing pydantic_ai imports) is preserved.
    """
    expected = {
        WebSearchTool,
        XSearchTool,
        CodeExecutionTool,
        ImageGenerationTool,
        WebFetchTool,
        FileSearchTool,
        MemoryTool,
        MCPServerTool,
    }
    for cls in expected:
        # Each is a subclass of AbstractBuiltinTool (the umbrella type).
        assert issubclass(cls, AbstractBuiltinTool), cls


# ---------------------------------------------------------------------------
# Dispatch — forwarding to pydantic_ai.Agent(builtin_tools=...)
# ---------------------------------------------------------------------------


async def test_dispatch_forwards_builtin_tools_as_list() -> None:
    """Verifies ``build_pydantic_ai_agent`` forwards builtin_tools.

    PydanticAI's Agent stores them on a private structure that varies by
    version; we assert via the ``builtin_tools`` constructor kwarg by
    monkey-patching ``pydantic_ai.Agent.__init__`` to capture the kwargs.
    """
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        agent = _make_agent(
            builtin_tools=(WebSearchTool(max_uses=3), CodeExecutionTool()),
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

    assert "builtin_tools" in captured
    forwarded = captured["builtin_tools"]
    # Defensive copy: list, not the original tuple.
    assert isinstance(forwarded, list)
    assert len(forwarded) == 2
    assert isinstance(forwarded[0], WebSearchTool)
    assert forwarded[0].max_uses == 3


async def test_dispatch_forwards_empty_list_when_builtin_tools_empty() -> None:
    """PydanticAI iterates ``builtin_tools`` directly inside its ``__init__``,
    so the empty default must be an empty list — not None. Locks in the
    contract."""
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

    assert captured["builtin_tools"] == []


async def test_dispatch_defensive_copy_decouples_from_user_tuple() -> None:
    """The list passed to PydanticAI is a fresh copy — mutating either side
    after construction can't affect the other."""
    captured_lists: list[Any] = []
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured_lists.append(kwargs.get("builtin_tools"))
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    try:
        original_tuple = (WebSearchTool(),)
        agent = _make_agent(builtin_tools=original_tuple)
        await build_pydantic_ai_agent(
            agent=agent,
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        pydantic_ai.Agent.__init__ = original_init  # ty: ignore[invalid-assignment]  # test seam restore

    [forwarded] = captured_lists
    assert forwarded is not original_tuple  # different object
    assert isinstance(forwarded, list)
