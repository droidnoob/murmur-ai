"""Tests for :class:`murmur.tools.executor.ToolExecutor`.

Verifies the lifecycle events (``tool_call_started`` /
``tool_call_completed`` / ``tool_call_failed``) emit with the right shape,
and the trust-level + allow-list policy rejects the right calls.
"""

from __future__ import annotations

import pytest
from structlog.testing import capture_logs

from murmur.core.errors import RegistryError, ToolExecutionError, TrustViolationError
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import TrustLevel


def _registry_with(name: str = "web_search") -> ToolRegistry:
    """Tiny registry that returns a deterministic value for the named tool."""

    async def _impl(query: str) -> str:
        return f"results: {query}"

    reg = ToolRegistry()
    reg.register(name, _impl)
    return reg


# ---------------------------------------------------------------------------
# Lifecycle events
# ---------------------------------------------------------------------------


async def test_success_path_emits_started_then_completed() -> None:
    executor = ToolExecutor(_registry_with("web_search"))
    with capture_logs() as captured:
        result = await executor.execute(
            agent_name="r",
            task_id="t-1",
            trust_level=TrustLevel.LOW,  # web_search is read-only
            allowed=frozenset({"web_search"}),
            name="web_search",
            args={"query": "k"},
        )
    assert result == "results: k"
    events = [c["event"] for c in captured]
    assert events == ["tool_call_started", "tool_call_completed"]
    started = next(c for c in captured if c["event"] == "tool_call_started")
    assert started["agent_name"] == "r"
    assert started["task_id"] == "t-1"
    assert started["tool_name"] == "web_search"
    assert started["trust_level"] == "low"


async def test_failing_tool_emits_failed_event_and_raises() -> None:
    async def _broken() -> None:
        raise RuntimeError("kaboom")

    reg = ToolRegistry()
    reg.register("web_search", _broken)
    executor = ToolExecutor(reg)

    with capture_logs() as captured, pytest.raises(ToolExecutionError, match="kaboom"):
        await executor.execute(
            agent_name="r",
            task_id="t-2",
            trust_level=TrustLevel.HIGH,
            allowed=frozenset({"web_search"}),
            name="web_search",
            args={},
        )
    events = [c["event"] for c in captured]
    assert events == ["tool_call_started", "tool_call_failed"]
    failed = next(c for c in captured if c["event"] == "tool_call_failed")
    assert failed["error"] == "kaboom"
    assert failed["tool_name"] == "web_search"


# ---------------------------------------------------------------------------
# Trust-level enforcement
# ---------------------------------------------------------------------------


async def test_sandbox_trust_rejects_all_tools() -> None:
    executor = ToolExecutor(_registry_with("web_search"))
    with pytest.raises(TrustViolationError, match="SANDBOX"):
        await executor.execute(
            agent_name="r",
            task_id="t-3",
            trust_level=TrustLevel.SANDBOX,
            allowed=frozenset({"web_search"}),
            name="web_search",
            args={"query": "k"},
        )


async def test_low_trust_rejects_non_readonly_tool() -> None:
    executor = ToolExecutor(_registry_with("write_file"))
    with pytest.raises(TrustViolationError, match="not read-only"):
        await executor.execute(
            agent_name="r",
            task_id="t-4",
            trust_level=TrustLevel.LOW,
            allowed=frozenset({"write_file"}),
            name="write_file",
            args={},
        )


async def test_low_trust_allows_readonly_tool() -> None:
    """``web_search`` is in the built-in read-only set."""
    executor = ToolExecutor(_registry_with("web_search"))
    result = await executor.execute(
        agent_name="r",
        task_id="t-5",
        trust_level=TrustLevel.LOW,
        allowed=frozenset({"web_search"}),
        name="web_search",
        args={"query": "k"},
    )
    assert result == "results: k"


async def test_high_trust_still_respects_allow_list() -> None:
    """Trust level alone doesn't bypass the per-agent allow-list."""
    executor = ToolExecutor(_registry_with("write_file"))
    with pytest.raises(TrustViolationError, match="not in the allow-list"):
        await executor.execute(
            agent_name="r",
            task_id="t-6",
            trust_level=TrustLevel.HIGH,
            allowed=frozenset(),  # empty allow-list
            name="write_file",
            args={},
        )


# ---------------------------------------------------------------------------
# Registry edge case
# ---------------------------------------------------------------------------


async def test_unknown_tool_in_registry_raises_registry_error() -> None:
    """Tool was allow-listed but not registered — RegistryError, not Trust."""
    executor = ToolExecutor(ToolRegistry())  # empty registry
    with pytest.raises(RegistryError, match="not found"):
        await executor.execute(
            agent_name="r",
            task_id="t-7",
            trust_level=TrustLevel.HIGH,
            allowed=frozenset({"missing"}),
            name="missing",
            args={},
        )
