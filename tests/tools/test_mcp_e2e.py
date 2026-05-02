"""End-to-end tests for ``MCPToolsetProvider`` against a real MCP subprocess.

Drives ``mcp_stdio`` against the bundled stub server in
``_mcp_stub_server.py`` — exercising the actual MCP wire protocol
(initialize, list_tools, call_tool) rather than our in-process fakes.
``test_mcp.py`` covers the wrapper bookkeeping with mocks; this module
proves the wrapper composes correctly with PydanticAI's MCPServer
across all four ``ToolsetProviderContract`` axes plus the trust-gating
matrix from ``9mt.5``.

The stub MCP server runs as a subprocess on each test, so each test is
independent (no shared state). Tests are slower than the mock-backed
ones (~100 ms each for subprocess spawn) — kept here, not in
``integration``, because they don't need Docker.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs
from tests.contracts.toolset_provider_contract import ToolsetProviderContract

from murmur._dispatch import _resolve_mcp_toolsets
from murmur.agent import Agent
from murmur.tools.executor import ToolExecutor
from murmur.tools.mcp import MCPToolsetProvider
from murmur.tools.registry import ToolRegistry
from murmur.types import TrustLevel

_STUB_SERVER = str(Path(__file__).parent / "_mcp_stub_server.py")


def _make_provider(allow: list[str] | None = None) -> MCPToolsetProvider:
    """Build a provider against the bundled stub MCP server."""
    from murmur.tools.mcp import mcp_stdio

    return mcp_stdio(sys.executable, [_STUB_SERVER], allow=allow)


# ---------------------------------------------------------------------------
# Contract subclass
# ---------------------------------------------------------------------------


class TestMCPToolsetProviderContract(ToolsetProviderContract):
    """Run the shared ``ToolsetProviderContract`` against a real subprocess."""

    @pytest.fixture
    async def provider(self) -> AsyncIterator[MCPToolsetProvider]:
        p = _make_provider()
        try:
            yield p
        finally:
            # The contract drives start/stop itself; this is a safety net
            # for tests that fail before reaching their own ``stop``.
            await p.stop()


# ---------------------------------------------------------------------------
# Discovery + descriptor round-trip
# ---------------------------------------------------------------------------


class _Out(BaseModel):
    answer: str


def _agent(
    *,
    trust: TrustLevel = TrustLevel.MEDIUM,
    allow: list[str] | None = None,
) -> Agent:
    return Agent(
        name="researcher",
        model="test",
        instructions="...",
        output_type=_Out,
        mcp_servers=(_make_provider(allow=allow),),
        trust_level=trust,
    )


async def test_real_discovery_returns_two_tools_with_annotations() -> None:
    """End-to-end: stub server reports echo (read-only) + mutate (not)."""
    provider = _make_provider()
    try:
        descriptors = await provider.list_tools()
        by_name = {d.name: d for d in descriptors}
        assert set(by_name) == {"echo", "mutate"}
        assert by_name["echo"].read_only is True
        assert by_name["mutate"].read_only is False
        properties = by_name["echo"].input_schema["properties"]
        assert isinstance(properties, dict)
        assert "text" in properties
    finally:
        await provider.stop()


async def test_real_call_round_trips_through_subprocess() -> None:
    provider = _make_provider()
    try:
        result = await provider.call_tool("echo", {"text": "hello mcp"})
        assert "hello mcp" in str(result)
    finally:
        await provider.stop()


# ---------------------------------------------------------------------------
# Trust gating against the real server
# ---------------------------------------------------------------------------


async def test_real_sandbox_skips_provider() -> None:
    agent = _agent(trust=TrustLevel.SANDBOX)
    executor = ToolExecutor(ToolRegistry())
    toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert toolsets == []
    assert names == frozenset()


async def test_real_low_without_allow_skips_provider() -> None:
    agent = _agent(trust=TrustLevel.LOW)  # no allow= → safe default
    executor = ToolExecutor(ToolRegistry())
    toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert toolsets == []
    assert names == frozenset()


async def test_real_low_with_allow_exposes_only_listed() -> None:
    agent = _agent(trust=TrustLevel.LOW, allow=["echo"])
    executor = ToolExecutor(ToolRegistry())
    _toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert names == frozenset({"echo"})


async def test_real_medium_with_allow_narrows_exposed_tools() -> None:
    agent = _agent(trust=TrustLevel.MEDIUM, allow=["mutate"])
    executor = ToolExecutor(ToolRegistry())
    _toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert names == frozenset({"mutate"})


# ---------------------------------------------------------------------------
# Lifecycle event integration — call_tool flows through executor
# ---------------------------------------------------------------------------


async def test_real_call_via_dispatch_fires_lifecycle_events() -> None:
    """End-to-end: build the toolset wrapper from real provider, drive a call,
    confirm structlog ``tool_call_started`` / ``tool_call_completed`` fire."""
    agent = _agent(trust=TrustLevel.MEDIUM)
    executor = ToolExecutor(ToolRegistry())
    toolsets, _names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t-1"
    )
    assert len(toolsets) == 1
    wrapper = toolsets[0]

    with capture_logs() as captured:
        result = await wrapper.call_tool("echo", {"text": "hi"}, None, None)  # ty: ignore[invalid-argument-type]

    assert "hi" in str(result)
    events = [c["event"] for c in captured]
    assert "tool_call_started" in events
    assert "tool_call_completed" in events
    started = next(c for c in captured if c["event"] == "tool_call_started")
    assert started["tool_name"] == "echo"
    assert started["agent_name"] == "researcher"
    assert started["task_id"] == "t-1"
