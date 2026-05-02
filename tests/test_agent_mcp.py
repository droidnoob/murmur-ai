"""Tests for ``Agent.mcp_servers`` field shape (``9mt.4``)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pydantic import BaseModel

from murmur.agent import Agent
from murmur.core.protocols.toolsets import ToolDescriptor
from murmur.types import TrustLevel


class _Out(BaseModel):
    text: str


class _StubProvider:
    """Tiny in-process toolset provider used as a fixture."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        return ()

    async def call_tool(self, name: str, args: Mapping[str, object]) -> object:
        return f"called {name} with {dict(args)}"


def test_default_mcp_servers_is_empty_tuple() -> None:
    a = Agent(
        name="x",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
    )
    assert a.mcp_servers == ()


def test_agent_holds_mcp_servers_as_frozen_tuple() -> None:
    p = _StubProvider()
    q = _StubProvider()
    a = Agent(
        name="x",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        mcp_servers=(p, q),
        trust_level=TrustLevel.MEDIUM,
    )
    assert a.mcp_servers == (p, q)
    assert isinstance(a.mcp_servers, tuple)


def test_with_replaces_mcp_servers() -> None:
    a = Agent(
        name="x",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
    )
    p = _StubProvider()
    a2 = a.with_(mcp_servers=(p,))
    assert a.mcp_servers == ()
    assert a2.mcp_servers == (p,)
