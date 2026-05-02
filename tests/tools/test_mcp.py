"""Unit tests for ``murmur.tools.mcp``.

Two slices live here:

1. **Factory wiring** — ``mcp_stdio`` / ``mcp_http`` / ``mcp_sse`` produce
   an :class:`MCPToolsetProvider` wrapping the right PydanticAI server
   subclass with the right transport config.
2. **Wrapper bookkeeping** — start / stop idempotence, pre-state guards,
   list_tools caching, call_tool name pre-check, exception translation.
   The PydanticAI ``MCPServer`` is replaced by a tiny in-process fake so
   we exercise the wrapper logic without spawning a subprocess.

End-to-end behaviour against a real stub MCP server is covered by
``tests/tools/test_mcp_e2e.py`` (``9mt.7``) which subclasses
``ToolsetProviderContract``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from mcp.shared.exceptions import ErrorData, McpError
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.mcp import MCPServerSSE, MCPServerStdio, MCPServerStreamableHTTP

from murmur.core.errors import ToolExecutionError
from murmur.core.protocols.toolsets import ToolDescriptor
from murmur.tools.mcp import (
    MCPToolsetProvider,
    mcp_http,
    mcp_sse,
    mcp_stdio,
)

# ---------------------------------------------------------------------------
# Stub PydanticAI MCPServer
# ---------------------------------------------------------------------------


class _FakeMCPTool:
    """Stand-in for ``mcp.types.Tool`` shaped to what ``_to_descriptor`` reads."""

    def __init__(
        self,
        name: str,
        description: str | None,
        input_schema: dict[str, Any],
        read_only: bool = False,
    ) -> None:
        self.name = name
        self.description = description
        self.inputSchema = input_schema
        self.annotations = _FakeAnnotations(read_only) if read_only else None


class _FakeAnnotations:
    def __init__(self, read_only: bool) -> None:
        self.readOnlyHint = read_only


class _FakeMCPServer:
    """In-process stand-in for ``pydantic_ai.mcp.MCPServer``.

    Mirrors only the surface :class:`MCPToolsetProvider` consumes —
    ``__aenter__`` / ``__aexit__`` / ``list_tools`` / ``direct_call_tool``.
    """

    def __init__(
        self,
        tools: list[_FakeMCPTool] | None = None,
        *,
        raise_on_call: BaseException | None = None,
    ) -> None:
        self._tools = tools or [
            _FakeMCPTool(
                name="echo",
                description="Echo the text back.",
                input_schema={
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                },
            ),
        ]
        self._raise_on_call = raise_on_call
        self.enter_count = 0
        self.exit_count = 0
        self.list_tools_count = 0
        self.call_log: list[tuple[str, Mapping[str, object]]] = []

    async def __aenter__(self) -> _FakeMCPServer:
        self.enter_count += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        self.exit_count += 1

    async def list_tools(self) -> list[_FakeMCPTool]:
        self.list_tools_count += 1
        return self._tools

    async def direct_call_tool(
        self,
        name: str,
        args: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> object:
        self.call_log.append((name, args))
        if self._raise_on_call is not None:
            raise self._raise_on_call
        return f"echoed: {args.get('text')}"


def _provider(server: _FakeMCPServer | None = None) -> MCPToolsetProvider:
    """Build an :class:`MCPToolsetProvider` wrapping a fake MCPServer."""
    fake = server or _FakeMCPServer()
    # The fake matches the surface MCPToolsetProvider actually consumes.
    return MCPToolsetProvider(fake)  # ty: ignore[invalid-argument-type]


# ---------------------------------------------------------------------------
# Factory wiring
# ---------------------------------------------------------------------------


def test_mcp_stdio_builds_stdio_server() -> None:
    provider = mcp_stdio("npx", ["@modelcontextprotocol/server-everything"])
    assert isinstance(provider, MCPToolsetProvider)
    assert isinstance(provider._mcp, MCPServerStdio)
    assert provider._mcp.command == "npx"
    assert list(provider._mcp.args) == ["@modelcontextprotocol/server-everything"]


def test_mcp_stdio_passes_env_and_cwd() -> None:
    provider = mcp_stdio(
        "echo",
        ["hi"],
        env={"FOO": "bar"},
        cwd="/tmp",
    )
    assert isinstance(provider._mcp, MCPServerStdio)
    assert provider._mcp.env == {"FOO": "bar"}
    assert str(provider._mcp.cwd) == "/tmp"


def test_mcp_stdio_default_env_is_none() -> None:
    provider = mcp_stdio("echo", ["hi"])
    assert isinstance(provider._mcp, MCPServerStdio)
    assert provider._mcp.env is None


def test_mcp_http_builds_streamable_http_server() -> None:
    provider = mcp_http("http://localhost:7000/mcp")
    assert isinstance(provider._mcp, MCPServerStreamableHTTP)
    assert provider._mcp.url == "http://localhost:7000/mcp"


def test_mcp_http_passes_headers() -> None:
    provider = mcp_http(
        "http://localhost:7000/mcp",
        headers={"Authorization": "Bearer x"},
    )
    assert isinstance(provider._mcp, MCPServerStreamableHTTP)
    assert provider._mcp.headers == {"Authorization": "Bearer x"}


def test_mcp_sse_builds_sse_server() -> None:
    provider = mcp_sse("http://localhost:7001/sse")
    assert isinstance(provider._mcp, MCPServerSSE)
    assert provider._mcp.url == "http://localhost:7001/sse"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_enters_underlying_server_once() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)

    await provider.start()
    await provider.start()  # idempotent

    assert fake.enter_count == 1


async def test_stop_exits_underlying_server_once() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)

    await provider.start()
    await provider.stop()
    await provider.stop()  # idempotent

    assert fake.exit_count == 1


async def test_stop_without_start_is_a_noop() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)

    await provider.stop()

    assert fake.exit_count == 0


async def test_restart_after_stop_re_enters() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)

    await provider.start()
    await provider.stop()
    await provider.start()

    assert fake.enter_count == 2
    assert fake.exit_count == 1


# ---------------------------------------------------------------------------
# Pre-state guards
# ---------------------------------------------------------------------------


async def test_list_tools_works_without_start() -> None:
    """``MCPServer.list_tools`` manages its own context — start is opt-in."""
    fake = _FakeMCPServer()
    provider = _provider(fake)
    tools = await provider.list_tools()
    assert len(tools) == 1
    assert tools[0].name == "echo"


async def test_call_tool_works_without_start() -> None:
    """``MCPServer.direct_call_tool`` manages its own context — start is opt-in."""
    fake = _FakeMCPServer()
    provider = _provider(fake)
    result = await provider.call_tool("echo", {"text": "hi"})
    assert result == "echoed: hi"


# ---------------------------------------------------------------------------
# list_tools
# ---------------------------------------------------------------------------


async def test_list_tools_returns_descriptors() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)
    await provider.start()
    try:
        tools = await provider.list_tools()
        assert len(tools) == 1
        assert isinstance(tools[0], ToolDescriptor)
        assert tools[0].name == "echo"
        properties = tools[0].input_schema["properties"]
        assert isinstance(properties, dict)
        assert "text" in properties
    finally:
        await provider.stop()


async def test_list_tools_caches_after_first_call() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)
    await provider.start()
    try:
        await provider.list_tools()
        await provider.list_tools()
        await provider.list_tools()
        assert fake.list_tools_count == 1
    finally:
        await provider.stop()


async def test_list_tools_cache_clears_on_stop() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)
    await provider.start()
    await provider.list_tools()
    await provider.stop()
    await provider.start()
    try:
        await provider.list_tools()
        assert fake.list_tools_count == 2
    finally:
        await provider.stop()


async def test_list_tools_translates_read_only_annotation() -> None:
    fake = _FakeMCPServer(
        tools=[
            _FakeMCPTool(
                name="read_file",
                description="Read.",
                input_schema={"type": "object"},
                read_only=True,
            ),
            _FakeMCPTool(
                name="write_file",
                description="Write.",
                input_schema={"type": "object"},
                read_only=False,
            ),
        ]
    )
    provider = _provider(fake)
    await provider.start()
    try:
        tools = {t.name: t for t in await provider.list_tools()}
        assert tools["read_file"].read_only is True
        assert tools["write_file"].read_only is False
    finally:
        await provider.stop()


# ---------------------------------------------------------------------------
# call_tool
# ---------------------------------------------------------------------------


async def test_call_tool_round_trips() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)
    await provider.start()
    try:
        result = await provider.call_tool("echo", {"text": "world"})
        assert result == "echoed: world"
        assert fake.call_log == [("echo", {"text": "world"})]
    finally:
        await provider.stop()


async def test_call_tool_unknown_name_raises_without_round_trip() -> None:
    fake = _FakeMCPServer()
    provider = _provider(fake)
    await provider.start()
    try:
        with pytest.raises(ToolExecutionError, match="unknown MCP tool"):
            await provider.call_tool("does_not_exist", {})
        assert fake.call_log == []
    finally:
        await provider.stop()


async def test_call_tool_translates_model_retry() -> None:
    fake = _FakeMCPServer(raise_on_call=ModelRetry("server hiccup"))
    provider = _provider(fake)
    await provider.start()
    try:
        with pytest.raises(ToolExecutionError, match="server hiccup"):
            await provider.call_tool("echo", {"text": "x"})
    finally:
        await provider.stop()


async def test_call_tool_translates_mcp_error() -> None:
    fake = _FakeMCPServer(
        raise_on_call=McpError(ErrorData(code=-32603, message="boom"))
    )
    provider = _provider(fake)
    await provider.start()
    try:
        with pytest.raises(ToolExecutionError) as excinfo:
            await provider.call_tool("echo", {"text": "x"})
        assert excinfo.value.__cause__ is not None
    finally:
        await provider.stop()
