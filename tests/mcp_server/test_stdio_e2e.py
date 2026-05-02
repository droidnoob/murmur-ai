"""End-to-end stdio smoke test for ``AgentServer.serve_mcp``.

Spawns ``_stub_server.py`` as a real subprocess, connects with the
official MCP Python SDK's stdio client, runs the standard handshake,
enumerates tools, and invokes one. Proves the wire format works
against a real MCP client without mocking the transport.

Marked ``slow`` so the standard ``pytest -m "not integration"`` run
still picks it up but the gate stays under 30s. Skips cleanly if
the ``mcp-server`` extra isn't available — the tests under
``tests/mcp_server/`` would have already failed at import time
elsewhere if that were the case.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp.client.stdio")

from mcp import ClientSession  # noqa: E402
from mcp.client.stdio import StdioServerParameters, stdio_client  # noqa: E402

_STUB = Path(__file__).resolve().parent / "_stub_server.py"


@pytest.mark.asyncio
async def test_stdio_round_trip() -> None:
    """Spawn the stub server, list tools, call one, verify the response."""
    params = StdioServerParameters(command=sys.executable, args=[str(_STUB)])

    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()

        # ---- list_tools — the enrolled agent appears with the right shape
        tools = await session.list_tools()
        names = {t.name for t in tools.tools}
        assert "echo" in names
        echo_tool = next(t for t in tools.tools if t.name == "echo")
        assert echo_tool.description == "Echo a short answer for the given input."
        schema = echo_tool.inputSchema
        assert "input" in schema.get("properties", {})
        assert schema["properties"]["input"]["type"] == "string"

        # ---- call_tool — round-trip succeeds, structured payload returns
        result = await session.call_tool("echo", {"input": "hello"})
        assert not result.isError, f"tool call errored: {result}"
        # PydanticAI's test pseudo-model fills `answer` with synthetic
        # but validating data; we don't assert content, just that the
        # field is present and the wire encoding survived intact.
        structured = result.structuredContent
        assert structured is not None
        assert "answer" in structured
        assert isinstance(structured["answer"], str)
