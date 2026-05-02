"""Stdio MCP server used by ``test_mcp_e2e.py``.

A minimal real MCP server with two tools (``echo``, marked read-only;
``mutate``, not read-only) so the contract suite can exercise the actual
MCP wire protocol — initialize, list, call. Run as a subprocess via
``mcp_stdio(sys.executable, [<path to this file>])``.

Kept separate from the test module so pytest's collection doesn't pick
it up — the leading underscore makes pytest skip it.
"""

from __future__ import annotations

import asyncio

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

server: Server = Server("murmur-test-stub")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="echo",
            description="Echo the text back.",
            inputSchema={
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
            annotations=types.ToolAnnotations(readOnlyHint=True),
        ),
        types.Tool(
            name="mutate",
            description="Pretend to mutate state.",
            inputSchema={
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
            },
            annotations=types.ToolAnnotations(readOnlyHint=False),
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    if name == "echo":
        return [types.TextContent(type="text", text=f"echoed: {arguments.get('text')}")]
    if name == "mutate":
        return [types.TextContent(type="text", text=f"mutated: {arguments.get('key')}")]
    raise ValueError(f"unknown tool: {name}")


async def _main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(_main())
