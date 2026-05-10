"""Expose Murmur agents as an MCP server.

The inverse of ``mcp.py``. Where that file shows a Murmur :class:`Agent`
*consuming* an external MCP server, this one shows :class:`AgentServer`
*hosting* a Murmur agent so MCP clients (Claude Desktop, Cursor, MCP
Inspector, …) can call it as a tool.

Two opt-in steps gate exposure:

1. ``server.register_mcp(agent, tool_name=...)`` enrolls the agent.
2. ``server.serve_mcp(transport="stdio" | "http")`` activates the surface.

Constructing :class:`AgentServer` alone never starts MCP.

Run modes:

* ``stdio`` — the conventional mode for local MCP clients. The client
  spawns this script as a subprocess and talks JSON-RPC over stdin/stdout.
* ``http`` — Streamable-HTTP transport, useful for remote clients.
  Default bind is ``127.0.0.1:8765``.

Pairs with: ``examples/mcp.py`` (consume side), ``docs/concepts/mcp.md``.

Prereqs:
    pip install 'murmur-runtime[mcp-server]'
    export ANTHROPIC_API_KEY=...

Run (HTTP for easy curl/Inspector testing):
    python examples/mcp_server.py http

Run (stdio for Claude Desktop / Cursor):
    python examples/mcp_server.py stdio
"""

import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TrustLevel
from murmur.server.app import AgentServer


class CapitalLookup(BaseModel):
    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)


async def main(transport: str) -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2
    if transport not in {"stdio", "http"}:
        print(
            f"unknown transport {transport!r}; use 'stdio' or 'http'",
            file=sys.stderr,
        )
        return 2

    geographer = Agent(
        name="geographer",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions=(
            "You answer geography questions. Return the country, its capital, "
            "and your confidence (0..1)."
        ),
        output_type=CapitalLookup,
        trust_level=TrustLevel.LOW,
    )

    runtime = AgentRuntime()
    server = AgentServer(runtime=runtime)

    # Enroll for MCP. ``tool_name`` is what the calling LLM sees; default
    # would be ``geographer`` — we override to a more verb-y name. The
    # description is what the client-side LLM reads to pick this tool, so
    # be specific.
    server.register_mcp(
        geographer,
        tool_name="lookup_capital",
        description="Look up the capital city of a given country.",
    )

    if transport == "stdio":
        # Blocks until stdin closes (Ctrl-D) or the client disconnects.
        await server.serve_mcp(transport="stdio", server_name="murmur-geography")
    else:
        # Blocks until SIGTERM / SIGINT. Endpoint: POST http://127.0.0.1:8765/mcp
        print("MCP HTTP server on http://127.0.0.1:8765/mcp  (Ctrl-C to stop)")
        await server.serve_mcp(
            transport="http",
            server_name="murmur-geography",
            host="127.0.0.1",
            port=8765,
        )
    return 0


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "stdio"
    sys.exit(asyncio.run(main(arg)))
