"""MCP server adapter — wraps :class:`mcp.server.fastmcp.FastMCP`.

Constructed lazily by :meth:`AgentServer.serve_mcp` so the heavy
``mcp`` SDK only loads when the operator actually opts in. Importing
this module raises ``ImportError`` (with a clear message) when the
``murmur-ai[mcp-server]`` extra isn't installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from murmur.mcp_server._bridge import make_agent_tool

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover — exercised when extra missing
    raise ImportError(
        "The murmur-ai[mcp-server] extra is required to expose agents over MCP. "
        "Install it with: pip install 'murmur-ai[mcp-server]'"
    ) from exc

if TYPE_CHECKING:
    from collections.abc import Iterable

    from murmur.mcp_server._enrollment import MCPEnrollment
    from murmur.runtime import AgentRuntime


def build_fastmcp(
    *,
    runtime: AgentRuntime,
    enrollments: Iterable[MCPEnrollment],
    server_name: str = "murmur",
    instructions: str | None = None,
) -> FastMCP:
    """Construct a configured :class:`FastMCP` for the given enrollments.

    Each enrolled agent becomes one tool on the server with the
    enrollment's ``tool_name`` and ``description``. The tool's input
    schema is auto-derived from the wrapper's signature
    (``input: str``); the output is the agent's
    ``output_type.model_dump()`` dict.
    """
    fastmcp: FastMCP = FastMCP(name=server_name, instructions=instructions)
    for enrollment in enrollments:
        fastmcp.add_tool(
            fn=make_agent_tool(runtime=runtime, enrollment=enrollment),
            name=enrollment.tool_name,
            description=enrollment.description,
        )
    return fastmcp


async def serve(
    *,
    runtime: AgentRuntime,
    enrollments: Iterable[MCPEnrollment],
    transport: Literal["stdio", "http"],
    server_name: str = "murmur",
    instructions: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> None:
    """Build the MCP server and run it on the chosen transport.

    Blocks until the transport's run loop exits (Ctrl-C for stdio;
    standard ASGI shutdown for HTTP). Caller's responsibility to wrap
    in ``asyncio.run`` or compose with their own event loop.
    """
    fastmcp = build_fastmcp(
        runtime=runtime,
        enrollments=enrollments,
        server_name=server_name,
        instructions=instructions,
    )
    if transport == "stdio":
        await fastmcp.run_stdio_async()
    elif transport == "http":
        # FastMCP's host / port are set on construction; thread them through.
        fastmcp.settings.host = host
        fastmcp.settings.port = port
        await fastmcp.run_streamable_http_async()
    else:  # pragma: no cover — Literal narrowing prevents reaching here
        raise ValueError(f"unknown transport: {transport!r}")


__all__ = ["build_fastmcp", "serve"]
