"""MCP server adapter — wraps :class:`mcp.server.fastmcp.FastMCP`.

Constructed lazily by :meth:`AgentServer.serve_mcp` so the heavy
``mcp`` SDK only loads when the operator actually opts in. Importing
this module raises ``ImportError`` (with a clear message) when the
``murmur-runtime[mcp-server]`` extra isn't installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from murmur.mcp_server._bridge import make_agent_tool

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover — exercised when extra missing
    raise ImportError(
        "The murmur-runtime[mcp-server] extra is required to expose agents over MCP. "
        "Install it with: pip install 'murmur-runtime[mcp-server]'"
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
    auth_token: str | None = None,
) -> None:
    """Build the MCP server and run it on the chosen transport.

    Blocks until the transport's run loop exits (Ctrl-C for stdio;
    standard ASGI shutdown for HTTP). Caller's responsibility to wrap
    in ``asyncio.run`` or compose with their own event loop.

    ``auth_token`` (HTTP transport only): when set, requests must carry
    ``Authorization: Bearer <token>`` or get a 401. Stdio is local-only
    by definition so the token is ignored there.
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
        if auth_token is None:
            await fastmcp.run_streamable_http_async()
        else:
            # Replicate ``run_streamable_http_async`` so we can wrap the
            # Starlette app with bearer-token middleware before serving.
            import uvicorn

            starlette_app = fastmcp.streamable_http_app()
            _install_bearer_auth(starlette_app, expected_token=auth_token)
            config = uvicorn.Config(
                starlette_app,
                host=host,
                port=port,
                log_level=fastmcp.settings.log_level.lower(),
            )
            await uvicorn.Server(config).serve()
    else:  # pragma: no cover — Literal narrowing prevents reaching here
        raise ValueError(f"unknown transport: {transport!r}")


def _install_bearer_auth(starlette_app: Any, *, expected_token: str) -> None:
    """Wrap ``starlette_app`` with a 401 guard that requires
    ``Authorization: Bearer <expected_token>`` on every request.

    Implemented as a Starlette pure-ASGI middleware via ``add_middleware``.
    No path carve-outs — MCP HTTP doesn't expose health probes, every
    request is data.
    """
    from starlette.middleware import Middleware
    from starlette.responses import JSONResponse
    from starlette.types import ASGIApp, Receive, Scope, Send

    expected = f"Bearer {expected_token}"

    class _BearerAuth:
        def __init__(self, app: ASGIApp) -> None:
            self.app = app

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] != "http":
                await self.app(scope, receive, send)
                return
            headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in scope.get("headers", [])
            }
            if headers.get("authorization") != expected:
                resp = JSONResponse(
                    status_code=401,
                    content={"error": "Unauthorized"},
                    headers={"WWW-Authenticate": "Bearer"},
                )
                await resp(scope, receive, send)
                return
            await self.app(scope, receive, send)

    # ``user_middleware`` is the public-ish slot Starlette consumes when
    # building the ASGI stack. We append rather than re-construct to avoid
    # disturbing whatever middleware FastMCP installed itself.
    starlette_app.user_middleware.append(Middleware(_BearerAuth))
    # Force a fresh middleware stack on next request.
    starlette_app.middleware_stack = None


__all__ = ["build_fastmcp", "serve"]
