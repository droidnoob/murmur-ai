"""Auth on the MCP HTTP transport.

Boots ``AgentServer.serve_mcp(transport='http', auth_token=...)`` on a
random local port, then talks to it with raw httpx — no header → 401,
right header → at least the MCP handshake reaches the server (verified
by the response code being one FastMCP would emit, not 401).
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from typing import Any

import httpx
import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import TrustLevel

pytest.importorskip("mcp.server.fastmcp")


class _Echo(BaseModel):
    text: str


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _build_server(*, auth_token: str | None) -> AgentServer:
    backend = AsyncBackend()

    async def _build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Echo(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend._build_pa_agent = _build  # ty: ignore[invalid-assignment]  # test seam
    runtime = AgentRuntime(backend=backend)
    server = AgentServer(runtime=runtime, auth_token=auth_token)
    server.register_mcp(
        Agent(
            name="echo",
            model="anthropic:claude-sonnet-4-6",
            instructions="echo",
            output_type=_Echo,
            trust_level=TrustLevel.SANDBOX,
            context_passer=NullContextPasser(),
        ),
        tool_name="echo",
        description="Echo back.",
    )
    return server


@contextlib.asynccontextmanager
async def _running_mcp(server: AgentServer, port: int):
    task = asyncio.create_task(
        server.serve_mcp(transport="http", host="127.0.0.1", port=port)
    )
    # Poll the port (raw socket so we don't trip MCP's session machinery just
    # to detect "is uvicorn listening yet").
    loop = asyncio.get_event_loop()
    deadline = loop.time() + 10.0
    while loop.time() < deadline:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            break
        except (ConnectionRefusedError, OSError):
            await asyncio.sleep(0.05)
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task


_INIT_BODY = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "auth-test", "version": "0"},
    },
}
_INIT_HEADERS = {
    "content-type": "application/json",
    "accept": "application/json, text/event-stream",
}


@pytest.mark.asyncio
async def test_mcp_http_missing_token_returns_401() -> None:
    server = _build_server(auth_token="s3cret")
    port = _free_port()
    async with _running_mcp(server, port):
        async with httpx.AsyncClient(timeout=5.0) as http:
            r = await http.post(
                f"http://127.0.0.1:{port}/mcp",
                json=_INIT_BODY,
                headers=_INIT_HEADERS,
            )
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") == "Bearer"


@pytest.mark.asyncio
async def test_mcp_http_correct_token_passes_auth_layer() -> None:
    server = _build_server(auth_token="s3cret")
    port = _free_port()
    async with _running_mcp(server, port):
        async with httpx.AsyncClient(timeout=5.0) as http:
            r = await http.post(
                f"http://127.0.0.1:{port}/mcp",
                json=_INIT_BODY,
                headers={**_INIT_HEADERS, "authorization": "Bearer s3cret"},
            )
        # FastMCP responds with 200 OK and an SSE/JSON event stream on
        # successful initialise. Anything other than 401 means our auth
        # middleware let the request through to FastMCP.
        assert r.status_code != 401
        assert r.status_code < 500


# Note: ``auth_token=None`` against the MCP HTTP transport is already
# covered by ``test_stdio_e2e.py`` (stdio path) and the existing
# ``test_fastmcp_construction.py`` (no-auth construction). We don't repeat
# that path here because FastMCP's streamable-HTTP session manager carries
# enough module-level state that two consecutive ``run_streamable_http_async``
# instances in the same event loop can clash.
