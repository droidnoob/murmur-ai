"""Static bearer-token auth on :class:`AgentServer`.

Default surface is unauth'd (existing behaviour). When ``auth_token`` is set,
data routes return 401 unless the request carries
``Authorization: Bearer <token>``. Health probes always bypass.
"""

from __future__ import annotations

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


class _Echo(BaseModel):
    text: str


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
    server.register(
        Agent(
            name="echo",
            model="anthropic:claude-sonnet-4-6",
            instructions="echo",
            output_type=_Echo,
            trust_level=TrustLevel.SANDBOX,
            context_passer=NullContextPasser(),
        )
    )
    return server


async def _client(server: AgentServer) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url="http://test",
        transport=httpx.ASGITransport(app=server.app),
    )


# ---------------------------------------------------------------------------
# Default behaviour: no token configured → no auth required.
# ---------------------------------------------------------------------------


async def test_default_no_auth_required() -> None:
    server = _build_server(auth_token=None)
    async with await _client(server) as http:
        r = await http.get("/agents")
        assert r.status_code == 200
        assert "echo" in r.json()


# ---------------------------------------------------------------------------
# auth_token configured: missing / wrong / right header.
# ---------------------------------------------------------------------------


async def test_missing_header_returns_401() -> None:
    server = _build_server(auth_token="s3cret")
    async with await _client(server) as http:
        r = await http.get("/agents")
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") == "Bearer"
        body = r.json()
        assert body["error"] == "Unauthorized"


async def test_wrong_token_returns_401() -> None:
    server = _build_server(auth_token="s3cret")
    async with await _client(server) as http:
        r = await http.get("/agents", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401


async def test_correct_token_returns_200() -> None:
    server = _build_server(auth_token="s3cret")
    async with await _client(server) as http:
        r = await http.get("/agents", headers={"Authorization": "Bearer s3cret"})
        assert r.status_code == 200
        assert "echo" in r.json()


@pytest.mark.parametrize("path", ["/health", "/healthz", "/readyz"])
async def test_health_probes_bypass_auth(path: str) -> None:
    server = _build_server(auth_token="s3cret")
    async with await _client(server) as http:
        r = await http.get(path)
        assert r.status_code == 200


async def test_run_endpoint_protected() -> None:
    server = _build_server(auth_token="s3cret")
    async with await _client(server) as http:
        # Without header
        r1 = await http.post("/agents/echo/run", json={"task": {"input": "hi"}})
        assert r1.status_code == 401
        # With header — should reach the route
        r2 = await http.post(
            "/agents/echo/run",
            json={"task": {"input": "hi"}},
            headers={"Authorization": "Bearer s3cret"},
        )
        assert r2.status_code == 200
