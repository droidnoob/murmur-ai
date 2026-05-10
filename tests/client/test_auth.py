"""``MurmurClient(auth_token=...)`` round-trips through the bearer-token
guard on :class:`AgentServer`.

Verifies the matched-token path returns 200 and the wrong-token path
returns 401 (surfaced as :class:`MurmurError`)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pydantic_ai
import pytest
from murmur_client.client import MurmurClient
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import MurmurError
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import TrustLevel


class _Echo(BaseModel):
    text: str


@pytest.fixture
def server() -> AgentServer:
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
    s = AgentServer(runtime=runtime, auth_token="s3cret")
    s.register(
        Agent(
            name="echo",
            model="anthropic:claude-sonnet-4-6",
            instructions="echo",
            output_type=_Echo,
            trust_level=TrustLevel.SANDBOX,
            context_passer=NullContextPasser(),
        )
    )
    return s


async def _client(
    server: AgentServer, token: str | None
) -> AsyncIterator[MurmurClient]:
    transport = httpx.ASGITransport(app=server.app)
    async with MurmurClient("http://test", transport=transport, auth_token=token) as c:
        yield c


@pytest.fixture
async def authed_client(server: AgentServer) -> AsyncIterator[MurmurClient]:
    async for c in _client(server, "s3cret"):
        yield c


@pytest.fixture
async def wrong_token_client(server: AgentServer) -> AsyncIterator[MurmurClient]:
    async for c in _client(server, "wrong"):
        yield c


@pytest.fixture
async def no_token_client(server: AgentServer) -> AsyncIterator[MurmurClient]:
    async for c in _client(server, None):
        yield c


async def test_authed_client_can_list_agents(authed_client: MurmurClient) -> None:
    agents = await authed_client.list_agents()
    assert "echo" in agents


async def test_no_token_client_gets_401(no_token_client: MurmurClient) -> None:
    with pytest.raises(MurmurError):
        await no_token_client.list_agents()


async def test_wrong_token_client_gets_401(wrong_token_client: MurmurClient) -> None:
    with pytest.raises(MurmurError):
        await wrong_token_client.list_agents()


async def test_health_endpoint_works_unauthed(no_token_client: MurmurClient) -> None:
    # /health is exempt from the auth guard — orchestrator probes don't
    # need credentials.
    assert await no_token_client.health() == {"status": "ok"}
