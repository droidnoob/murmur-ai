"""End-to-end: ``MurmurClient`` against an in-process ``AgentServer``.

httpx's ``ASGITransport`` lets the client talk to the FastAPI app without
binding a real port, so the test exercises the full HTTP loop:
serialisation, status mapping, request_id propagation, error round-trip.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import RegistryError
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import TaskSpec, TrustLevel
from murmur_client.client import MurmurClient, Run


class _Echo(BaseModel):
    text: str


@pytest.fixture
def server() -> AgentServer:
    backend = ThreadBackend()

    def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Echo(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend._build_pa_agent = build  # ty: ignore[invalid-assignment]  # test seam
    runtime = AgentRuntime(backend=backend)
    s = AgentServer(runtime=runtime)
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


@pytest.fixture
async def client(server: AgentServer) -> AsyncIterator[MurmurClient]:
    transport = httpx.ASGITransport(app=server.app)
    async with MurmurClient("http://test", transport=transport) as c:
        yield c


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def test_health(client: MurmurClient) -> None:
    assert await client.health() == {"status": "ok"}


async def test_list_agents_includes_registered(client: MurmurClient) -> None:
    agents = await client.list_agents()
    assert "echo" in agents


async def test_get_agent_schema(client: MurmurClient) -> None:
    schema = await client.get_agent_schema("echo")
    assert schema["name"] == "echo"
    assert schema["output_type"]["properties"]["text"]


async def test_unknown_agent_raises_typed_RegistryError(
    client: MurmurClient,
) -> None:
    with pytest.raises(RegistryError):
        await client.get_agent_schema("ghost")


# ---------------------------------------------------------------------------
# Sync dispatch
# ---------------------------------------------------------------------------


async def test_run_returns_agent_result(client: MurmurClient) -> None:
    result = await client.run("echo", TaskSpec(input="hi"))
    assert result.is_ok()
    assert result.output is not None
    # client-side output is a dict-bearing untyped model
    assert result.output.model_dump() == {"text": "ok"}


async def test_gather_returns_n_results(client: MurmurClient) -> None:
    tasks = [TaskSpec(input=f"q-{i}") for i in range(4)]
    results = await client.gather("echo", tasks)
    assert len(results) == 4
    assert all(r.is_ok() for r in results)


# ---------------------------------------------------------------------------
# Submit / status / result
# ---------------------------------------------------------------------------


async def test_submit_run_handle_round_trip(client: MurmurClient) -> None:
    run: Run = await client.submit("echo", TaskSpec(input="x"))
    assert run.target == "echo"

    import asyncio

    for _ in range(50):
        status = await run.status()
        if status.state.value in {"completed", "failed"}:
            break
        await asyncio.sleep(0.01)

    result = await run.result()
    assert result.is_ok()
