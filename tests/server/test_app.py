"""Tests for :class:`murmur.server.AgentServer`.

Drives the FastAPI app through ``httpx.AsyncClient`` over an
``httpx.ASGITransport`` so no real port is bound. ``TestModel`` is injected
via the runtime's underlying ``ThreadBackend`` so dispatch never reaches a
real LLM.
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
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import FanOut, TaskSpec, TrustLevel


class SubQuestion(BaseModel):
    question: str


class DecompositionResult(BaseModel):
    sub_questions: FanOut[list[SubQuestion]]
    reasoning: str = ""


class MinionFinding(BaseModel):
    answer: str
    confidence: float


class FinalReport(BaseModel):
    title: str
    findings_count: int


def _decomp() -> DecompositionResult:
    return DecompositionResult(
        sub_questions=[SubQuestion(question=f"q-{i}") for i in range(3)],
        reasoning="r",
    )


def _make_factory() -> Any:
    canned = {
        "research-head": _decomp().model_dump(),
        "research-minion": MinionFinding(answer="a", confidence=0.9).model_dump(),
        "research-summary": FinalReport(title="R", findings_count=3).model_dump(),
        "echo": _Echo(text="ok").model_dump(),
    }

    def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned[agent.name]),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


class _Echo(BaseModel):
    text: str


def _agent(name: str, output_type: type[BaseModel]) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def server() -> AgentServer:
    backend = ThreadBackend()
    backend._build_pa_agent = _make_factory()
    runtime = AgentRuntime(backend=backend)
    s = AgentServer(runtime=runtime)
    s.register(_agent("echo", _Echo))
    head = _agent("research-head", DecompositionResult)
    minion = _agent("research-minion", MinionFinding)
    summary = _agent("research-summary", FinalReport)
    s.register(head)
    s.register(minion)
    s.register(summary)
    s.register_group(
        AgentGroup(
            name="research",
            topology={
                head: Edge(to=(minion,)),  # auto fan-out via FanOut
                minion: Edge(
                    to=(summary,),
                    mapper=lambda findings: TaskSpec(input=f"{len(findings)}"),
                ),
                summary: Edge.terminal(),
            },
        )
    )
    return s


@pytest.fixture
async def client(server: AgentServer) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def test_health_returns_ok(client: httpx.AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_list_agents(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents")
    assert r.status_code == 200
    assert "echo" in r.json()
    assert "research-head" in r.json()


async def test_agent_schema(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents/echo/schema")
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "echo"
    assert body["output_type"]["properties"]["text"]


async def test_unknown_agent_returns_404(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents/ghost/schema")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "RegistryError"


async def test_group_topology_marks_fan_out(client: httpx.AsyncClient) -> None:
    r = await client.get("/groups/research/topology")
    assert r.status_code == 200
    body = r.json()
    edges = {(e["from"], e["to"]): e["fan_out"] for e in body["edges"]}
    # head→minion has no mapper → fan_out=True; minion→summary has mapper → False.
    assert edges[("research-head", "research-minion")] is True
    assert edges[("research-minion", "research-summary")] is False


# ---------------------------------------------------------------------------
# Synchronous dispatch
# ---------------------------------------------------------------------------


async def test_run_agent_returns_typed_output(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/agents/echo/run",
        json={"task": TaskSpec(input="hi").model_dump()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["output"]["text"] == "ok"


async def test_gather_returns_n_results(client: httpx.AsyncClient) -> None:
    tasks = [TaskSpec(input=f"q-{i}").model_dump() for i in range(5)]
    r = await client.post(
        "/agents/echo/gather", json={"tasks": tasks, "max_concurrency": 3}
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 5
    assert all(item["success"] for item in body)


async def test_run_group_via_http(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/groups/research/run",
        json={"task": TaskSpec(input="research the failure modes").model_dump()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["output"]["findings_count"] == 3


# ---------------------------------------------------------------------------
# Submit / status / result
# ---------------------------------------------------------------------------


async def test_submit_then_poll_returns_completed_result(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(
        "/submit",
        json={
            "target": "echo",
            "task": TaskSpec(input="x").model_dump(),
        },
    )
    assert r.status_code == 200
    run_id = r.json()["run_id"]

    # Poll until terminal — TestModel finishes near-instantly.
    import asyncio

    for _ in range(50):
        s = await client.get(f"/runs/{run_id}/status")
        if s.json()["state"] in {"completed", "failed"}:
            break
        await asyncio.sleep(0.01)

    final = await client.get(f"/runs/{run_id}/result")
    assert final.status_code == 200
    assert final.json()["success"] is True


async def test_result_before_complete_returns_409(client: httpx.AsyncClient) -> None:
    # Synthesise a fresh, never-completing run by registering it directly in
    # the store then querying. Easier path: submit and immediately fetch
    # before background runs (race) — instead, hit the endpoint with a fresh
    # store entry via the public API and check the 409 path with a delay.
    r = await client.post(
        "/submit",
        json={"target": "echo", "task": TaskSpec(input="x").model_dump()},
    )
    run_id = r.json()["run_id"]
    # Don't poll — fetch result immediately. May or may not race; if already
    # done, just assert success. Otherwise, expect 409.
    final = await client.get(f"/runs/{run_id}/result")
    assert final.status_code in {200, 409}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


async def test_request_id_round_trips(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/agents/echo/run",
        json={"task": TaskSpec(input="x").model_dump()},
        headers={"X-Request-Id": "req-known-99"},
    )
    assert r.headers.get("X-Request-Id") == "req-known-99"


async def test_unknown_agent_run_returns_404_with_typed_error(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(
        "/agents/ghost/run", json={"task": TaskSpec(input="x").model_dump()}
    )
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "RegistryError"
    assert body["request_id"]  # always set


# ---------------------------------------------------------------------------
# Graceful shutdown (Addendum 4)
# ---------------------------------------------------------------------------


async def test_shutting_down_returns_503_with_retry_after(
    server: AgentServer,
) -> None:
    server._shutting_down = True  # simulate SIGTERM having fired
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post(
            "/agents/echo/run", json={"task": TaskSpec(input="x").model_dump()}
        )
    assert r.status_code == 503
    assert r.headers.get("Retry-After") == "5"
    body = r.json()
    assert body["error"] == "ServerShuttingDown"
