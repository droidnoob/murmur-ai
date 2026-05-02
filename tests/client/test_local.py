"""End-to-end: ``LocalClient`` against an in-process ``AgentServer``.

Mirrors ``test_client.py`` but skips httpx — the client calls the
runtime directly. Same ``Run`` handle works for both clients because
both satisfy the ``_RunBackend`` Protocol.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
import pytest
from murmur_client.client import Run
from murmur_client.local import LocalClient
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import RegistryError
from murmur.runs import RunState
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import TaskSpec, TrustLevel


class _Echo(BaseModel):
    text: str


def _build_factory(canned: dict[str, Any]) -> Any:
    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned[agent.name]),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _runtime_with(canned: dict[str, Any]) -> AgentRuntime:
    backend = ThreadBackend()
    backend._build_pa_agent = _build_factory(canned)  # noqa: SLF001
    return AgentRuntime(backend=backend)


def _agent(name: str = "echo") -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Echo,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def server() -> AgentServer:
    runtime = _runtime_with({"echo": _Echo(text="ok").model_dump()})
    s = AgentServer(runtime=runtime)
    s.register(_agent())
    return s


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def test_health_returns_ok(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        assert await client.health() == {"status": "ok"}


async def test_list_agents_includes_registered(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        agents = await client.list_agents()
        assert "echo" in agents


async def test_get_agent_schema(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        schema = await client.get_agent_schema("echo")
        assert schema["name"] == "echo"
        assert schema["output_type"]["properties"]["text"]


async def test_unknown_agent_raises_registry_error(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        with pytest.raises(RegistryError, match="ghost"):
            await client.get_agent_schema("ghost")


# ---------------------------------------------------------------------------
# Synchronous dispatch — local returns the typed model, no _UntypedOutput
# ---------------------------------------------------------------------------


async def test_run_returns_typed_output(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        result = await client.run("echo", TaskSpec(input="hi"))
        assert result.is_ok()
        assert isinstance(result.output, _Echo)
        assert result.output.text == "ok"


async def test_gather_returns_n_typed_results(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        results = await client.gather(
            "echo",
            tasks=[TaskSpec(input=f"q-{i}") for i in range(4)],
            max_concurrency=2,
        )
        assert len(results) == 4
        assert all(r.is_ok() and isinstance(r.output, _Echo) for r in results)


# ---------------------------------------------------------------------------
# Async dispatch — submit + Run handle round-trip
# ---------------------------------------------------------------------------


async def test_submit_returns_run_handle(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        run = await client.submit("echo", TaskSpec(input="bg"))
        assert isinstance(run, Run)
        assert run.target == "echo"
        # Drain — wait for the background task to settle.
        for _ in range(50):
            status = await run.status()
            if status.state in {RunState.COMPLETED, RunState.FAILED}:
                break
            import asyncio

            await asyncio.sleep(0.02)
        result = await run.result()
        assert result.is_ok()


async def test_run_cancel_on_completed_run_is_noop(server: AgentServer) -> None:
    async with LocalClient(server=server) as client:
        run = await client.submit("echo", TaskSpec(input="bg"))
        # Wait for it to land in a terminal state.
        for _ in range(50):
            status = await run.status()
            if status.state in {RunState.COMPLETED, RunState.FAILED}:
                break
            import asyncio

            await asyncio.sleep(0.02)
        # cancel after completion is a no-op.
        await run.cancel()
        terminal = await run.status()
        assert terminal.state == RunState.COMPLETED


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_kwarg_conflict_runtime_and_server() -> None:
    s = AgentServer()
    with pytest.raises(ValueError, match="server="):
        LocalClient(server=s, runtime=AgentRuntime())


async def test_construct_from_runtime_only() -> None:
    """Without `server=`, LocalClient builds an internal AgentServer."""
    runtime = _runtime_with({"echo": _Echo(text="ok").model_dump()})
    client = LocalClient(runtime=runtime)
    client.server.register(_agent())
    result = await client.run("echo", TaskSpec(input="hi"))
    assert result.is_ok()
    await client.close()


# ---------------------------------------------------------------------------
# Sync entry point
# ---------------------------------------------------------------------------


def test_run_sync_returns_typed_result(server: AgentServer) -> None:
    client = LocalClient(server=server)
    result = client.run_sync("echo", TaskSpec(input="hi"))
    assert result.is_ok()
    assert result.agent_name == "echo"


async def test_run_sync_rejects_nested_call(server: AgentServer) -> None:
    client = LocalClient(server=server)
    with pytest.raises(RuntimeError, match="LocalClient.run_sync"):
        client.run_sync("echo", TaskSpec(input="hi"))
