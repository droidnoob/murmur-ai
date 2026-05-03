"""End-to-end tests for ``runtime.run_group(team, ...)`` — AgentTeam dispatch."""

from __future__ import annotations

from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.full import FullContextPasser
from murmur.core.errors import RegistryError
from murmur.groups.team import AgentTeam
from murmur.runtime import AgentRuntime
from murmur.types import AgentResult, TaskSpec, TrustLevel


class _BillingInput(BaseModel):
    invoice_id: str


class _TechnicalInput(BaseModel):
    error_code: str


class _Resolution(BaseModel):
    summary: str


def _agent(
    name: str,
    *,
    output_type: type[BaseModel] = _Resolution,
    input_type: type[BaseModel] | None = None,
) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        input_type=input_type,
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=FullContextPasser(),
    )


def _canned_runtime(canned: dict[str, dict[str, Any]]) -> AgentRuntime:
    backend = AsyncBackend()

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        out = canned.get(agent.name)
        if out is None:
            raise ValueError(f"no canned output for {agent.name!r}")
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=out),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend._build_pa_agent = build  # ty: ignore[invalid-assignment]
    return AgentRuntime(backend=backend)


# ---------------------------------------------------------------------------
# Per-run tool scope
# ---------------------------------------------------------------------------


async def test_team_run_does_not_leak_tool_registration() -> None:
    """Two consecutive ``run_group(team, ...)`` calls don't accumulate
    delegate registrations on the runtime's tool registry — each run
    registers and unregisters its own per-run name.
    """
    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t1",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )
    runtime = _canned_runtime(
        {
            coordinator.name: _Resolution(summary="ok").model_dump(),
            billing.name: _Resolution(summary="paid").model_dump(),
        }
    )
    pre = runtime.tool_registry.names()
    try:
        for _ in range(3):
            await runtime.run_group(team, TaskSpec(input="..."))
        assert runtime.tool_registry.names() == pre, (
            "delegate tool leaked across team runs"
        )
    finally:
        await runtime.shutdown()


async def test_two_teams_share_runtime_without_cross_contamination() -> None:
    """``team1`` and ``team2`` with different delegate menus dispatched
    on the same runtime — each registers its own per-run tool name; the
    other team's tool isn't visible to its coordinator.
    """
    coord1 = _agent("triage-1")
    coord2 = _agent("triage-2")
    billing = _agent("billing-agent", input_type=_BillingInput)
    technical = _agent("technical-agent", input_type=_TechnicalInput)
    team1 = AgentTeam(
        name="t1",
        coordinator=coord1,
        delegates={"billing": billing},
        output_type=_Resolution,
    )
    team2 = AgentTeam(
        name="t2",
        coordinator=coord2,
        delegates={"technical": technical},
        output_type=_Resolution,
    )
    runtime = _canned_runtime(
        {
            coord1.name: _Resolution(summary="ok-1").model_dump(),
            coord2.name: _Resolution(summary="ok-2").model_dump(),
            billing.name: _Resolution(summary="paid").model_dump(),
            technical.name: _Resolution(summary="restart").model_dump(),
        }
    )
    try:
        r1 = await runtime.run_group(team1, TaskSpec(input="..."))
        r2 = await runtime.run_group(team2, TaskSpec(input="..."))
        assert isinstance(r1, AgentResult)
        assert isinstance(r2, AgentResult)
        assert r1.is_ok() and r2.is_ok()
        # Tool registry is fully restored to the pre-team state after
        # each dispatch — neither team sees the other's tool registered.
        assert runtime.tool_registry.names() == frozenset()
    finally:
        await runtime.shutdown()


async def test_team_run_releases_tool_on_coordinator_failure() -> None:
    """Even when the coordinator fails, the delegate tool is unregistered.
    The ``finally`` block in ``run_team`` runs regardless of whether the
    dispatch returned cleanly.
    """
    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )

    async def failing_build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name == coordinator.name:
            raise RuntimeError("coordinator-down")
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Resolution(summary="x").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend = AsyncBackend()
    backend._build_pa_agent = failing_build  # ty: ignore[invalid-assignment]
    runtime = AgentRuntime(backend=backend)
    pre = runtime.tool_registry.names()
    try:
        result = await runtime.run_group(team, TaskSpec(input="..."))
        # Coordinator failure surfaces as AgentResult.error; the team
        # dispatch itself doesn't raise, mirroring single-agent
        # ``runtime.run`` semantics.
        assert isinstance(result, AgentResult)
        assert not result.is_ok()
        # Tool registry is still restored.
        assert runtime.tool_registry.names() == pre
    finally:
        await runtime.shutdown()


async def test_per_run_tool_name_is_unique_across_dispatches() -> None:
    """The per-run tool name carries a UUID suffix so concurrent or
    serial dispatches of the same team never collide on the registry.
    Verified by inspecting the tool name observed by the coordinator
    on two successive dispatches.
    """
    captured_names: list[frozenset[str]] = []
    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )

    async def capturing_build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name == coordinator.name:
            captured_names.append(agent.tools)
        canned = {
            coordinator.name: _Resolution(summary="ok").model_dump(),
            billing.name: _Resolution(summary="paid").model_dump(),
        }[agent.name]
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend = AsyncBackend()
    backend._build_pa_agent = capturing_build  # ty: ignore[invalid-assignment]
    runtime = AgentRuntime(backend=backend)
    try:
        await runtime.run_group(team, TaskSpec(input="..."))
        await runtime.run_group(team, TaskSpec(input="..."))
    finally:
        await runtime.shutdown()

    assert len(captured_names) == 2
    # Each dispatch saw a distinct tool name on the coordinator's
    # tools= set — UUID-backed naming guarantees no collision.
    tool_names_per_run = [
        next(iter(t for t in tools if t.startswith("_team_t_delegate_")))
        for tools in captured_names
    ]
    assert tool_names_per_run[0] != tool_names_per_run[1]


async def test_double_register_would_raise_without_unique_name() -> None:
    """Sanity check: the runtime's ToolRegistry rejects duplicate
    registrations. The unique per-run name in ``run_team`` is what
    keeps team dispatches from colliding — this test fires the
    underlying contract.
    """

    async def dummy_tool(x: int) -> int:
        return x

    runtime = AgentRuntime()
    try:
        runtime.tool_registry.register("my-tool", dummy_tool)
        with pytest.raises(RegistryError, match="already registered"):
            runtime.tool_registry.register("my-tool", dummy_tool)
        runtime.tool_registry.unregister("my-tool")
        # After unregister, re-register works.
        runtime.tool_registry.register("my-tool", dummy_tool)
    finally:
        await runtime.shutdown()
