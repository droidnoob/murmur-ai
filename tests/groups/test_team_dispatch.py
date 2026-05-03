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


# ---------------------------------------------------------------------------
# Backend-coupling regression: the per-run delegate tool must land on
# whichever ``ToolRegistry`` the dispatch path actually consults.
# ---------------------------------------------------------------------------


async def test_team_dispatch_uses_backend_tool_registry_for_injection() -> None:
    """A user-injected ``AsyncBackend`` carries its own tool registry that
    is *not* shared with ``runtime.tool_registry``. ``run_team`` must
    register the per-run delegate tool on the registry the backend
    actually uses at dispatch (the backend's own) — registering on
    ``runtime.tool_registry`` would silently leave the tool invisible.
    """
    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t-injected",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )
    backend = AsyncBackend()
    # Sanity: the backend's registry is a distinct object from any
    # default the runtime would build.
    assert backend.tool_registry is not None
    runtime = AgentRuntime(backend=backend)
    # Confirm divergence — runtime's view is separate from backend's.
    assert runtime.tool_registry is not backend.tool_registry

    captured_tool_names: list[str] = []
    real_register = backend.tool_registry.register

    def capturing_register(name: str, func: Any) -> None:
        captured_tool_names.append(name)
        real_register(name, func)

    backend.tool_registry.register = capturing_register  # ty: ignore[invalid-assignment]
    try:
        # The dispatch will probably fail (no API key, no model) —
        # we only care about *where* the per-run delegate tool got
        # registered. ``contextlib.suppress`` keeps the test focused
        # on the registration side-effect.
        import contextlib

        with contextlib.suppress(Exception):
            await runtime.run_group(team, TaskSpec(input="..."))
    finally:
        await runtime.shutdown()

    # Exactly one delegate tool got registered on the backend's
    # registry — registering on ``runtime.tool_registry`` would leave
    # ``captured_tool_names`` empty.
    matching = [
        n for n in captured_tool_names if n.startswith("_team_t-injected_delegate_")
    ]
    assert len(matching) == 1


async def test_team_dispatch_rejects_jobbackend_with_clear_error() -> None:
    """Distributed team dispatch through ``JobBackend`` is not yet
    supported — the modified coordinator + per-run delegate tool are
    publisher-side constructs that don't survive the broker hop. Surface
    the limitation cleanly rather than letting the worker silently
    dispatch the un-modified coordinator (which has no ``delegate``
    tool) and produce confusing failures.
    """
    from murmur.backends._inmemory_broker import InMemoryBroker

    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t-broker",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )
    runtime = AgentRuntime(broker_instance=InMemoryBroker())
    try:
        with pytest.raises(NotImplementedError, match="JobBackend"):
            await runtime.run_group(team, TaskSpec(input="..."))
    finally:
        await runtime.shutdown()


# ---------------------------------------------------------------------------
# Registry/executor identity guard
# ---------------------------------------------------------------------------


async def test_async_backend_rejects_registry_executor_split() -> None:
    """If a caller passes both ``tool_registry`` and ``tool_executor`` and
    the executor's registry isn't the same object, ``AsyncBackend``
    rejects at construction. Otherwise team registrations would land
    on one view and execution would miss them.
    """
    from murmur.tools.executor import ToolExecutor
    from murmur.tools.registry import ToolRegistry

    reg_a = ToolRegistry()
    reg_b = ToolRegistry()
    exec_with_b = ToolExecutor(reg_b)

    with pytest.raises(ValueError, match="share the same registry"):
        AsyncBackend(tool_registry=reg_a, tool_executor=exec_with_b)


async def test_agent_runtime_rejects_registry_executor_split() -> None:
    """Same identity rule on ``AgentRuntime`` — registrations on
    ``runtime.tool_registry`` must be visible to ``runtime`` 's
    executor at execution time.
    """
    from murmur.tools.executor import ToolExecutor
    from murmur.tools.registry import ToolRegistry

    reg_a = ToolRegistry()
    reg_b = ToolRegistry()
    exec_with_b = ToolExecutor(reg_b)

    with pytest.raises(ValueError, match="share the same registry"):
        AgentRuntime(tool_registry=reg_a, tool_executor=exec_with_b)


async def test_async_backend_derives_registry_from_executor() -> None:
    """When only ``tool_executor`` is passed, the backend's
    ``tool_registry`` is the executor's registry — single source of
    truth, no divergence path.
    """
    from murmur.tools.executor import ToolExecutor
    from murmur.tools.registry import ToolRegistry

    reg = ToolRegistry()
    executor = ToolExecutor(reg)
    backend = AsyncBackend(tool_executor=executor)
    assert backend.tool_registry is reg


async def test_run_group_rejects_non_group_non_team_input() -> None:
    """``run_group`` accepts only ``AgentGroup | AgentTeam``. Anything
    else surfaces a clear ``TypeError`` rather than crashing on a
    missing attribute deep inside the runner.
    """
    runtime = AgentRuntime()
    try:
        with pytest.raises(TypeError, match="AgentGroup or AgentTeam"):
            await runtime.run_group("not a group", TaskSpec(input="..."))  # ty: ignore[invalid-argument-type]
    finally:
        await runtime.shutdown()


async def test_agent_team_delegates_is_immutable_after_construction() -> None:
    """``AgentTeam.delegates`` is wrapped in ``MappingProxyType`` at
    construction so callers can't mutate the mapping post-hoc and
    bypass the validators.
    """
    coordinator = _agent("triage")
    billing = _agent("billing-agent", input_type=_BillingInput)
    team = AgentTeam(
        name="t-frozen",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=_Resolution,
    )
    with pytest.raises(TypeError, match="does not support item assignment"):
        team.delegates["new"] = billing  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError, match="does not support item deletion"):
        del team.delegates["billing"]  # ty: ignore[not-subscriptable]
