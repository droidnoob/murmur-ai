"""Unit tests for ``murmur.groups.team`` — AgentTeam spec + delegate tool."""

from __future__ import annotations

import inspect
from typing import Any, get_args, get_origin

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser
from murmur.core.errors import SpecValidationError, ToolExecutionError
from murmur.groups.team import AgentTeam, _make_delegate_tool
from murmur.runtime import AgentRuntime
from murmur.types import AgentContext, TrustLevel

# ---------------------------------------------------------------------------
# Domain types — three delegate input shapes + one shared output
# ---------------------------------------------------------------------------


class BillingInput(BaseModel):
    invoice_id: str


class TechnicalInput(BaseModel):
    error_code: str


class EscalationInput(BaseModel):
    severity: str


class Resolution(BaseModel):
    summary: str


def _agent(
    name: str,
    *,
    output_type: type[BaseModel] = Resolution,
    input_type: type[BaseModel] | None = None,
    context_passer: Any = None,
) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        input_type=input_type,
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=context_passer or FullContextPasser(),
    )


@pytest.fixture
def coordinator() -> Agent:
    return _agent("triage")


@pytest.fixture
def billing() -> Agent:
    return _agent("billing-agent", input_type=BillingInput)


@pytest.fixture
def technical() -> Agent:
    return _agent("technical-agent", input_type=TechnicalInput)


@pytest.fixture
def escalation() -> Agent:
    return _agent("escalation-agent", input_type=EscalationInput)


# ---------------------------------------------------------------------------
# Spec validators
# ---------------------------------------------------------------------------


def test_agent_team_well_formed_constructs(
    coordinator: Agent,
    billing: Agent,
    technical: Agent,
    escalation: Agent,
) -> None:
    team = AgentTeam(
        name="customer-support",
        coordinator=coordinator,
        delegates={
            "billing": billing,
            "technical": technical,
            "escalation": escalation,
        },
        output_type=Resolution,
    )
    assert team.name == "customer-support"
    assert team.max_rounds == 10
    assert team.retain_delegate_history is True


def test_agent_team_empty_delegates_raises(coordinator: Agent) -> None:
    with pytest.raises(SpecValidationError, match="no delegates"):
        AgentTeam(
            name="empty",
            coordinator=coordinator,
            delegates={},
            output_type=Resolution,
        )


def test_agent_team_coordinator_as_delegate_raises(
    coordinator: Agent, billing: Agent
) -> None:
    with pytest.raises(SpecValidationError, match="cannot also be a delegate"):
        AgentTeam(
            name="self-delegate",
            coordinator=coordinator,
            delegates={"billing": billing, "self": coordinator},
            output_type=Resolution,
        )


def test_agent_team_delegate_missing_input_type_raises(
    coordinator: Agent, billing: Agent
) -> None:
    untyped = _agent("untyped", input_type=None)
    with pytest.raises(SpecValidationError, match="must declare Agent.input_type"):
        AgentTeam(
            name="no-input-type",
            coordinator=coordinator,
            delegates={"billing": billing, "untyped": untyped},
            output_type=Resolution,
        )


def test_agent_team_duplicate_input_type_raises(
    coordinator: Agent, billing: Agent
) -> None:
    other_billing = _agent("billing-2", input_type=BillingInput)
    with pytest.raises(SpecValidationError, match="ambiguous routing"):
        AgentTeam(
            name="dup-input-type",
            coordinator=coordinator,
            delegates={"a": billing, "b": other_billing},
            output_type=Resolution,
        )


# ---------------------------------------------------------------------------
# Tool factory — generated signature
# ---------------------------------------------------------------------------


def test_delegate_tool_signature_carries_literal_targets(
    billing: Agent, technical: Agent, escalation: Agent
) -> None:
    """The auto-generated ``delegate`` callable's ``target`` parameter
    is a ``Literal`` over the delegate keys, and ``input`` is a ``Union``
    over every delegate's ``input_type``. PydanticAI's tool-schema
    introspection consumes those annotations as a closed enum + typed
    payload.
    """
    runtime = AgentRuntime()
    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing, "technical": technical, "escalation": escalation},
            retain_history=True,
        )
        sig = inspect.signature(tool)
        target_ann = sig.parameters["target"].annotation
        input_ann = sig.parameters["input"].annotation

        assert get_origin(target_ann) is not None  # Literal has an origin
        assert set(get_args(target_ann)) == {"billing", "technical", "escalation"}

        # input_union covers every declared input_type.
        input_members = set(get_args(input_ann))
        assert input_members == {BillingInput, TechnicalInput, EscalationInput}
    finally:
        # Runtime has no external deps in this test — but call shutdown
        # explicitly for hygiene parity with the dispatch tests below.
        import asyncio

        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0)) if False else None


def test_delegate_tool_factory_rejects_empty_delegates() -> None:
    runtime = AgentRuntime()
    with pytest.raises(SpecValidationError, match="at least one delegate"):
        _make_delegate_tool(runtime, {}, retain_history=True)


# ---------------------------------------------------------------------------
# Tool dispatch — per-delegate routing + history retention
# ---------------------------------------------------------------------------


def _canned_runtime(canned: dict[str, dict[str, Any]]) -> AgentRuntime:
    """Build a runtime whose AsyncBackend.build factory returns canned outputs.

    Mirrors the test pattern used in ``tests/groups/test_runner.py`` —
    each agent gets a ``TestModel`` with pre-set ``custom_output_args``
    so the tool dispatch round-trips deterministically.
    """
    from murmur.backends.async_backend import AsyncBackend

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


async def test_delegate_dispatch_routes_to_named_target(
    billing: Agent, technical: Agent
) -> None:
    """``delegate("billing", BillingInput(...))`` runs the billing agent."""
    runtime = _canned_runtime(
        {
            billing.name: Resolution(summary="paid").model_dump(),
            technical.name: Resolution(summary="restarted").model_dump(),
        }
    )
    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing, "technical": technical},
            retain_history=True,
        )
        result = await tool(target="billing", input=BillingInput(invoice_id="INV-1"))
        assert result == {"summary": "paid"}

        result2 = await tool(target="technical", input=TechnicalInput(error_code="E42"))
        assert result2 == {"summary": "restarted"}
    finally:
        await runtime.shutdown()


async def test_delegate_history_retained_across_calls_default(
    billing: Agent, technical: Agent
) -> None:
    """Two consecutive ``delegate("billing", ...)`` calls — the second
    sees the first's input + output in ``AgentContext.messages``.
    """
    captured: list[AgentContext] = []
    runtime = _canned_runtime(
        {
            billing.name: Resolution(summary="ok").model_dump(),
            technical.name: Resolution(summary="ok").model_dump(),
        }
    )
    real_spawn = runtime._backend.spawn

    async def capturing_spawn(agent: Agent, task: Any, context: AgentContext):
        captured.append(context)
        return await real_spawn(agent, task, context)

    runtime._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing, "technical": technical},
            retain_history=True,
        )
        await tool(target="billing", input=BillingInput(invoice_id="INV-1"))
        await tool(target="billing", input=BillingInput(invoice_id="INV-2"))
    finally:
        await runtime.shutdown()

    # First call: empty history.
    assert captured[0].messages == ()
    # Second call: previous user input + assistant output threaded in.
    assert len(captured[1].messages) == 2
    assert captured[1].messages[0]["role"] == "user"
    assert "INV-1" in captured[1].messages[0]["content"]
    assert captured[1].messages[1]["role"] == "assistant"
    assert "ok" in captured[1].messages[1]["content"]


async def test_delegate_history_isolated_across_delegates(
    billing: Agent, technical: Agent
) -> None:
    """``delegate("billing", X)`` followed by ``delegate("technical", Y)``
    — technical's history is empty (independent of billing's).
    """
    captured: dict[str, list[AgentContext]] = {
        billing.name: [],
        technical.name: [],
    }
    runtime = _canned_runtime(
        {
            billing.name: Resolution(summary="b").model_dump(),
            technical.name: Resolution(summary="t").model_dump(),
        }
    )
    real_spawn = runtime._backend.spawn

    async def capturing_spawn(agent: Agent, task: Any, context: AgentContext):
        captured[agent.name].append(context)
        return await real_spawn(agent, task, context)

    runtime._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing, "technical": technical},
            retain_history=True,
        )
        await tool(target="billing", input=BillingInput(invoice_id="INV-1"))
        await tool(target="technical", input=TechnicalInput(error_code="E0"))
    finally:
        await runtime.shutdown()

    assert captured[billing.name][0].messages == ()
    # Technical's first call sees no history — billing's exchange does
    # not bleed across delegates.
    assert captured[technical.name][0].messages == ()


async def test_delegate_history_resets_between_factory_calls(
    billing: Agent,
) -> None:
    """A fresh ``_make_delegate_tool`` call yields a fresh history dict.
    Distinct ``run_group(team, ...)`` invocations must NOT share state.
    """
    captured: list[AgentContext] = []
    runtime = _canned_runtime({billing.name: Resolution(summary="ok").model_dump()})
    real_spawn = runtime._backend.spawn

    async def capturing_spawn(agent: Agent, task: Any, context: AgentContext):
        captured.append(context)
        return await real_spawn(agent, task, context)

    runtime._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    try:
        # First "team run".
        tool1 = _make_delegate_tool(runtime, {"billing": billing}, retain_history=True)
        await tool1(target="billing", input=BillingInput(invoice_id="INV-A"))

        # Second "team run" — fresh factory call.
        tool2 = _make_delegate_tool(runtime, {"billing": billing}, retain_history=True)
        await tool2(target="billing", input=BillingInput(invoice_id="INV-B"))
    finally:
        await runtime.shutdown()

    # Both first calls of their respective runs see empty history.
    assert captured[0].messages == ()
    assert captured[1].messages == ()


async def test_delegate_history_disabled_dispatches_with_empty_messages(
    billing: Agent,
) -> None:
    """``retain_history=False`` — every dispatch sees empty
    ``AgentContext.messages`` regardless of prior calls.
    """
    captured: list[AgentContext] = []
    runtime = _canned_runtime({billing.name: Resolution(summary="ok").model_dump()})
    real_spawn = runtime._backend.spawn

    async def capturing_spawn(agent: Agent, task: Any, context: AgentContext):
        captured.append(context)
        return await real_spawn(agent, task, context)

    runtime._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    try:
        tool = _make_delegate_tool(runtime, {"billing": billing}, retain_history=False)
        await tool(target="billing", input=BillingInput(invoice_id="INV-1"))
        await tool(target="billing", input=BillingInput(invoice_id="INV-2"))
    finally:
        await runtime.shutdown()

    assert captured[0].messages == ()
    assert captured[1].messages == ()  # No retention — second call still empty.


async def test_delegate_tool_propagates_dispatch_failure_as_tool_error() -> None:
    """A failed delegate dispatch surfaces as ``ToolExecutionError`` so
    the coordinator's tool loop sees it inline.
    """

    class _FailingBilling(BaseModel):
        invoice_id: str

    failing = _agent("fail", input_type=_FailingBilling)

    async def failing_build(agent: Agent, _allowed: frozenset[str], _task_id: str):
        raise RuntimeError("backend down")

    from murmur.backends.async_backend import AsyncBackend

    backend = AsyncBackend()
    backend._build_pa_agent = failing_build  # ty: ignore[invalid-assignment]
    runtime = AgentRuntime(backend=backend)
    try:
        tool = _make_delegate_tool(runtime, {"fail": failing}, retain_history=False)
        with pytest.raises(ToolExecutionError, match="delegate 'fail' failed"):
            await tool(target="fail", input=_FailingBilling(invoice_id="X"))
    finally:
        await runtime.shutdown()


async def test_delegate_tool_rejects_mismatched_input_type(
    billing: Agent, technical: Agent
) -> None:
    """``delegate("billing", TechnicalInput(...))`` is schema-valid under the
    raw ``Literal+Union`` advertisement (PydanticAI sees an enum target
    and a union input independently). The runtime guard catches the
    mismatch before dispatch and surfaces it as ``ToolExecutionError``
    so the coordinator's tool loop sees the failure inline.
    """
    runtime = _canned_runtime({billing.name: Resolution(summary="b").model_dump()})
    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing, "technical": technical},
            retain_history=False,
        )
        with pytest.raises(ToolExecutionError, match="expects input of type"):
            await tool(target="billing", input=TechnicalInput(error_code="E0"))
    finally:
        await runtime.shutdown()


async def test_delegate_tool_enforces_max_rounds(billing: Agent) -> None:
    """``max_rounds`` caps total dispatches per factory call. Independent
    of ``RuntimeOptions.max_spawn_depth`` (which still bounds total
    cascade depth across the whole runtime) — this knob fires
    team-locally so a runaway coordinator can't burn through delegates
    in a tight loop.
    """
    runtime = _canned_runtime({billing.name: Resolution(summary="ok").model_dump()})
    try:
        tool = _make_delegate_tool(
            runtime,
            {"billing": billing},
            retain_history=False,
            max_rounds=2,
        )
        await tool(target="billing", input=BillingInput(invoice_id="A"))
        await tool(target="billing", input=BillingInput(invoice_id="B"))
        with pytest.raises(ToolExecutionError, match="max_rounds=2 reached"):
            await tool(target="billing", input=BillingInput(invoice_id="C"))
    finally:
        await runtime.shutdown()


async def test_delegate_tool_normalises_runtime_errors_to_tool_execution_error(
    coordinator: Agent, billing: Agent
) -> None:
    """``runtime.run`` rejections that fire before the backend sees the
    dispatch (``SpawnCycleError`` here) escape as ``MurmurError``
    subclasses. The factory wraps them in ``ToolExecutionError`` so the
    coordinator sees a uniform shape regardless of where the failure
    originated.
    """
    from murmur.runtime import _current_spawn, _SpawnFrame

    runtime = _canned_runtime({billing.name: Resolution(summary="x").model_dump()})
    # Pre-set the spawn chain so ``billing`` is on it — runtime.run
    # rejects the next dispatch as a SpawnCycleError before any backend
    # call.
    parent_frame = _SpawnFrame(
        agent_name=billing.name,
        agent_context=AgentContext(),
        trace_id="t-pre",
    )
    token = _current_spawn.set(parent_frame)
    try:
        tool = _make_delegate_tool(runtime, {"billing": billing}, retain_history=False)
        with pytest.raises(
            ToolExecutionError, match="delegate 'billing' failed: SpawnCycleError"
        ):
            await tool(target="billing", input=BillingInput(invoice_id="A"))
    finally:
        _current_spawn.reset(token)
        await runtime.shutdown()


async def test_run_group_dispatches_agent_team_and_releases_tool_registration(
    coordinator: Agent, billing: Agent
) -> None:
    """``AgentRuntime.run_group(team, ...)`` runs the coordinator with
    the delegate tool registered, then unregisters it on exit so the
    runtime's tool registry is clean for subsequent runs.
    """
    from murmur.types import TaskSpec

    team = AgentTeam(
        name="customer-support",
        coordinator=coordinator,
        delegates={"billing": billing},
        output_type=Resolution,
    )
    runtime = _canned_runtime(
        {
            coordinator.name: Resolution(summary="ok").model_dump(),
            billing.name: Resolution(summary="paid").model_dump(),
        }
    )
    from murmur.types import AgentResult

    pre_tools = runtime.tool_registry.names()
    try:
        result = await runtime.run_group(team, TaskSpec(input="..."))
        # Team dispatch returns the coordinator's AgentResult — narrow
        # the union (run_group's return type widens to allow GroupResult
        # for multi-leaf AgentGroup topologies).
        assert isinstance(result, AgentResult)
        assert result.is_ok()
        assert result.agent_name == coordinator.name
        # Per-run tool scope: registry is restored to its pre-run state.
        assert runtime.tool_registry.names() == pre_tools
    finally:
        await runtime.shutdown()


async def test_delegate_tool_overrides_null_context_passer(
    billing: Agent,
) -> None:
    """Even when the delegate is configured with ``NullContextPasser``
    (which would normally wipe ``AgentContext.messages``), the tool's
    history-injecting passer overrides the dispatch's context_passer
    so the prior exchange survives. Resolves design doc §5.1's
    NullContextPasser concern: callers don't need to special-case
    their delegate's context_passer to opt into history retention.
    """
    null_billing = billing.model_copy(update={"context_passer": NullContextPasser()})
    captured: list[AgentContext] = []
    runtime = _canned_runtime(
        {null_billing.name: Resolution(summary="ok").model_dump()}
    )
    real_spawn = runtime._backend.spawn

    async def capturing_spawn(agent: Agent, task: Any, context: AgentContext):
        captured.append(context)
        return await real_spawn(agent, task, context)

    runtime._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    try:
        tool = _make_delegate_tool(
            runtime, {"billing": null_billing}, retain_history=True
        )
        await tool(target="billing", input=BillingInput(invoice_id="A"))
        await tool(target="billing", input=BillingInput(invoice_id="B"))
    finally:
        await runtime.shutdown()

    # Second call sees first's exchange threaded in despite NullContextPasser.
    assert len(captured[1].messages) == 2
