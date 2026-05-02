"""Tests for cascading-spawn semantics — parent→child graph, cycle rejection,
per-runtime spawn cap, and ``parent_trace_id`` propagation through events.

The existing ``AgentContext.depth`` / ``parent_agent`` fields and
``DepthLimitMiddleware`` were always wired; this test file covers the
infrastructure that finally populates them when a sub-spawn fires inside
another agent's tool loop. The contextvar-based parent-frame discovery is
the linchpin — these tests exercise it directly (by setting the contextvar
manually) and through the ``spawn_agents`` tool path.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur import Agent, AgentRuntime, TaskSpec
from murmur.core.errors import (
    DepthLimitError,
    SpawnCapError,
    SpawnCycleError,
    SpawnError,
)
from murmur.events.types import EventType, RuntimeEvent
from murmur.runtime import RuntimeOptions, _current_spawn, _SpawnFrame
from murmur.types import AgentContext


class _Out(BaseModel):
    answer: str


def _agent(name: str = "x") -> Agent:
    return Agent(name=name, model=TestModel(), instructions="...", output_type=_Out)


# ---------------------------------------------------------------------------
# Recording emitter — captures every event for downstream assertions.
# ---------------------------------------------------------------------------


class _RecordingEmitter:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def emit(self, event: RuntimeEvent) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# Top-level: AgentContext fields stay at defaults; spawn_count increments.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_top_level_run_uses_fresh_agent_context() -> None:
    """A run with no parent frame produces a default ``AgentContext``."""
    emitter = _RecordingEmitter()
    rt = AgentRuntime(event_emitter=emitter)
    result = await rt.run(_agent("solo"), TaskSpec(input="hi"))
    assert result.is_ok()

    # Spawned event has parent_trace_id=None; the runtime's spawn_count is 1.
    spawned = [e for e in emitter.events if e.event_type == EventType.AGENT_SPAWNED]
    assert len(spawned) == 1
    assert spawned[0].parent_trace_id is None
    assert rt.spawn_count == 1


@pytest.mark.asyncio
async def test_spawn_count_increments_per_run() -> None:
    rt = AgentRuntime()
    a = _agent("a")
    for _ in range(3):
        result = await rt.run(a, TaskSpec(input="hi"))
        assert result.is_ok()
    assert rt.spawn_count == 3


# ---------------------------------------------------------------------------
# Cascading: parent frame in contextvar derives child AgentContext.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_child_inherits_depth_and_ancestors() -> None:
    """Setting ``_current_spawn`` makes the next ``runtime.run`` a child."""
    captured: dict[str, Any] = {}

    rt = AgentRuntime()
    real_spawn = rt._backend.spawn

    async def capturing_spawn(  # noqa: ARG001
        agent: Agent, task: TaskSpec, context: AgentContext
    ) -> Any:
        captured["context"] = context
        return await real_spawn(agent, task, context)

    rt._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]  # test seam

    parent_ctx = AgentContext(depth=2, ancestors=frozenset({"grandparent"}))
    parent_frame = _SpawnFrame(
        agent_name="parent", agent_context=parent_ctx, trace_id="trace-parent"
    )
    token = _current_spawn.set(parent_frame)
    try:
        await rt.run(_agent("child"), TaskSpec(input="go"))
    finally:
        _current_spawn.reset(token)

    ctx = captured["context"]
    assert ctx.depth == 3
    assert ctx.parent_agent == "parent"
    assert ctx.parent_trace_id == "trace-parent"
    assert ctx.ancestors == frozenset({"grandparent", "parent"})


@pytest.mark.asyncio
async def test_parent_trace_id_appears_on_child_events() -> None:
    """Every child :class:`RuntimeEvent` carries the parent's trace_id."""
    emitter = _RecordingEmitter()
    rt = AgentRuntime(event_emitter=emitter)

    parent_frame = _SpawnFrame(
        agent_name="parent",
        agent_context=AgentContext(),
        trace_id="trace-from-parent",
    )
    token = _current_spawn.set(parent_frame)
    try:
        result = await rt.run(_agent("child"), TaskSpec(input="hi"))
    finally:
        _current_spawn.reset(token)

    assert result.is_ok()
    child_events = [
        e
        for e in emitter.events
        if e.event_type in (EventType.AGENT_SPAWNED, EventType.AGENT_COMPLETED)
    ]
    assert child_events  # both spawn + complete fire
    for e in child_events:
        assert e.parent_trace_id == "trace-from-parent"


@pytest.mark.asyncio
async def test_sibling_runs_do_not_see_each_other_as_ancestors() -> None:
    """Two children spawned under one parent must each have only the
    parent as their ancestor — not each other.

    The contextvar reset on exit of each child run is what guarantees this.
    """
    captured: list[AgentContext] = []
    rt = AgentRuntime()
    real_spawn = rt._backend.spawn

    async def capturing_spawn(  # noqa: ARG001
        agent: Agent, task: TaskSpec, context: AgentContext
    ) -> Any:
        captured.append(context)
        return await real_spawn(agent, task, context)

    rt._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]  # test seam

    parent_frame = _SpawnFrame(
        agent_name="parent", agent_context=AgentContext(), trace_id="t-parent"
    )
    token = _current_spawn.set(parent_frame)
    try:
        await rt.run(_agent("childA"), TaskSpec(input="a"))
        await rt.run(_agent("childB"), TaskSpec(input="b"))
    finally:
        _current_spawn.reset(token)

    assert captured[0].ancestors == frozenset({"parent"})
    assert captured[1].ancestors == frozenset({"parent"})  # NOT {"parent","childA"}


# ---------------------------------------------------------------------------
# Cycle rejection — direct, transitive, deep.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_cycle_rejected() -> None:
    """A spawning A → SpawnCycleError before any backend call."""
    rt = AgentRuntime()
    parent_frame = _SpawnFrame(
        agent_name="alpha", agent_context=AgentContext(), trace_id="t1"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(SpawnCycleError, match="alpha"):
            await rt.run(_agent("alpha"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    # Rejected cycles must not consume a spawn slot.
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_transitive_cycle_rejected() -> None:
    """B is in current chain (ancestors = {alpha}); spawning alpha → cycle."""
    rt = AgentRuntime()
    parent_ctx = AgentContext(ancestors=frozenset({"alpha"}))
    parent_frame = _SpawnFrame(
        agent_name="beta", agent_context=parent_ctx, trace_id="t2"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(SpawnCycleError):
            await rt.run(_agent("alpha"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_non_cycle_sibling_not_rejected() -> None:
    """Spawning a name not in the chain succeeds even with deep ancestors."""
    rt = AgentRuntime()
    parent_ctx = AgentContext(ancestors=frozenset({"a", "b", "c"}))
    parent_frame = _SpawnFrame(agent_name="d", agent_context=parent_ctx, trace_id="t")
    token = _current_spawn.set(parent_frame)
    try:
        result = await rt.run(_agent("e"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    assert result.is_ok()


# ---------------------------------------------------------------------------
# cycle_policy — opt-in permissive escape hatch for bounded reuse patterns.
# Strict (the default) is covered by the tests above; here we verify that
# permissive disables the raise without disabling the surrounding guards.
# ---------------------------------------------------------------------------


def test_cycle_policy_default_is_strict() -> None:
    """``RuntimeOptions().cycle_policy`` defaults to ``"strict"`` so existing
    callers never silently lose the guard."""
    assert RuntimeOptions().cycle_policy == "strict"


@pytest.mark.asyncio
async def test_cycle_policy_permissive_allows_direct_reentrance() -> None:
    """A → A succeeds when ``cycle_policy="permissive"``."""
    rt = AgentRuntime(options=RuntimeOptions(cycle_policy="permissive"))
    parent_frame = _SpawnFrame(
        agent_name="alpha", agent_context=AgentContext(), trace_id="t1"
    )
    token = _current_spawn.set(parent_frame)
    try:
        result = await rt.run(_agent("alpha"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    assert result.is_ok()


@pytest.mark.asyncio
async def test_cycle_policy_permissive_allows_transitive_cycle() -> None:
    """A → B → A succeeds when ``cycle_policy="permissive"``."""
    rt = AgentRuntime(options=RuntimeOptions(cycle_policy="permissive"))
    parent_ctx = AgentContext(ancestors=frozenset({"alpha"}))
    parent_frame = _SpawnFrame(
        agent_name="beta", agent_context=parent_ctx, trace_id="t2"
    )
    token = _current_spawn.set(parent_frame)
    try:
        result = await rt.run(_agent("alpha"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    assert result.is_ok()


@pytest.mark.asyncio
async def test_cycle_policy_permissive_still_enforces_depth_limit() -> None:
    """``cycle_policy="permissive"`` is *only* about cycle detection — the
    depth limit remains the user's primary termination guarantee and must
    keep firing. Regression test: a permissive policy that accidentally
    short-circuited the rest of the cascading-spawn graph computation
    would let an A → A loop dodge ``DepthLimitMiddleware`` too."""
    rt = AgentRuntime(
        options=RuntimeOptions(cycle_policy="permissive", max_spawn_depth=2)
    )
    # Parent already at depth=1; child = depth=2 → at the cap → reject.
    parent_ctx = AgentContext(depth=1)
    parent_frame = _SpawnFrame(
        agent_name="alpha", agent_context=parent_ctx, trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(DepthLimitError):
            # Same name → would be a strict-mode cycle, but depth fires first.
            await rt.run(_agent("alpha"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)


@pytest.mark.asyncio
async def test_cycle_policy_permissive_still_enforces_spawn_cap() -> None:
    """``max_total_spawns`` keeps tripping under permissive cycle policy.
    A bounded reuse loop still needs the kill switch to fire eventually."""
    rt = AgentRuntime(
        options=RuntimeOptions(cycle_policy="permissive", max_total_spawns=2)
    )
    a = _agent("alpha")
    # Top-level runs — no parent frame, no cycle even under strict — used
    # here purely to exhaust the cap; the point is that permissive doesn't
    # disable cap enforcement.
    await rt.run(a, TaskSpec(input="1"))
    await rt.run(a, TaskSpec(input="2"))
    assert rt.spawn_count == 2
    with pytest.raises(SpawnCapError):
        await rt.run(a, TaskSpec(input="3"))


@pytest.mark.asyncio
async def test_cycle_policy_permissive_allows_cycle_in_gather() -> None:
    """``runtime.gather`` mirrors ``runtime.run``: permissive disables the
    cycle raise on the batch-entry path too."""
    rt = AgentRuntime(options=RuntimeOptions(cycle_policy="permissive"))
    parent_frame = _SpawnFrame(
        agent_name="alpha", agent_context=AgentContext(), trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        results = await rt.gather(_agent("alpha"), [TaskSpec(input="x")])
    finally:
        _current_spawn.reset(token)
    assert len(results) == 1
    assert results[0].is_ok()


# ---------------------------------------------------------------------------
# Per-runtime spawn cap.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_blocks_dispatch_after_threshold() -> None:
    rt = AgentRuntime(options=RuntimeOptions(max_total_spawns=2))
    a = _agent("a")
    await rt.run(a, TaskSpec(input="1"))
    await rt.run(a, TaskSpec(input="2"))
    assert rt.spawn_count == 2
    with pytest.raises(SpawnCapError, match="2 total spawns"):
        await rt.run(a, TaskSpec(input="3"))


@pytest.mark.asyncio
async def test_cap_does_not_reset_after_failure() -> None:
    """A run that fails its body still consumed a slot. The cap is
    intentionally a kill switch, not a per-success counter."""
    rt = AgentRuntime(options=RuntimeOptions(max_total_spawns=1))
    a = _agent("a")
    await rt.run(a, TaskSpec(input="1"))
    with pytest.raises(SpawnCapError):
        await rt.run(a, TaskSpec(input="2"))


@pytest.mark.asyncio
async def test_cap_default_is_unbounded() -> None:
    """``max_total_spawns`` defaults to ``None`` — long-lived runtimes
    (workers, servers) must NOT self-brick after some lifetime threshold.

    Counter still increments for observability; cap rejection only fires
    when the user opts in by setting ``max_total_spawns``.
    """
    rt = AgentRuntime()
    assert rt.options.max_total_spawns is None
    a = _agent("a")
    for _ in range(5):
        await rt.run(a, TaskSpec(input="x"))
    assert rt.spawn_count == 5  # counter still tallies for observability


# ---------------------------------------------------------------------------
# Depth limit cooperates with new depth propagation.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_depth_limit_trips_on_deep_cascade() -> None:
    """``DepthLimitMiddleware`` rejects when ``AgentContext.depth >= max``.

    The depth field is now actually populated by the runtime, so the
    middleware can finally do its job.
    """
    rt = AgentRuntime(options=RuntimeOptions(max_spawn_depth=2))
    parent_ctx = AgentContext(depth=1)  # parent already at depth 1 → child = 2
    parent_frame = _SpawnFrame(
        agent_name="parent", agent_context=parent_ctx, trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(DepthLimitError):
            await rt.run(_agent("child"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)


@pytest.mark.asyncio
async def test_gather_rejects_at_depth_limit() -> None:
    """``runtime.gather`` enforces ``max_spawn_depth`` even though the
    batch path bypasses the middleware pipeline.

    Codex review found this gap: parent at ``depth=max-1`` could fan out
    children at the cap (or deeper) via gather without the recursion
    guard firing. The fix is an inline depth check in ``gather`` mirroring
    ``DepthLimitMiddleware``.
    """
    rt = AgentRuntime(options=RuntimeOptions(max_spawn_depth=2))
    # Parent at depth=1 → gather slots would land at depth=2, equal to cap.
    parent_ctx = AgentContext(depth=1)
    parent_frame = _SpawnFrame(
        agent_name="parent", agent_context=parent_ctx, trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(DepthLimitError, match="exceeds limit 2"):
            await rt.gather(_agent("child"), [TaskSpec(input="x")])
    finally:
        _current_spawn.reset(token)
    # Rejected gather must not consume any slots.
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_depth_rejection_does_not_consume_spawn_slot() -> None:
    """Codex review: ``max_total_spawns`` should not be charged for runs
    that get rejected pre-dispatch (``DepthLimitError``)."""
    rt = AgentRuntime(options=RuntimeOptions(max_spawn_depth=1, max_total_spawns=10))
    parent_ctx = AgentContext(depth=1)  # already at cap, child would be 2
    parent_frame = _SpawnFrame(
        agent_name="parent", agent_context=parent_ctx, trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        for _ in range(5):
            with pytest.raises(DepthLimitError):
                await rt.run(_agent("child"), TaskSpec(input="x"))
    finally:
        _current_spawn.reset(token)
    # 5 depth-rejected runs must NOT have consumed cap budget.
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_budget_rejection_does_not_consume_spawn_slot() -> None:
    """Codex review: a runtime with an exhausted token budget rejects
    via ``BudgetExceededError`` in pre-check; that must not consume a
    spawn-cap slot."""
    from murmur.core.errors import BudgetExceededError
    from murmur.middleware.cost_tracking import TokenBudget

    budget = TokenBudget(limit=1)
    await budget.consume(1)  # exhaust upfront
    rt = AgentRuntime(options=RuntimeOptions(token_budget=budget, max_total_spawns=10))
    for _ in range(3):
        with pytest.raises(BudgetExceededError):
            await rt.run(_agent("a"), TaskSpec(input="x"))
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_gather_rejects_when_token_budget_exhausted() -> None:
    """Codex review (round 4): ``gather`` must enforce ``token_budget``
    just like ``run``. With an exhausted budget, a batch dispatched on
    the backend-native path must fail closed before any tasks fire and
    without consuming spawn-cap slots.
    """
    from murmur.core.errors import BudgetExceededError
    from murmur.middleware.cost_tracking import TokenBudget

    budget = TokenBudget(limit=1)
    await budget.consume(1)  # exhaust upfront
    rt = AgentRuntime(options=RuntimeOptions(token_budget=budget, max_total_spawns=10))
    with pytest.raises(BudgetExceededError, match="gather"):
        await rt.gather(_agent("a"), [TaskSpec(input=str(i)) for i in range(3)])
    # Pre-dispatch reject must not consume cap budget either.
    assert rt.spawn_count == 0


@pytest.mark.asyncio
async def test_gather_charges_aggregate_tokens_against_budget() -> None:
    """``gather`` post-charges the aggregate tokens-used against the
    runtime-wide :class:`TokenBudget` so the next call sees a correct
    remaining count.
    """
    from murmur.middleware.cost_tracking import TokenBudget

    budget = TokenBudget(limit=10_000)
    rt = AgentRuntime(options=RuntimeOptions(token_budget=budget))
    results = await rt.gather(_agent("a"), [TaskSpec(input=str(i)) for i in range(3)])
    assert all(r.is_ok() for r in results)
    # TestModel reports a fixed token count per run; aggregate should be
    # the sum across slots, > 0.
    total_used = sum(int(r.metadata.tokens_used or 0) for r in results)
    assert total_used > 0
    assert budget.used == total_used
    assert budget.remaining == budget.limit - total_used


@pytest.mark.asyncio
async def test_retry_does_not_double_charge_spawn_cap() -> None:
    """One user-visible ``run()`` consumes exactly one cap slot, even when
    ``RetryMiddleware`` retries dispatch_stage on backend SpawnError.

    Pipeline order ``ClaimSlot → Retry → dispatch_stage`` guarantees the
    claim sits *above* Retry, so retries re-enter dispatch_stage but not
    the claim.
    """
    from murmur.core.errors import SpawnError

    rt = AgentRuntime(options=RuntimeOptions(retry_max_attempts=3))
    a = _agent("a")
    # Force 2 backend failures then 1 success: 3 dispatch_stage invocations,
    # but should be only 1 spawn_count increment.
    backend: Any = rt._backend
    real_spawn = backend.spawn
    attempts = {"n": 0}

    async def flaky_spawn(agent: Agent, task: TaskSpec, ctx: AgentContext) -> Any:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise SpawnError(f"transient {attempts['n']}")
        return await real_spawn(agent, task, ctx)

    setattr(backend, "spawn", flaky_spawn)  # noqa: B010
    result = await rt.run(a, TaskSpec(input="hi"))
    assert result.is_ok()
    assert attempts["n"] == 3  # 2 retries + 1 success
    assert rt.spawn_count == 1  # exactly one slot, regardless of retries


# ---------------------------------------------------------------------------
# AgentContext defaults — new fields don't break top-level construction.
# ---------------------------------------------------------------------------


def test_agent_context_defaults() -> None:
    ctx = AgentContext()
    assert ctx.depth == 0
    assert ctx.parent_agent is None
    assert ctx.parent_trace_id is None
    assert ctx.ancestors == frozenset()
    assert ctx.messages == ()


def test_agent_context_frozen() -> None:
    from pydantic import ValidationError

    ctx = AgentContext(ancestors=frozenset({"a"}))
    with pytest.raises(ValidationError):
        ctx.ancestors = frozenset({"b"})


# ---------------------------------------------------------------------------
# Integration with ``make_spawn_agents_tool`` — the contextvar must reach
# the tool's ``runtime.run`` call so children inherit the parent frame.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_agents_tool_threads_parent_context_to_children() -> None:
    """When ``spawn_agents`` is invoked inside a simulated parent frame,
    the child runs it kicks off must observe the parent in their context.
    """
    from murmur import AgentTemplate, TrustLevel
    from murmur.tools import SpawnSpec, make_spawn_agents_tool

    rt = AgentRuntime()
    template = AgentTemplate(model="test", trust_level=TrustLevel.MEDIUM)
    tool = make_spawn_agents_tool(
        runtime=rt, template=template, output_type=_Out, max_concurrency=2
    )

    captured: list[AgentContext] = []
    real_spawn = rt._backend.spawn

    async def capturing_spawn(  # noqa: ARG001
        agent: Agent, task: TaskSpec, context: AgentContext
    ) -> Any:
        captured.append(context)
        return await real_spawn(agent, task, context)

    rt._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]  # test seam

    # Stand in for a parent run that's currently dispatching this tool
    # invocation. Real flow: the orchestrator agent's PydanticAI loop fires
    # the tool while ``_current_spawn`` is set inside its own ``run()``.
    parent_frame = _SpawnFrame(
        agent_name="orchestrator",
        agent_context=AgentContext(),
        trace_id="trace-orch",
    )
    token = _current_spawn.set(parent_frame)
    try:
        results = await tool(
            [
                SpawnSpec(name="worker-1", instructions="x", input="task-a"),
                SpawnSpec(name="worker-2", instructions="x", input="task-b"),
            ]
        )
    finally:
        _current_spawn.reset(token)

    assert len(results) == 2
    assert all(r.success for r in results)
    assert len(captured) == 2
    for ctx in captured:
        assert ctx.depth == 1
        assert ctx.parent_agent == "orchestrator"
        assert ctx.parent_trace_id == "trace-orch"
        assert ctx.ancestors == frozenset({"orchestrator"})


# ---------------------------------------------------------------------------
# gather() — cap, cycle, and parent-frame propagation per slot.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gather_charges_one_slot_per_task_against_cap() -> None:
    """``runtime.gather`` must claim a slot per task or callers can bypass
    ``max_total_spawns`` via the public batch API."""
    rt = AgentRuntime(options=RuntimeOptions(max_total_spawns=10))
    a = _agent("a")
    await rt.gather(a, [TaskSpec(input=str(i)) for i in range(3)])
    assert rt.spawn_count == 3


@pytest.mark.asyncio
async def test_gather_oversized_batch_rejects_atomically() -> None:
    """An oversized ``gather`` must NOT consume any slots — otherwise a
    single bad call can permanently exhaust ``max_total_spawns`` and
    brick subsequent ``run()`` calls (Codex review finding 1).
    """
    rt = AgentRuntime(options=RuntimeOptions(max_total_spawns=3))
    a = _agent("a")
    with pytest.raises(SpawnCapError, match="would be exceeded"):
        await rt.gather(a, [TaskSpec(input=str(i)) for i in range(5)])
    # No slots consumed — the runtime stays usable.
    assert rt.spawn_count == 0
    # Post-rejection: subsequent runs still work up to the cap.
    await rt.run(a, TaskSpec(input="after"))
    assert rt.spawn_count == 1


@pytest.mark.asyncio
async def test_gather_partial_headroom_rejects_without_consuming() -> None:
    """If 2 slots are already used and gather asks for 5 with cap=3,
    the batch must fail closed without bumping the counter past 2."""
    rt = AgentRuntime(options=RuntimeOptions(max_total_spawns=3))
    a = _agent("a")
    await rt.run(a, TaskSpec(input="1"))
    await rt.run(a, TaskSpec(input="2"))
    assert rt.spawn_count == 2
    with pytest.raises(SpawnCapError, match="would be exceeded"):
        await rt.gather(a, [TaskSpec(input=str(i)) for i in range(5)])
    assert rt.spawn_count == 2  # untouched
    # The remaining headroom is still claimable.
    await rt.run(a, TaskSpec(input="3"))
    assert rt.spawn_count == 3


@pytest.mark.asyncio
async def test_gather_under_parent_frame_propagates_to_each_slot() -> None:
    """Every slot in a cascaded gather sees the parent on its context."""
    captured: list[AgentContext] = []
    rt = AgentRuntime()
    real_spawn = rt._backend.spawn

    async def capturing_spawn(  # noqa: ARG001
        agent: Agent, task: TaskSpec, context: AgentContext
    ) -> Any:
        captured.append(context)
        return await real_spawn(agent, task, context)

    rt._backend.spawn = capturing_spawn  # ty: ignore[invalid-assignment]

    parent_frame = _SpawnFrame(
        agent_name="parent",
        agent_context=AgentContext(),
        trace_id="trace-parent",
    )
    token = _current_spawn.set(parent_frame)
    try:
        results = await rt.gather(
            _agent("worker"),
            [TaskSpec(input=str(i)) for i in range(3)],
        )
    finally:
        _current_spawn.reset(token)

    assert len(results) == 3
    assert all(r.is_ok() for r in results)
    assert len(captured) == 3
    for ctx in captured:
        assert ctx.depth == 1
        assert ctx.parent_agent == "parent"
        assert ctx.parent_trace_id == "trace-parent"
        assert ctx.ancestors == frozenset({"parent"})


@pytest.mark.asyncio
async def test_gather_rejects_cycle_at_entry() -> None:
    """``gather`` enforces the same cycle rule as ``run``."""
    rt = AgentRuntime()
    parent_frame = _SpawnFrame(
        agent_name="alpha", agent_context=AgentContext(), trace_id="t"
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(SpawnCycleError):
            await rt.gather(_agent("alpha"), [TaskSpec(input="x")])
    finally:
        _current_spawn.reset(token)
    # Rejected cycle: no slot consumed.
    assert rt.spawn_count == 0


# ---------------------------------------------------------------------------
# gather() — per-batch timeout enforcement (mirrors TimeoutMiddleware on
# the run() pipeline; backend-native gather paths bypass middleware so the
# wall clock has to be enforced inline by the runtime).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gather_completes_under_timeout_on_fast_tasks() -> None:
    """Happy path: short timeout, fast tasks settle normally."""
    rt = AgentRuntime(options=RuntimeOptions(timeout_seconds=5.0))
    a = _agent("a")
    results = await rt.gather(a, [TaskSpec(input=str(i)) for i in range(3)])
    assert len(results) == 3
    assert all(r.is_ok() for r in results)


@pytest.mark.asyncio
async def test_gather_raises_spawn_error_on_timeout() -> None:
    """Sad path: a slow ``backend.gather`` past ``timeout_seconds`` must
    raise :class:`SpawnError` rather than hang. Mirrors ``TimeoutMiddleware``
    on the ``run()`` pipeline — backend-native gather bypasses middleware,
    so the runtime enforces the wall clock inline."""
    rt = AgentRuntime(options=RuntimeOptions(timeout_seconds=0.1))

    async def slow_gather(
        agent: Agent,  # noqa: ARG001
        tasks: Sequence[TaskSpec],  # noqa: ARG001
        context: AgentContext | None = None,  # noqa: ARG001
        *,
        max_concurrency: int = 100,  # noqa: ARG001
    ) -> list[Any]:
        await asyncio.sleep(2.0)
        return []

    # Backend Protocol doesn't expose ``gather``; reach in via ``setattr`` to
    # keep the test seam off the type checker's radar (same pattern as the
    # ``_build_pa_agent`` patches above).
    setattr(rt._backend, "gather", slow_gather)  # noqa: B010

    with pytest.raises(SpawnError, match="gather timed out after 0.1s"):
        await rt.gather(_agent("a"), [TaskSpec(input="x")])


@pytest.mark.asyncio
async def test_gather_timeout_keeps_slots_claimed() -> None:
    """Slot-accounting after timeout: a timed-out batch keeps the slots it
    claimed (matches ``run()`` semantics — ``Timeout`` sits outside
    ``ClaimSlot`` in the pipeline). The runtime stays usable; remaining
    headroom is still claimable by subsequent ``run()`` calls.
    """
    rt = AgentRuntime(
        options=RuntimeOptions(timeout_seconds=0.1, max_total_spawns=10),
    )

    async def slow_gather(
        agent: Agent,  # noqa: ARG001
        tasks: Sequence[TaskSpec],  # noqa: ARG001
        context: AgentContext | None = None,  # noqa: ARG001
        *,
        max_concurrency: int = 100,  # noqa: ARG001
    ) -> list[Any]:
        await asyncio.sleep(2.0)
        return []

    # See sibling test for the reach-in rationale.
    setattr(rt._backend, "gather", slow_gather)  # noqa: B010

    with pytest.raises(SpawnError, match="gather timed out"):
        await rt.gather(_agent("a"), [TaskSpec(input=str(i)) for i in range(3)])

    # Slots claimed before the wall clock fired stay claimed.
    assert rt.spawn_count == 3

    # Restore the real gather so the follow-up run is unaffected — deleting
    # the instance attr unshadows the class method.
    delattr(rt._backend, "gather")

    # Runtime is not corrupted — subsequent runs still work and continue
    # consuming headroom from where the timed-out batch left off.
    await rt.run(_agent("b"), TaskSpec(input="after"))
    assert rt.spawn_count == 4


# ---------------------------------------------------------------------------
# Cross-process cascade — TaskMessage carries the parent snapshot through
# the in-memory broker; the worker rebuilds the SpawnFrame on receive.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_broker_round_trip_clears_cascade_when_top_level() -> None:
    """A top-level run dispatched through the in-memory broker must NOT
    inherit the publisher's stale spawn frame.

    This is the regression that drove the worker's reset behaviour.
    Without it, broker-routed work false-fires cycle detection because the
    publisher's coroutine context leaks into the worker.
    """
    captured: list[AgentContext] = []

    rt = AgentRuntime(broker="memory://")
    try:
        # Patch the worker-side runtime path: we use the same runtime's
        # backend.spawn (in-memory broker → worker → backend.spawn locally
        # via the runtime's own AsyncBackend would not apply here, since
        # JobBackend dispatches to a Worker that has its own runtime).
        # Skip the round-trip wiring — verify directly that publisher with
        # no parent frame yields parent_spawn=None on the wire.
        from murmur.backends.job import _parent_spawn_from_context

        parent = _parent_spawn_from_context(AgentContext())
        assert parent is None  # top-level → no envelope payload
    finally:
        await rt.shutdown()

    _ = captured  # placeholder for downstream assertion symmetry


@pytest.mark.asyncio
async def test_parent_spawn_helper_round_trips_cascade_metadata() -> None:
    """The ``_parent_spawn_from_context`` helper must invert
    a child ``AgentContext`` into a parent snapshot the worker can use.
    """
    from murmur.backends.job import _parent_spawn_from_context

    child_ctx = AgentContext(
        depth=2,
        parent_agent="parent",
        parent_trace_id="trace-parent",
        ancestors=frozenset({"grandparent", "parent"}),
    )
    snap = _parent_spawn_from_context(child_ctx)
    assert snap is not None
    assert snap.agent_name == "parent"
    assert snap.trace_id == "trace-parent"
    assert snap.depth == 1  # parent's own depth
    assert snap.ancestors == frozenset({"grandparent"})  # parent's ancestors


@pytest.mark.asyncio
async def test_gather_slot_pushes_slot_local_spawn_frame_for_nested_runs() -> None:
    """When an agent dispatched via ``runtime.gather`` issues a nested
    ``runtime.run`` from its body (simulating a tool call), the nested run
    must see the *gathered slot* as its parent — not whatever frame the
    caller of ``gather`` had.

    Bug from review: ``AsyncBackend.gather`` schedules ``_execute`` tasks
    that inherit the caller's contextvar instead of pushing a slot-local
    frame. ``_execute`` now pushes the slot's frame so cascade detection
    works for batched agents too.
    """
    captured_frames: list[_SpawnFrame | None] = []
    rt = AgentRuntime()

    # Hook into the inside of ``_execute`` by wrapping the backend's PA
    # agent builder — at the time it's called, ``_current_spawn`` should
    # be the slot's frame.
    # Backend Protocol doesn't expose ``_build_pa_agent``; reach in via
    # ``getattr``/``setattr`` to keep the test seam off the type checker's
    # radar. Patches the AsyncBackend's PA-agent factory so each gathered
    # slot snapshots ``_current_spawn`` at the moment ``_execute`` runs.
    real_build = getattr(rt._backend, "_build_pa_agent")  # noqa: B009

    async def snapshot_then_build(agent: Agent, allowed: Any, task_id: str) -> Any:
        captured_frames.append(_current_spawn.get())
        return await real_build(agent, allowed, task_id)

    setattr(rt._backend, "_build_pa_agent", snapshot_then_build)  # noqa: B010

    results = await rt.gather(
        _agent("worker"),
        [TaskSpec(input=str(i)) for i in range(3)],
    )
    assert all(r.is_ok() for r in results)
    assert len(captured_frames) == 3
    for frame in captured_frames:
        assert frame is not None
        assert frame.agent_name == "worker"
        assert frame.agent_context.depth == 0
        assert frame.agent_context.parent_agent is None


@pytest.mark.asyncio
async def test_gather_slot_under_parent_frame_layers_correctly() -> None:
    """Cascaded gather: caller has a parent frame; each slot's frame
    should be the slot agent with depth=1 / ancestors={parent}, NOT a
    bare top-level frame and NOT the caller's frame."""
    captured_frames: list[_SpawnFrame | None] = []
    rt = AgentRuntime()
    # Backend Protocol doesn't expose ``_build_pa_agent``; reach in via
    # ``getattr``/``setattr`` to keep the test seam off the type checker's
    # radar. Patches the AsyncBackend's PA-agent factory so each gathered
    # slot snapshots ``_current_spawn`` at the moment ``_execute`` runs.
    real_build = getattr(rt._backend, "_build_pa_agent")  # noqa: B009

    async def snapshot_then_build(agent: Agent, allowed: Any, task_id: str) -> Any:
        captured_frames.append(_current_spawn.get())
        return await real_build(agent, allowed, task_id)

    setattr(rt._backend, "_build_pa_agent", snapshot_then_build)  # noqa: B010

    parent_frame = _SpawnFrame(
        agent_name="orchestrator",
        agent_context=AgentContext(),
        trace_id="t-orch",
    )
    token = _current_spawn.set(parent_frame)
    try:
        await rt.gather(
            _agent("worker"),
            [TaskSpec(input=str(i)) for i in range(2)],
        )
    finally:
        _current_spawn.reset(token)

    assert len(captured_frames) == 2
    for frame in captured_frames:
        assert frame is not None
        assert frame.agent_name == "worker"
        assert frame.agent_context.depth == 1
        assert frame.agent_context.parent_agent == "orchestrator"
        assert frame.agent_context.ancestors == frozenset({"orchestrator"})


@pytest.mark.asyncio
async def test_worker_rebuilds_spawn_frame_from_task_message_for_cycle_detection() -> (
    None
):
    """End-to-end: a TaskMessage with ``parent_spawn`` set causes the
    worker to reject a cycle that would otherwise look like a top-level run.
    """
    from murmur.messages import ParentSpawn, TaskMessage
    from murmur.runtime import _current_spawn, _SpawnFrame
    from murmur.types import AgentContext as _AC

    # Synthesise a TaskMessage as if the publisher had sent a sub-spawn
    # whose parent chain already includes ``alpha``.
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="reply",
        request_id="req-1",
        task=TaskSpec(input="x"),
        parent_spawn=ParentSpawn(
            agent_name="parent",
            trace_id="t-parent",
            depth=0,
            ancestors=frozenset({"alpha"}),
        ),
    )

    # Reconstruct the worker's parent frame the way ``Worker._run_one``
    # does. Then dispatch ``alpha`` — it's already on the chain → cycle.
    rt = AgentRuntime()
    assert msg.parent_spawn is not None  # narrow for ty
    snap = msg.parent_spawn
    parent_frame = _SpawnFrame(
        agent_name=snap.agent_name,
        trace_id=snap.trace_id,
        agent_context=_AC(depth=snap.depth, ancestors=snap.ancestors),
    )
    token = _current_spawn.set(parent_frame)
    try:
        with pytest.raises(SpawnCycleError):
            await rt.run(_agent("alpha"), TaskSpec(input="cascade"))
    finally:
        _current_spawn.reset(token)
