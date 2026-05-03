"""End-to-end tests for ``runtime.run_group`` — the DAG walker.

These run the full pipeline (head → minions → summary) declaratively via
``AgentGroup``. Compared to ``tests/integration/test_research_pipeline.py``
they go through ``run_group`` and exercise both fan-out modes:

- explicit mapper (``head_to_minions`` returns ``list[TaskSpec]``)
- auto fan-out via :data:`FanOut`-annotated field

``InMemoryBroker`` + ``TestModel`` keep the tests hermetic.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel, Field
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import AllAgentsFailedError, TopologyError
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.runtime import AgentRuntime
from murmur.types import FanOut, TaskSpec, TrustLevel
from murmur.worker.worker import Worker

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class SubQuestion(BaseModel):
    question: str
    search_terms: list[str] = Field(default_factory=list)


class DecompositionResult(BaseModel):
    sub_questions: FanOut[list[SubQuestion]]
    reasoning: str = ""


class MinionFinding(BaseModel):
    question: str
    answer: str
    confidence: float


class FinalReport(BaseModel):
    title: str
    findings_count: int


N_MINIONS = 25  # smaller than 100 to keep the suite fast; mechanism is identical


# ---------------------------------------------------------------------------
# Canned outputs
# ---------------------------------------------------------------------------


def _decomposition() -> DecompositionResult:
    return DecompositionResult(
        sub_questions=[
            SubQuestion(question=f"q-{i}", search_terms=[f"t-{i}"])
            for i in range(N_MINIONS)
        ],
        reasoning="r",
    )


def _finding() -> MinionFinding:
    return MinionFinding(question="q", answer="a", confidence=0.9)


def _final() -> FinalReport:
    return FinalReport(title="R", findings_count=N_MINIONS)


def _make_canned_factory() -> Any:
    by_agent = {
        "research-head": _decomposition().model_dump(),
        "research-minion": _finding().model_dump(),
        "research-summary": _final().model_dump(),
    }

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        canned = by_agent.get(agent.name)
        if canned is None:
            raise ValueError(f"no canned output for {agent.name!r}")
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _make_failing_minion_factory() -> Any:
    """Minion always fails; head + summary succeed."""

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name == "research-minion":
            raise RuntimeError("minion-down")
        canned = (
            _decomposition().model_dump()
            if agent.name == "research-head"
            else _final().model_dump()
        )
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


# ---------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------


@pytest.fixture
def head_agent() -> Agent:
    return Agent(
        name="research-head",
        model="anthropic:claude-sonnet-4-6",
        instructions="decompose",
        output_type=DecompositionResult,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def minion_agent() -> Agent:
    return Agent(
        name="research-minion",
        model="anthropic:claude-sonnet-4-6",
        instructions="research one",
        output_type=MinionFinding,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def summary_agent() -> Agent:
    return Agent(
        name="research-summary",
        model="anthropic:claude-sonnet-4-6",
        instructions="synthesise",
        output_type=FinalReport,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# Wiring helpers
# ---------------------------------------------------------------------------


async def _wire(
    head: Agent,
    minion: Agent,
    summary: Agent,
    *,
    factory: Any | None = None,
) -> tuple[AgentRuntime, Worker]:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-group")
    worker_backend = AsyncBackend()
    worker_backend._build_pa_agent = factory or _make_canned_factory()
    worker_runtime = AgentRuntime(backend=worker_backend)
    worker = Worker(
        broker=broker,
        agents={head.name: head, minion.name: minion, summary.name: summary},
        runtime=worker_runtime,
        concurrency=10,
    )
    await worker.start()
    return publisher, worker


@pytest.fixture
async def wired(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> AsyncIterator[tuple[AgentRuntime, Worker]]:
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        yield publisher, worker
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_run_group_with_fan_out_via_mapper(
    wired: tuple[AgentRuntime, Worker],
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    publisher, _ = wired

    def head_to_minions(out: DecompositionResult) -> list[TaskSpec]:
        return [TaskSpec(input=q.model_dump_json()) for q in out.sub_questions]

    def minions_to_summary(findings: list[MinionFinding]) -> TaskSpec:
        return TaskSpec(input=f"synthesise {len(findings)} findings")

    crew = AgentGroup(
        name="research",
        topology={
            head_agent: Edge(to=(minion_agent,), mapper=head_to_minions),
            minion_agent: Edge(to=(summary_agent,), mapper=minions_to_summary),
            summary_agent: Edge.terminal(),
        },
    )

    result = await publisher.run_group(
        crew,
        TaskSpec(input="What are the failure modes of LLM agents?"),
    )
    assert result.is_ok()
    assert isinstance(result.output, FinalReport)
    assert result.output.findings_count == N_MINIONS


async def test_run_group_with_auto_fan_out_via_FanOut_annotation(
    wired: tuple[AgentRuntime, Worker],
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """No mapper between head→minion: runner discovers FanOut[list[SubQuestion]]."""
    publisher, _ = wired

    def minions_to_summary(findings: list[MinionFinding]) -> TaskSpec:
        return TaskSpec(input=f"synthesise {len(findings)} findings")

    crew = AgentGroup(
        name="auto-fanout",
        topology={
            head_agent: Edge(to=(minion_agent,)),  # no mapper — auto fan-out
            minion_agent: Edge(to=(summary_agent,), mapper=minions_to_summary),
            summary_agent: Edge.terminal(),
        },
    )

    result = await publisher.run_group(crew, TaskSpec(input="..."))
    assert result.is_ok()
    assert isinstance(result.output, FinalReport)
    assert result.output.findings_count == N_MINIONS


async def test_run_group_with_no_mapper_no_fan_out_serializes_json(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Single→single edge with no mapper: upstream output is JSON-serialised."""
    # Build a 2-node group: minion → summary. A minion finding has no FanOut
    # field, so the runner must serialise the typed output to JSON for summary.
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="serialize",
            topology={
                minion_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="research"))
        assert result.is_ok()
        assert isinstance(result.output, FinalReport)
    finally:
        await worker.stop()


async def test_run_group_raises_AllAgentsFailedError_when_all_minions_fail(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    publisher, worker = await _wire(
        head_agent,
        minion_agent,
        summary_agent,
        factory=_make_failing_minion_factory(),
    )
    try:

        def minions_to_summary(findings: list[MinionFinding]) -> TaskSpec:
            return TaskSpec(input=f"synthesise {len(findings)}")

        crew = AgentGroup(
            name="all-fail",
            topology={
                head_agent: Edge(to=(minion_agent,)),
                minion_agent: Edge(to=(summary_agent,), mapper=minions_to_summary),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(AllAgentsFailedError):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


async def test_run_group_rejects_unconditional_multi_terminal(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Multi-terminal topology without branch-routing conditions — every
    terminal fires at runtime, which the runner rejects. Branch routing
    (#26) resolves this by gating each branch with a mutually-exclusive
    condition.
    """
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="two-terminals",
            topology={
                head_agent: Edge(to=(minion_agent, summary_agent)),
                minion_agent: Edge.terminal(),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="multiple terminal results"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Conditional edges + branch routing
# ---------------------------------------------------------------------------


async def test_condition_true_fires_edge(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """``condition`` returning True is equivalent to no condition."""
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="cond-true",
            topology={
                head_agent: Edge(
                    to=(summary_agent,),
                    mapper=lambda _: TaskSpec(input="aggregated"),
                    condition=lambda _: True,
                ),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert result.agent_name == summary_agent.name
    finally:
        await worker.stop()


async def test_branch_routing_one_of_two_fires(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Two outgoing edges with mutually-exclusive conditions — only one fires."""
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        # The head's reasoning field will be "r" (from canned output).
        # We branch on whether reasoning starts with "r".
        crew = AgentGroup(
            name="branch",
            topology={
                head_agent: (
                    Edge(
                        to=(summary_agent,),
                        mapper=lambda _: TaskSpec(input="picked-summary"),
                        condition=lambda out: out.reasoning.startswith("r"),
                    ),
                    Edge(
                        to=(minion_agent,),
                        mapper=lambda _: TaskSpec(input="picked-minion"),
                        condition=lambda out: not out.reasoning.startswith("r"),
                    ),
                ),
                summary_agent: Edge.terminal(),
                minion_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        # Only the summary branch fired.
        assert result.agent_name == summary_agent.name
    finally:
        await worker.stop()


async def test_async_condition_is_awaited(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """An async predicate is awaited transparently."""
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:

        async def is_ok(out):  # noqa: ANN001 — runtime callable
            import asyncio

            await asyncio.sleep(0)
            return out.reasoning == "r"

        crew = AgentGroup(
            name="async-cond",
            topology={
                head_agent: Edge(
                    to=(summary_agent,),
                    mapper=lambda _: TaskSpec(input="x"),
                    condition=is_ok,
                ),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert result.agent_name == summary_agent.name
    finally:
        await worker.stop()


async def test_condition_raise_wrapped_in_topology_error(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:

        def boom(_out):  # noqa: ANN001
            raise ValueError("nope")

        crew = AgentGroup(
            name="raises",
            topology={
                head_agent: Edge(
                    to=(summary_agent,),
                    mapper=lambda _: TaskSpec(input="x"),
                    condition=boom,
                ),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="research-head.*research-summary"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


async def test_all_branches_skipped_raises(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """If every outgoing edge from the entry returns False, the run produces
    no terminal result."""
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="all-false",
            topology={
                head_agent: (
                    Edge(
                        to=(summary_agent,),
                        mapper=lambda _: TaskSpec(input="x"),
                        condition=lambda _: False,
                    ),
                    Edge(
                        to=(minion_agent,),
                        mapper=lambda _: TaskSpec(input="x"),
                        condition=lambda _: False,
                    ),
                ),
                summary_agent: Edge.terminal(),
                minion_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="produced no terminal result"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Multi-input aggregation
# ---------------------------------------------------------------------------


def _multi_input_factory(canned: dict[str, Any]) -> Any:
    """Per-agent canned outputs; raises if an agent isn't in the table."""

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

    return build


@pytest.fixture
def auditor_agent() -> Agent:
    return Agent(
        name="auditor",
        model="anthropic:claude-sonnet-4-6",
        instructions="audit",
        output_type=MinionFinding,  # reuse — same shape as minion
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


async def _wire_multi(
    head: Agent,
    minion: Agent,
    auditor: Agent,
    summary: Agent,
    *,
    factory: Any,
) -> tuple[AgentRuntime, Worker]:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-multi")
    worker_backend = AsyncBackend()
    worker_backend._build_pa_agent = factory
    worker_runtime = AgentRuntime(backend=worker_backend)
    worker = Worker(
        broker=broker,
        agents={
            head.name: head,
            minion.name: minion,
            auditor.name: auditor,
            summary.name: summary,
        },
        runtime=worker_runtime,
        concurrency=10,
    )
    await worker.start()
    return publisher, worker


async def test_multi_input_two_upstreams_converging(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Two upstreams converge on a synthesiser via dict-shaped mapper."""
    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    seen_keys: dict[str, list[str]] = {}

    def aggregator(inputs):  # noqa: ANN001
        seen_keys["keys"] = sorted(inputs.keys())
        return TaskSpec(input="aggregated")

    try:
        crew = AgentGroup(
            name="multi-in",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,), mapper=aggregator),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert result.agent_name == summary_agent.name
        assert seen_keys["keys"] == [auditor_agent.name, minion_agent.name]
    finally:
        await worker.stop()


async def test_multi_input_one_upstream_dead_mapper_sees_empty_list(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """If one upstream fully fails, the mapper still runs with [] for that key."""

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name == auditor_agent.name:
            raise RuntimeError("auditor-down")
        out = {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }[agent.name]
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=out),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=build
    )

    captured: dict[str, Any] = {}

    def aggregator(inputs):  # noqa: ANN001
        captured["inputs"] = inputs
        return TaskSpec(input="aggregated")

    try:
        crew = AgentGroup(
            name="multi-in-partial",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,), mapper=aggregator),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        # auditor key is present but empty.
        assert captured["inputs"][auditor_agent.name] == []
        assert isinstance(captured["inputs"][minion_agent.name], BaseModel)
    finally:
        await worker.stop()


async def test_multi_input_all_upstreams_dead_raises(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """All upstreams dead → AllAgentsFailedError, aggregator never called."""

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name in {minion_agent.name, auditor_agent.name}:
            raise RuntimeError("dead")
        out = {
            head_agent.name: _decomposition().model_dump(),
            summary_agent.name: _final().model_dump(),
        }[agent.name]
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=out),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=build
    )

    def aggregator(inputs):  # noqa: ANN001 - never called
        raise AssertionError("aggregator should not run when all upstreams are dead")

    try:
        crew = AgentGroup(
            name="multi-in-all-dead",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,), mapper=aggregator),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(AllAgentsFailedError):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


async def test_multi_input_no_aggregator_mapper_raises(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Multi-input without any mapper on incoming edges is a topology error."""
    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    try:
        crew = AgentGroup(
            name="multi-in-no-mapper",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,)),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="aggregating mapper"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


async def test_multi_input_two_aggregator_mappers_raises(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Multiple mappers on incoming edges of one node — ambiguity, reject."""
    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    try:
        crew = AgentGroup(
            name="multi-in-two-mappers",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(
                    to=(summary_agent,), mapper=lambda d: TaskSpec(input="A")
                ),
                auditor_agent: Edge(
                    to=(summary_agent,), mapper=lambda d: TaskSpec(input="B")
                ),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="multiple incoming edges with mappers"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Tier-parallel sibling dispatch
# ---------------------------------------------------------------------------


def _gated_factory(
    canned: dict[str, Any],
    *,
    barrier: asyncio.Barrier,
    gated_names: frozenset[str],
) -> Any:
    """Per-agent canned outputs whose build for ``gated_names`` waits on a barrier.

    Used to prove that two sibling nodes in one tier dispatch concurrently:
    each gated build awaits the same barrier. If the runner ran siblings
    sequentially, the second sibling's build would never start (the first
    would be blocked at the barrier forever) and the ``run_group`` call
    would time out.
    """

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name in gated_names:
            await barrier.wait()
        out = canned.get(agent.name)
        if out is None:
            raise ValueError(f"no canned output for {agent.name!r}")
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=out),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


async def test_run_group_dispatches_tier_siblings_in_parallel(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Two sibling nodes in one tier must dispatch concurrently.

    The shared ``asyncio.Barrier(2)`` only releases when both gated builds
    arrive. A sequential walker would block forever on the first sibling's
    barrier wait; the test would time out and fail.
    """
    barrier = asyncio.Barrier(2)
    factory = _gated_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        },
        barrier=barrier,
        gated_names=frozenset({minion_agent.name, auditor_agent.name}),
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    def aggregator(_inputs):  # noqa: ANN001
        return TaskSpec(input="aggregated")

    try:
        crew = AgentGroup(
            name="parallel-siblings",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,), mapper=aggregator),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        async with asyncio.timeout(5):
            result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert result.agent_name == summary_agent.name
    finally:
        await worker.stop()


async def test_run_group_linear_dag_skips_tier_parallel_helper(
    monkeypatch: pytest.MonkeyPatch,
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Single-agent tiers stay on the sequential fast path.

    Spies on the runner-local ``_dispatch_tier_parallel`` helper rather
    than patching ``asyncio.gather`` globally — that way the spy can't be
    inflated by other unrelated ``asyncio.gather`` calls from the
    backend/broker layers during the test.
    """
    from murmur.groups import runner as runner_module

    real_helper = runner_module._dispatch_tier_parallel
    calls: list[tuple[str, ...]] = []

    async def counting_helper(*, tier, **kwargs):
        calls.append(tuple(node.name for node in tier))
        return await real_helper(tier=tier, **kwargs)

    monkeypatch.setattr(runner_module, "_dispatch_tier_parallel", counting_helper)

    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="linear",
            topology={
                head_agent: Edge(to=(minion_agent,)),
                minion_agent: Edge(
                    to=(summary_agent,),
                    mapper=lambda findings: TaskSpec(input=f"sum {len(findings)}"),
                ),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert calls == [], (
            "linear DAG should not have invoked the tier-parallel helper "
            f"(calls={calls})"
        )
    finally:
        await worker.stop()


async def test_run_group_invokes_tier_parallel_helper_for_sibling_tiers(
    monkeypatch: pytest.MonkeyPatch,
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Multi-node tiers go through ``_dispatch_tier_parallel`` (positive
    side of the previous test — proves the spy isn't a no-op)."""
    from murmur.groups import runner as runner_module

    real_helper = runner_module._dispatch_tier_parallel
    calls: list[tuple[str, ...]] = []

    async def counting_helper(*, tier, **kwargs):
        calls.append(tuple(node.name for node in tier))
        return await real_helper(tier=tier, **kwargs)

    monkeypatch.setattr(runner_module, "_dispatch_tier_parallel", counting_helper)

    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )
    try:
        crew = AgentGroup(
            name="invokes-helper",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(
                    to=(summary_agent,), mapper=lambda _: TaskSpec(input="agg")
                ),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        # Exactly one tier had >=2 siblings: [minion, auditor].
        assert calls == [(minion_agent.name, auditor_agent.name)]
    finally:
        await worker.stop()


async def test_run_group_sibling_ancestors_only_contain_run_group_parent(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Two siblings in one tier must each see only the run_group's parent
    frame (or empty) in ``ancestors`` — never each other.

    Mirrors ``test_sibling_runs_do_not_see_each_other_as_ancestors`` from
    ``tests/test_runtime_cascading.py`` but exercises the run_group walker
    rather than direct ``runtime.run`` calls. ``asyncio.gather`` forks the
    parent task's contextvars at task creation, so each sibling reads the
    same parent ``_SpawnFrame`` independently.
    """
    from murmur.runtime import _current_spawn, _SpawnFrame
    from murmur.types import AgentContext

    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    captured: dict[str, AgentContext] = {}

    real_run = publisher.run

    async def capturing_run(agent_or_name, task_spec):  # noqa: ANN001
        # Snapshot the parent frame at the moment runtime.run is entered.
        # The runtime derives the child AgentContext from this frame, so
        # capturing it here matches what the dispatch will see.
        frame = _current_spawn.get()
        if frame is not None:
            ctx = AgentContext(
                depth=frame.agent_context.depth + 1,
                parent_agent=frame.agent_name,
                parent_trace_id=frame.trace_id,
                ancestors=frame.agent_context.ancestors | {frame.agent_name},
            )
        else:
            ctx = AgentContext()
        # The agent here is always an Agent instance in run_group.
        captured[agent_or_name.name] = ctx
        return await real_run(agent_or_name, task_spec)

    publisher.run = capturing_run  # ty: ignore[invalid-assignment]  # test seam

    parent_frame = _SpawnFrame(
        agent_name="orchestrator",
        agent_context=AgentContext(),
        trace_id="t-orch",
    )
    token = _current_spawn.set(parent_frame)

    def aggregator(_inputs):  # noqa: ANN001
        return TaskSpec(input="aggregated")

    try:
        crew = AgentGroup(
            name="sibling-ancestors",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(to=(auditor_agent,), mapper=lambda _: TaskSpec(input="a")),
                ),
                minion_agent: Edge(to=(summary_agent,), mapper=aggregator),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
    finally:
        _current_spawn.reset(token)
        await worker.stop()

    # Both siblings must see only the orchestrator on the chain — never
    # each other. Equivalent invariant to the cascading-spawn sibling
    # isolation test, but proven through the run_group walker.
    assert captured[minion_agent.name].ancestors == frozenset({"orchestrator"})
    assert captured[auditor_agent.name].ancestors == frozenset({"orchestrator"})
    assert captured[minion_agent.name].parent_agent == "orchestrator"
    assert captured[auditor_agent.name].parent_agent == "orchestrator"


async def test_run_group_dispatcher_failure_aborts_run(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """When one sibling's dispatcher raises (here: condition predicate),
    the whole run_group raises — gather propagates the first exception.
    """
    factory = _multi_input_factory(
        {
            head_agent.name: _decomposition().model_dump(),
            minion_agent.name: _finding().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }
    )
    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=factory
    )

    def boom(_out):  # noqa: ANN001
        raise ValueError("nope")

    try:
        crew = AgentGroup(
            name="sibling-dispatch-failure",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(
                        to=(auditor_agent,),
                        mapper=lambda _: TaskSpec(input="a"),
                        condition=boom,
                    ),
                ),
                minion_agent: Edge(
                    to=(summary_agent,), mapper=lambda _: TaskSpec(input="agg")
                ),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        with pytest.raises(TopologyError, match="research-head.*auditor"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()


async def test_run_group_cancels_inflight_sibling_on_failfast(
    head_agent: Agent,
    minion_agent: Agent,
    auditor_agent: Agent,
    summary_agent: Agent,
) -> None:
    """When one sibling fails, an in-flight sibling waiting on the broker
    must be cancelled — ``run_group`` returns promptly rather than waiting
    for the slow sibling's broker round-trip to settle.

    Without ``_dispatch_tier_parallel``'s cancel-on-failfast, this would
    hang forever (the blocking sibling never gets a result, the publisher
    never sees its ``ResultMessage``). The ``asyncio.timeout(2.0)``
    guard makes the regression failure mode visible.
    """
    block_event = asyncio.Event()  # never set

    async def gating_factory(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        if agent.name == minion_agent.name:
            # Block the minion side forever so the only way run_group
            # can return is if its broker await is cancelled.
            await block_event.wait()
        canned = {
            head_agent.name: _decomposition().model_dump(),
            auditor_agent.name: _finding().model_dump(),
            summary_agent.name: _final().model_dump(),
        }[agent.name]
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    publisher, worker = await _wire_multi(
        head_agent, minion_agent, auditor_agent, summary_agent, factory=gating_factory
    )

    def boom(_out):  # noqa: ANN001
        raise ValueError("nope")

    try:
        crew = AgentGroup(
            name="cancel-on-failfast",
            topology={
                head_agent: (
                    Edge(to=(minion_agent,), mapper=lambda _: TaskSpec(input="m")),
                    Edge(
                        to=(auditor_agent,),
                        mapper=lambda _: TaskSpec(input="a"),
                        condition=boom,
                    ),
                ),
                minion_agent: Edge(
                    to=(summary_agent,), mapper=lambda _: TaskSpec(input="agg")
                ),
                auditor_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        async with asyncio.timeout(2.0):
            with pytest.raises(TopologyError, match="research-head.*auditor"):
                await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        # Release the gate so any orphaned worker-side coroutine settles.
        block_event.set()
        await worker.stop()


# ---------------------------------------------------------------------------
# Heterogeneous fan-out — FanOut[list[T1 | T2 | ...]]
# ---------------------------------------------------------------------------


class _Question(BaseModel):
    text: str


class _Statement(BaseModel):
    claim: str


class _Command(BaseModel):
    verb: str


class _Hetero(BaseModel):
    items: FanOut[list[_Question | _Statement | _Command]]


class _Routed(BaseModel):
    """Per-item handler output. ``kind`` records which agent processed it."""

    kind: str


def _hetero_factory(canned: dict[str, Any]) -> Any:
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

    return build


@pytest.fixture
def hetero_source_agent() -> Agent:
    return Agent(
        name="hetero-source",
        model="anthropic:claude-sonnet-4-6",
        instructions="decompose into mixed item types",
        output_type=_Hetero,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def question_agent() -> Agent:
    return Agent(
        name="q-handler",
        model="anthropic:claude-sonnet-4-6",
        instructions="handle questions",
        input_type=_Question,
        output_type=_Routed,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def statement_agent() -> Agent:
    return Agent(
        name="s-handler",
        model="anthropic:claude-sonnet-4-6",
        instructions="handle statements",
        input_type=_Statement,
        output_type=_Routed,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def command_agent() -> Agent:
    return Agent(
        name="c-handler",
        model="anthropic:claude-sonnet-4-6",
        instructions="handle commands",
        input_type=_Command,
        output_type=_Routed,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


async def _wire_hetero(
    source: Agent,
    handlers: tuple[Agent, ...],
    summary: Agent,
    *,
    factory: Any,
) -> tuple[AgentRuntime, Worker]:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-hetero")
    worker_backend = AsyncBackend()
    worker_backend._build_pa_agent = factory
    worker_runtime = AgentRuntime(backend=worker_backend)
    agents = {source.name: source, summary.name: summary}
    for h in handlers:
        agents[h.name] = h
    worker = Worker(
        broker=broker, agents=agents, runtime=worker_runtime, concurrency=10
    )
    await worker.start()
    return publisher, worker


async def test_run_group_heterogeneous_fanout_routes_items_by_exact_type(
    hetero_source_agent: Agent,
    question_agent: Agent,
    statement_agent: Agent,
    command_agent: Agent,
    summary_agent: Agent,
) -> None:
    """End-to-end: a source emitting ``FanOut[list[Q | S | C]]`` with one
    of each type fires each handler exactly once for the matching item.
    """
    items: list[_Question | _Statement | _Command] = [
        _Question(text="why?"),
        _Statement(claim="because"),
        _Command(verb="run"),
    ]
    canned = {
        hetero_source_agent.name: _Hetero(items=items).model_dump(),
        question_agent.name: _Routed(kind="q").model_dump(),
        statement_agent.name: _Routed(kind="s").model_dump(),
        command_agent.name: _Routed(kind="c").model_dump(),
        summary_agent.name: _final().model_dump(),
    }
    factory = _hetero_factory(canned)
    publisher, worker = await _wire_hetero(
        hetero_source_agent,
        (question_agent, statement_agent, command_agent),
        summary_agent,
        factory=factory,
    )

    captured: dict[str, Any] = {}

    def aggregator(inputs):  # noqa: ANN001
        captured["inputs"] = inputs
        total = sum(len(v) if isinstance(v, list) else 1 for v in inputs.values() if v)
        return TaskSpec(input=f"agg {total}")

    try:
        crew = AgentGroup(
            name="hetero-3way",
            topology={
                hetero_source_agent: Edge(
                    to=(question_agent, statement_agent, command_agent)
                ),
                question_agent: Edge(to=(summary_agent,), mapper=aggregator),
                statement_agent: Edge(to=(summary_agent,)),
                command_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        # Each handler ran on its one matching item — fan-out lists of size 1.
        handler_names = (
            question_agent.name,
            statement_agent.name,
            command_agent.name,
        )
        for handler_name in handler_names:
            contribution = captured["inputs"][handler_name]
            assert isinstance(contribution, list)
            assert len(contribution) == 1
            assert isinstance(contribution[0], _Routed)
    finally:
        await worker.stop()


async def test_run_group_heterogeneous_fanout_repeated_type_fires_handler_n_times(
    hetero_source_agent: Agent,
    question_agent: Agent,
    statement_agent: Agent,
    command_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Three Question items + one Statement: the question handler runs
    three times, statement handler runs once, command handler is skipped
    (empty filter, returns []).
    """
    items: list[_Question | _Statement | _Command] = [
        _Question(text="q1"),
        _Question(text="q2"),
        _Question(text="q3"),
        _Statement(claim="s1"),
    ]
    canned = {
        hetero_source_agent.name: _Hetero(items=items).model_dump(),
        question_agent.name: _Routed(kind="q").model_dump(),
        statement_agent.name: _Routed(kind="s").model_dump(),
        command_agent.name: _Routed(kind="c").model_dump(),
        summary_agent.name: _final().model_dump(),
    }
    factory = _hetero_factory(canned)
    publisher, worker = await _wire_hetero(
        hetero_source_agent,
        (question_agent, statement_agent, command_agent),
        summary_agent,
        factory=factory,
    )

    captured: dict[str, Any] = {}

    def aggregator(inputs):  # noqa: ANN001
        captured["inputs"] = inputs
        return TaskSpec(input="agg")

    try:
        crew = AgentGroup(
            name="hetero-repeat",
            topology={
                hetero_source_agent: Edge(
                    to=(question_agent, statement_agent, command_agent)
                ),
                question_agent: Edge(to=(summary_agent,), mapper=aggregator),
                statement_agent: Edge(to=(summary_agent,)),
                command_agent: Edge(to=(summary_agent,)),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert len(captured["inputs"][question_agent.name]) == 3
        assert len(captured["inputs"][statement_agent.name]) == 1
        # Command handler dispatched against an empty list → empty contribution.
        assert captured["inputs"][command_agent.name] == []
    finally:
        await worker.stop()


async def test_single_type_fanout_still_works_unchanged(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Backward compat: ``FanOut[list[T]]`` (single-type) takes the
    existing path — no input_type required on downstream, no per-type
    filtering, identical behaviour to before heterogeneous routing.
    """
    publisher, worker = await _wire(head_agent, minion_agent, summary_agent)
    try:
        crew = AgentGroup(
            name="single-type-control",
            topology={
                head_agent: Edge(to=(minion_agent,)),  # no mapper — auto fan-out
                minion_agent: Edge(
                    to=(summary_agent,),
                    mapper=lambda findings: TaskSpec(input=f"sum {len(findings)}"),
                ),
                summary_agent: Edge.terminal(),
            },
        )
        result = await publisher.run_group(crew, TaskSpec(input="..."))
        assert result.is_ok()
        assert isinstance(result.output, FinalReport)
        assert result.output.findings_count == N_MINIONS
    finally:
        await worker.stop()
