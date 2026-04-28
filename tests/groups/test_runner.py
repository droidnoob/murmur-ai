"""End-to-end tests for ``runtime.run_group`` — the DAG walker.

These run the full pipeline (head → minions → summary) declaratively via
``AgentGroup``. Compared to ``tests/integration/test_research_pipeline.py``
they go through ``run_group`` and exercise both fan-out modes:

- explicit mapper (``head_to_minions`` returns ``list[TaskSpec]``)
- auto fan-out via :data:`FanOut`-annotated field

``InMemoryBroker`` + ``TestModel`` keep the tests hermetic.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel, Field
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.thread import ThreadBackend
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

    def build(
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

    def build(
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
    worker_backend = ThreadBackend()
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


async def test_run_group_rejects_multiple_terminals(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """A topology with two terminals is a valid DAG but rejected at run_group."""
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
        with pytest.raises(TopologyError, match="exactly one terminal"):
            await publisher.run_group(crew, TaskSpec(input="..."))
    finally:
        await worker.stop()
