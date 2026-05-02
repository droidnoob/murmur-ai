"""End-to-end pipeline: research head → 100 minions → summary.

Exercises the full distributed-mode loop with three agents of different
shapes, structured Pydantic I/O, fan-out, and aggregation. ``InMemoryBroker``
stands in for a real broker; ``pydantic_ai.models.test.TestModel`` returns
canned outputs so no real LLM is hit. The test asserts:

- The publisher-side runtime publishes ``TaskMessage`` envelopes onto the
  per-agent topic.
- The worker consumes them, dispatches via its inner ThreadBackend runtime,
  publishes ``ResultMessage`` back, and the publisher rehydrates the typed
  output against ``agent.output_type`` (this is the only thing that breaks
  if generic ``BaseModel`` deserialization is wrong).
- 100 minions fan out and come back in order, every slot typed correctly.
- Three sequential dispatches (head → gather → summary) cooperate cleanly
  on a single broker instance.

If this test passes, the distributed runtime is functionally correct
end-to-end. The only thing it does **not** exercise is a real broker
(Kafka/NATS/Rabbit/Redis); that lives in ``tests/integration/`` behind
``@pytest.mark.integration`` once the FastStream wrappers light up.
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
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class SubQuestion(BaseModel):
    question: str
    search_terms: list[str] = Field(default_factory=list)


class DecompositionResult(BaseModel):
    sub_questions: list[SubQuestion]
    reasoning: str = ""


class MinionFinding(BaseModel):
    question: str
    answer: str
    confidence: float
    sources: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)


class FinalReport(BaseModel):
    title: str
    summary: str
    findings_count: int


N_MINIONS = 100


# ---------------------------------------------------------------------------
# Canned per-agent dispatcher
# ---------------------------------------------------------------------------


def _make_canned_pa_factory(
    *,
    n_subquestions: int = N_MINIONS,
) -> object:
    """Return a ``_build_pa_agent`` replacement that produces canned outputs.

    Each Murmur agent name maps to a ``pydantic_ai.Agent`` configured with a
    ``TestModel`` that emits a fixed structured output. No LLM, no network.
    """

    decomposition = DecompositionResult(
        sub_questions=[
            SubQuestion(
                question=f"sub-question {i}",
                search_terms=[f"term-{i}-a", f"term-{i}-b"],
            )
            for i in range(n_subquestions)
        ],
        reasoning=f"decomposed into {n_subquestions} parts",
    )

    minion_finding = MinionFinding(
        question="canned",
        answer="canned answer",
        confidence=0.85,
        sources=["http://example.com/source"],
        key_facts=["a representative fact"],
    )

    final_report = FinalReport(
        title="LLM Agent Failure Modes",
        summary=f"Synthesised {n_subquestions} findings into one report.",
        findings_count=n_subquestions,
    )

    by_agent: dict[str, dict[str, Any]] = {
        "research-head": decomposition.model_dump(),
        "research-minion": minion_finding.model_dump(),
        "research-summary": final_report.model_dump(),
    }

    async def build_pa_agent(
        agent: Agent,
        _allowed: frozenset[str],
        _task_id: str,
    ) -> pydantic_ai.Agent[None, Any]:
        canned = by_agent.get(agent.name)
        if canned is None:
            raise ValueError(f"no canned output for agent {agent.name!r}")
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build_pa_agent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def head_agent() -> Agent:
    return Agent(
        name="research-head",
        model="anthropic:claude-sonnet-4-6",
        instructions="Decompose the research question into sub-questions.",
        output_type=DecompositionResult,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def minion_agent() -> Agent:
    return Agent(
        name="research-minion",
        model="anthropic:claude-sonnet-4-6",
        instructions="Research a single sub-question and produce one finding.",
        output_type=MinionFinding,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def summary_agent() -> Agent:
    return Agent(
        name="research-summary",
        model="anthropic:claude-sonnet-4-6",
        instructions="Synthesise the findings into a final report.",
        output_type=FinalReport,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
async def pipeline(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> AsyncIterator[tuple[AgentRuntime, Worker]]:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-research")

    worker_backend = ThreadBackend()
    worker_backend._build_pa_agent = _make_canned_pa_factory()  # ty: ignore[invalid-assignment]  # test seam
    worker_runtime = AgentRuntime(backend=worker_backend)

    worker = Worker(
        broker=broker,
        agents={
            head_agent.name: head_agent,
            minion_agent.name: minion_agent,
            summary_agent.name: summary_agent,
        },
        runtime=worker_runtime,
        concurrency=20,
    )
    await worker.start()
    try:
        yield publisher, worker
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_head_decomposes_into_n_subquestions(
    pipeline: tuple[AgentRuntime, Worker],
    head_agent: Agent,
) -> None:
    publisher, _ = pipeline
    result = await publisher.run(
        head_agent,
        TaskSpec(input="What are the failure modes of LLM agents?"),
    )
    assert result.is_ok()
    assert isinstance(result.output, DecompositionResult)
    assert len(result.output.sub_questions) == N_MINIONS
    assert all(isinstance(q, SubQuestion) for q in result.output.sub_questions)


async def test_gather_fans_out_to_n_minions(
    pipeline: tuple[AgentRuntime, Worker],
    minion_agent: Agent,
) -> None:
    publisher, _ = pipeline
    tasks = [
        TaskSpec(input=f"sub-question {i}: please research") for i in range(N_MINIONS)
    ]
    results = await publisher.gather(minion_agent, tasks)
    assert len(results) == N_MINIONS
    assert all(r.is_ok() for r in results)
    assert all(isinstance(r.output, MinionFinding) for r in results)
    # task_ids preserved across the broker round-trip
    assert {r.task_id for r in results} == {t.id for t in tasks}


async def test_full_pipeline_head_then_100_minions_then_summary(
    pipeline: tuple[AgentRuntime, Worker],
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    publisher, _ = pipeline

    # Stage 1: head decomposes the question.
    head_result = await publisher.run(
        head_agent,
        TaskSpec(input="What are the failure modes of LLM agents?"),
    )
    assert head_result.is_ok()
    assert isinstance(head_result.output, DecompositionResult)
    sub_questions = head_result.output.sub_questions
    assert len(sub_questions) == N_MINIONS

    # Stage 2: fan out one minion per sub-question.
    minion_tasks = [TaskSpec(input=q.model_dump_json()) for q in sub_questions]
    minion_results = await publisher.gather(minion_agent, minion_tasks)
    assert len(minion_results) == N_MINIONS
    assert all(r.is_ok() for r in minion_results)
    findings: list[MinionFinding] = [
        r.output for r in minion_results if isinstance(r.output, MinionFinding)
    ]
    assert len(findings) == N_MINIONS

    # Stage 3: summary agent over the (canned) successful findings.
    summary_result = await publisher.run(
        summary_agent,
        TaskSpec(input=f"synthesise {len(findings)} findings"),
    )
    assert summary_result.is_ok()
    assert isinstance(summary_result.output, FinalReport)
    assert summary_result.output.findings_count == N_MINIONS
    assert "LLM" in summary_result.output.title


async def test_pipeline_lifecycle_hooks_fire_for_every_dispatch(
    head_agent: Agent,
    minion_agent: Agent,
    summary_agent: Agent,
) -> None:
    """Worker hooks fire once per dispatched task across head, gather, summary."""
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-hooks")

    worker_backend = ThreadBackend()
    worker_backend._build_pa_agent = _make_canned_pa_factory(n_subquestions=10)  # ty: ignore[invalid-assignment]
    worker_runtime = AgentRuntime(backend=worker_backend)

    started: list[str] = []
    completed: list[str] = []

    worker = Worker(
        broker=broker,
        agents={
            head_agent.name: head_agent,
            minion_agent.name: minion_agent,
            summary_agent.name: summary_agent,
        },
        runtime=worker_runtime,
        concurrency=10,
    )

    @worker.on_task_start
    async def _on_start(_task_id: str, agent_name: str) -> None:
        started.append(agent_name)

    @worker.on_task_complete
    async def _on_complete(_task_id: str, agent_name: str, _ms: int) -> None:
        completed.append(agent_name)

    await worker.start()
    try:
        head_result = await publisher.run(head_agent, TaskSpec(input="x"))
        assert isinstance(head_result.output, DecompositionResult)
        minion_tasks = [
            TaskSpec(input=q.model_dump_json())
            for q in head_result.output.sub_questions
        ]
        await publisher.gather(minion_agent, minion_tasks)
        await publisher.run(summary_agent, TaskSpec(input="x"))
    finally:
        await worker.stop()

    # 1 head + 10 minions + 1 summary = 12 dispatches.
    assert len(started) == 12
    assert len(completed) == 12
    assert started.count("research-head") == 1
    assert started.count("research-minion") == 10
    assert started.count("research-summary") == 1
