"""End-to-end research pipeline over each FastStream broker.

Replaces ``InMemoryBroker`` with ``FastStreamBroker`` (Kafka / NATS /
RabbitMQ / Redis) wrapped in FastStream's ``TestBroker`` context — proving
the full distributed-mode loop works on the real wire format for every
supported broker. ``runtime.run_group`` → publisher → broker → worker →
ThreadBackend dispatch → wire envelope → publisher rehydrates typed
output, with `FanOut` driving auto fan-out across minions.

No Docker. Real-broker integration via ``testcontainers`` is a follow-up.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Callable
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel, Field
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._faststream_broker import FastStreamBroker
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.runtime import AgentRuntime
from murmur.types import FanOut, TaskSpec, TrustLevel
from murmur.worker.worker import Worker

# FastStream emits an AST-parsing RuntimeWarning during TestBroker setup
# that is harmless for our purposes.
warnings.filterwarnings("ignore", category=RuntimeWarning)


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
    answer: str
    confidence: float


class FinalReport(BaseModel):
    title: str
    findings_count: int


N_MINIONS = 10  # one round-trip per minion through real FastStream wire format


def _decomposition() -> DecompositionResult:
    return DecompositionResult(
        sub_questions=[
            SubQuestion(question=f"q-{i}", search_terms=[f"t-{i}"])
            for i in range(N_MINIONS)
        ],
        reasoning="r",
    )


def _make_canned_factory() -> Any:
    by_agent: dict[str, dict[str, Any]] = {
        "research-head": _decomposition().model_dump(),
        "research-minion": MinionFinding(answer="a", confidence=0.9).model_dump(),
        "research-summary": FinalReport(
            title="R", findings_count=N_MINIONS
        ).model_dump(),
    }

    def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        canned = by_agent[agent.name]
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _agent(name: str, output_type: type[BaseModel]) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# Per-scheme broker factories
# ---------------------------------------------------------------------------


def _kafka() -> tuple[str, str, Callable[[], Any], Callable[[Any], Any]]:
    from faststream.kafka import KafkaBroker, TestKafkaBroker

    return (
        "kafka",
        "kafka://localhost:9092",
        lambda: KafkaBroker("localhost:9092"),
        TestKafkaBroker,
    )


def _nats() -> tuple[str, str, Callable[[], Any], Callable[[Any], Any]]:
    from faststream.nats import NatsBroker, TestNatsBroker

    return (
        "nats",
        "nats://localhost:4222",
        lambda: NatsBroker("nats://localhost:4222"),
        TestNatsBroker,
    )


def _rabbit() -> tuple[str, str, Callable[[], Any], Callable[[Any], Any]]:
    from faststream.rabbit import RabbitBroker, TestRabbitBroker

    return (
        "amqp",
        "amqp://localhost:5672",
        lambda: RabbitBroker("amqp://localhost:5672"),
        TestRabbitBroker,
    )


def _redis() -> tuple[str, str, Callable[[], Any], Callable[[Any], Any]]:
    from faststream.redis import RedisBroker, TestRedisBroker

    return (
        "redis",
        "redis://localhost:6379",
        lambda: RedisBroker("redis://localhost:6379"),
        TestRedisBroker,
    )


_FACTORIES = [_kafka, _nats, _rabbit, _redis]


@pytest.fixture(params=_FACTORIES, ids=["kafka", "nats", "amqp", "redis"])
async def pipeline(
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[AgentRuntime, AgentGroup]]:
    scheme, url, broker_ctor, test_ctor = request.param()
    fs_broker = broker_ctor()

    async with test_ctor(fs_broker):
        # Two FastStreamBroker wrappers around the SAME underlying FastStream
        # broker — one for the publisher (runtime), one for the worker.
        # FastStream's TestBroker patches the transport so messages move
        # through the same in-memory routing.
        publisher_broker = FastStreamBroker(
            scheme=scheme, url=url, _fs_broker=fs_broker
        )
        worker_broker = FastStreamBroker(scheme=scheme, url=url, _fs_broker=fs_broker)

        publisher = AgentRuntime(
            broker_instance=publisher_broker,
            runtime_id=f"rt-{scheme}",
        )

        worker_backend = ThreadBackend()
        worker_backend._build_pa_agent = _make_canned_factory()
        worker_runtime = AgentRuntime(backend=worker_backend)

        head = _agent("research-head", DecompositionResult)
        minion = _agent("research-minion", MinionFinding)
        summary = _agent("research-summary", FinalReport)
        crew = AgentGroup(
            name="research",
            topology={
                head: Edge(to=(minion,)),  # auto fan-out via FanOut
                minion: Edge(
                    to=(summary,),
                    mapper=lambda findings: TaskSpec(
                        input=f"synthesise {len(findings)} findings"
                    ),
                ),
                summary: Edge.terminal(),
            },
        )

        worker = Worker(
            broker=worker_broker,
            agents={head.name: head, minion.name: minion, summary.name: summary},
            runtime=worker_runtime,
            concurrency=10,
        )
        await worker.start()
        try:
            yield publisher, crew
        finally:
            await worker.stop()


async def test_run_group_round_trips_through_faststream_broker(
    pipeline: tuple[AgentRuntime, AgentGroup],
) -> None:
    """head → 10 minions (auto fan-out via FanOut) → summary, over a real
    FastStream wire format (Kafka / NATS / Rabbit / Redis)."""
    publisher, crew = pipeline
    result = await publisher.run_group(
        crew, TaskSpec(input="What are the failure modes of LLM agents?")
    )
    assert result.is_ok()
    assert isinstance(result.output, FinalReport)
    assert result.output.findings_count == N_MINIONS
