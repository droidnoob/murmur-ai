"""Real-broker integration tests for ``FastStreamBroker``.

Same shape as ``test_faststream_pipeline.py`` but spins up *actual*
Kafka / NATS / RabbitMQ / Redis containers via ``testcontainers``
instead of FastStream's in-process ``TestBroker``. Verifies the wire
format end-to-end for each scheme — bytes payload, topic naming,
request_id flow through, structured-output round-trip.

Marked ``@pytest.mark.integration``: ``pytest -m integration`` runs the
suite, default ``pytest`` skips it. Each test pulls and starts a container
on first run; expect 5–15s of warm-up per test the first time.

Skipped at collection time if Docker isn't reachable on this host — the
gate matches what CI's integration job (``run-integration`` label) needs.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._faststream_broker import FastStreamBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker

# FastStream emits an AST-parsing RuntimeWarning during broker init.
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Mark every test in this module as integration — `pytest -m integration`
# opts in; default unit run skips.
pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Docker availability gate — skip the whole module if unreachable
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    try:
        import docker

        client = docker.from_env()
        client.ping()
    except Exception:  # noqa: BLE001 — any failure means we can't run the suite
        return False
    return True


if not _docker_available():  # pragma: no cover — env-specific
    pytest.skip(
        "Docker is not reachable; integration brokers cannot be started.",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Test agent + runtime factory
# ---------------------------------------------------------------------------


class _Out(BaseModel):
    text: str


def _stub_pa_factory() -> Any:
    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _worker_runtime() -> AgentRuntime:
    backend = AsyncBackend()
    backend._build_pa_agent = _stub_pa_factory()  # noqa: SLF001
    return AgentRuntime(backend=backend)


def _agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel injected
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# Per-broker fixtures + the shared end-to-end test body
# ---------------------------------------------------------------------------


async def _round_trip(broker: FastStreamBroker) -> None:
    """Single agent run from publisher → broker → worker → publisher."""
    agent = _agent()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-real")
    worker = Worker(
        broker=broker,
        agents={agent.name: agent},
        runtime=_worker_runtime(),
        concurrency=2,
    )
    await worker.start()
    try:
        result = await publisher.run(agent, TaskSpec(input="hi"))
        assert result.is_ok()
        assert isinstance(result.output, _Out)
        assert result.output.text == "ok"
    finally:
        await worker.stop()


@pytest.fixture
async def kafka_broker() -> AsyncIterator[FastStreamBroker]:
    """Kafka container. Note: requires ``docker pull confluentinc/cp-kafka:7.6.0``
    (~1GB) on first run — testcontainers will pull automatically when
    available, but a reliable CI run should warm the local cache."""
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as container:
        url = container.get_bootstrap_server()
        broker = FastStreamBroker(scheme="kafka", url=url)
        yield broker
        # ``broker`` lifecycle (start/stop) is driven by the test via
        # ``Worker`` / ``JobBackend`` — no extra cleanup needed here.


@pytest.fixture
async def nats_broker() -> AsyncIterator[FastStreamBroker]:
    from testcontainers.nats import NatsContainer

    with NatsContainer() as container:
        url = container.nats_uri()
        broker = FastStreamBroker(scheme="nats", url=url)
        yield broker


@pytest.fixture
async def rabbit_broker() -> AsyncIterator[FastStreamBroker]:
    """RabbitMQ container — pinned to 3.12 because newer RabbitMQ rejects
    ``transient_nonexcl_queues`` by default and FastStream's ``pika``-driven
    AMQP client trips that deprecation. Fix forward when FastStream's
    RabbitMQ adapter stops relying on transient queues."""
    from testcontainers.rabbitmq import RabbitMqContainer

    with RabbitMqContainer(image="rabbitmq:3.12-management") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        url = f"amqp://guest:guest@{host}:{port}"
        broker = FastStreamBroker(scheme="amqp", url=url)
        yield broker


@pytest.fixture
async def redis_broker() -> AsyncIterator[FastStreamBroker]:
    from testcontainers.redis import RedisContainer

    with RedisContainer() as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}"
        broker = FastStreamBroker(scheme="redis", url=url)
        yield broker


# ---------------------------------------------------------------------------
# One round-trip test per broker scheme
# ---------------------------------------------------------------------------


async def test_kafka_round_trip(kafka_broker: FastStreamBroker) -> None:
    await _round_trip(kafka_broker)


async def test_nats_round_trip(nats_broker: FastStreamBroker) -> None:
    await _round_trip(nats_broker)


async def test_rabbitmq_round_trip(rabbit_broker: FastStreamBroker) -> None:
    await _round_trip(rabbit_broker)


async def test_redis_round_trip(redis_broker: FastStreamBroker) -> None:
    await _round_trip(redis_broker)
