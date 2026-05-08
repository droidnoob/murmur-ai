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
from murmur.backends._faststream_broker import FastStreamBroker, FastStreamBrokerType
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


async def _round_trip(broker: FastStreamBrokerType) -> None:
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
async def kafka_broker() -> AsyncIterator[FastStreamBrokerType]:
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
async def nats_broker() -> AsyncIterator[FastStreamBrokerType]:
    from testcontainers.nats import NatsContainer

    with NatsContainer() as container:
        url = container.nats_uri()
        broker = FastStreamBroker(scheme="nats", url=url)
        yield broker


@pytest.fixture
async def rabbit_broker() -> AsyncIterator[FastStreamBrokerType]:
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
async def redis_broker() -> AsyncIterator[FastStreamBrokerType]:
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


async def test_kafka_round_trip(kafka_broker: FastStreamBrokerType) -> None:
    await _round_trip(kafka_broker)


async def test_nats_round_trip(nats_broker: FastStreamBrokerType) -> None:
    await _round_trip(nats_broker)


async def test_rabbitmq_round_trip(rabbit_broker: FastStreamBrokerType) -> None:
    await _round_trip(rabbit_broker)


async def test_redis_round_trip(redis_broker: FastStreamBrokerType) -> None:
    await _round_trip(redis_broker)


async def test_redis_multi_worker_competing_consumer(
    redis_broker: FastStreamBrokerType,
) -> None:
    """Multiple Workers on the same Redis URL must compete for tasks, not
    broadcast. Regression for the bug where every Worker received every
    TaskMessage — tripled LLM cost and orphan results on the publisher.

    Spins up three Workers attached to the same broker, fans 12 tasks via
    ``runtime.gather``, and asserts every task lands in exactly one Worker
    with no duplicates.
    """
    agent = _agent()
    publisher = AgentRuntime(broker_instance=redis_broker, runtime_id="rt-multi")
    workers = [
        Worker(
            broker=redis_broker,
            agents={agent.name: agent},
            runtime=_worker_runtime(),
            concurrency=4,
        )
        for _ in range(3)
    ]
    seen: dict[int, list[str]] = {i: [] for i in range(len(workers))}
    for i, w in enumerate(workers):
        # late-binding the index into the closure
        @w.on_task_complete
        async def _record(
            task_id: str, _agent_name: str, _duration_ms: int, _i: int = i
        ) -> None:
            seen[_i].append(task_id)

        await w.start()

    try:
        results = await publisher.gather(
            agent,
            [TaskSpec(input=f"task-{n}") for n in range(12)],
            max_concurrency=12,
        )
    finally:
        for w in workers:
            await w.stop()

    assert all(r.is_ok() for r in results)
    assert len(results) == 12
    # Every task_id appears in exactly one Worker's bucket — no broadcast.
    flat = [tid for bucket in seen.values() for tid in bucket]
    assert len(flat) == 12
    assert len(set(flat)) == 12
    # And every Worker handled at least one task — load actually distributed.
    assert all(len(b) > 0 for b in seen.values()), seen


async def test_redis_stable_consumer_id_bounds_xinfo_groups(
    redis_broker: FastStreamBrokerType,
) -> None:
    """Repeated Worker start/stop cycles with a stable ``consumer_id``
    do not grow the consumer roster on the Redis Streams group.

    Regression for unbounded PEL accumulation: the old wrapper minted a
    fresh uuid4 consumer name on every subscribe, so every Worker
    restart added a new consumer to ``XINFO GROUPS``. With a stable
    ``consumer_id`` (default = runtime id), restart reuses the slot.
    """
    from redis.asyncio import Redis

    from murmur.backends._faststream_broker import FastStreamBroker as _Broker

    agent = _agent()
    redis_url = redis_broker.url

    # Each iteration spins a fresh broker wrapper connected to the same
    # Redis server. ``Worker.stop()`` tears down its broker connection,
    # so we can't reuse one wrapper across iterations — but the
    # server-side group + consumer roster persist regardless. Five
    # churns with the same ``consumer_id`` should leave exactly one
    # consumer in the group's roster.
    for _ in range(5):
        broker = _Broker(scheme="redis", url=redis_url)
        publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-pin")
        worker = Worker(
            broker=broker,
            agents={agent.name: agent},
            runtime=_worker_runtime(),
            consumer_id="pinned-pod-1",
        )
        await worker.start()
        try:
            result = await publisher.run(agent, TaskSpec(input="hi"))
            assert result.is_ok()
        finally:
            await worker.stop()

    # Ask the server directly via a clean redis-asyncio client.
    client: Redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        groups = await client.xinfo_groups("murmur.echo.tasks")
    finally:
        await client.aclose()

    assert len(groups) == 1
    [group] = groups
    # Exactly one consumer slot, despite 5 restart cycles.
    assert group["consumers"] == 1, group


async def test_redis_uuid_consumer_id_leaks_xinfo_groups(
    redis_broker: FastStreamBrokerType,
) -> None:
    """Mirror of the stable-id test — proves the contract bites both
    ways. Three Worker churns each minting a fresh ``consumer_id``
    leave three (now-stale) consumers on the group. This is the
    behaviour the stable-id default fixes.
    """
    from redis.asyncio import Redis

    from murmur.backends._faststream_broker import FastStreamBroker as _Broker

    agent = _agent()
    redis_url = redis_broker.url
    for i in range(3):
        broker = _Broker(scheme="redis", url=redis_url)
        publisher = AgentRuntime(broker_instance=broker, runtime_id=f"rt-leak-{i}")
        worker = Worker(
            broker=broker,
            agents={agent.name: agent},
            runtime=_worker_runtime(),
            consumer_id=f"leaky-pod-{i}",
        )
        await worker.start()
        try:
            result = await publisher.run(agent, TaskSpec(input="hi"))
            assert result.is_ok()
        finally:
            await worker.stop()

    client: Redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        groups = await client.xinfo_groups("murmur.echo.tasks")
    finally:
        await client.aclose()
    assert len(groups) == 1
    [group] = groups
    # Three distinct consumer ids, three slots — bounded by *fleet size*,
    # not restart count, but only because each id was unique. This pins
    # the operator contract: stable id <=> bounded roster.
    assert group["consumers"] == 3, group
