"""Per-broker tests for the broker concretes.

Parametrised over kafka / nats / amqp / redis using each broker's
``TestBroker`` (in-memory). No Docker required — the FastStream test
brokers stub out the network layer and exercise the same code path our
production wrapper uses (constructor, ``start`` / ``stop`` / ``publish`` /
``subscribe``, and runtime subscriber registration via ``sub.start()``).

Real-broker integration tests under ``testcontainers`` are a follow-up.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from murmur.backends._brokers import BackedBroker, make_broker

# ---------------------------------------------------------------------------
# Per-scheme factories: (scheme, url, broker_ctor, test_broker_ctor).
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


# NB: Redis is intentionally absent from this in-memory ``TestBroker``
# parametrize set. The wrapper publishes Redis tasks via Streams (so consumer
# groups can claim them), but FastStream's ``TestRedisBroker`` mocks the
# redis client with ``MagicMock`` — ``await client.xadd(...)`` returns a
# non-awaitable mock and the subscriber's ``xgroup_create`` blocks on the
# same. Real-Redis coverage lives in ``tests/integration/test_real_brokers.py``.
_FACTORIES: list[
    Callable[[], tuple[str, str, Callable[[], Any], Callable[[Any], Any]]]
] = [
    _kafka,
    _nats,
    _rabbit,
]


@pytest.fixture(params=_FACTORIES, ids=["kafka", "nats", "amqp"])
async def broker(request: pytest.FixtureRequest) -> AsyncIterator[BackedBroker]:
    scheme, url, broker_ctor, test_ctor = request.param()
    fs_broker = broker_ctor()
    async with test_ctor(fs_broker):
        wrapper = make_broker(scheme=scheme, url=url, _fs_broker=fs_broker)
        await wrapper.start()
        try:
            yield wrapper
        finally:
            await wrapper.stop()


# ---------------------------------------------------------------------------
# Tests — run once per broker scheme.
# ---------------------------------------------------------------------------


async def test_publish_subscribe_round_trip(broker: BackedBroker) -> None:
    received: list[bytes] = []

    async def handler(msg: bytes) -> None:
        received.append(msg)

    await broker.subscribe("topic-a", handler)
    await broker.publish("topic-a", b"hello")
    await asyncio.sleep(0.05)
    assert received == [b"hello"]


async def test_separate_topics_dont_cross(broker: BackedBroker) -> None:
    a_msgs: list[bytes] = []
    b_msgs: list[bytes] = []

    async def a(msg: bytes) -> None:
        a_msgs.append(msg)

    async def b(msg: bytes) -> None:
        b_msgs.append(msg)

    await broker.subscribe("topic-a", a)
    await broker.subscribe("topic-b", b)
    await broker.publish("topic-a", b"only-a")
    await broker.publish("topic-b", b"only-b")
    await asyncio.sleep(0.05)
    assert a_msgs == [b"only-a"]
    assert b_msgs == [b"only-b"]


async def test_publish_before_start_raises() -> None:
    """No injection — wrapper rejects publish until start() is called."""
    wrapper = make_broker(scheme="kafka", url="kafka://localhost:9092")
    with pytest.raises(RuntimeError, match="before start"):
        await wrapper.publish("t", b"x")


async def test_subscribe_before_start_raises() -> None:
    wrapper = make_broker(scheme="kafka", url="kafka://localhost:9092")

    async def h(_: bytes) -> None:
        return None

    with pytest.raises(RuntimeError, match="before start"):
        await wrapper.subscribe("t", h)


def test_unsupported_scheme_rejected() -> None:
    from murmur.core.errors import SpecValidationError

    with pytest.raises(SpecValidationError, match="unsupported broker scheme"):
        make_broker(scheme="ftp", url="ftp://example.com")


async def test_stop_is_idempotent(broker: BackedBroker) -> None:
    await broker.stop()
    await broker.stop()  # must not raise


# ---------------------------------------------------------------------------
# RedisBroker — sidecar reclaim subscriber wiring.
# Verified by mocking the inner FastStream broker (TestRedisBroker doesn't
# exercise StreamSub options); the integration suite hits a real Redis.
# ---------------------------------------------------------------------------


class _MockSubscriber:
    def __init__(self) -> None:
        self.started = False
        self.handler_set = False

    def __call__(self, _handler: object) -> None:
        self.handler_set = True

    async def start(self) -> None:
        self.started = True


class _MockFSBroker:
    def __init__(self) -> None:
        self.subscriber_calls: list[dict[str, Any]] = []

    def subscriber(self, *, stream: Any) -> _MockSubscriber:
        self.subscriber_calls.append(
            {
                "stream_name": stream.name,
                "consumer": stream.consumer,
                "group": stream.group,
                "min_idle_time": stream.min_idle_time,
                "max_records": stream.max_records,
            }
        )
        return _MockSubscriber()

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def close(self) -> None:
        return None


async def test_redis_subscribe_without_reclaim_registers_one_subscriber() -> None:
    from murmur.backends._brokers import RedisBroker

    fake = _MockFSBroker()
    wrapper = RedisBroker(url="redis://localhost:6379", _fs_broker=fake)
    await wrapper.start()

    async def h(_: bytes) -> None:
        return None

    await wrapper.subscribe("t", h, group="g", consumer_id="c0")
    assert len(fake.subscriber_calls) == 1
    assert fake.subscriber_calls[0]["min_idle_time"] is None


async def test_redis_subscribe_with_reclaim_registers_sidecar() -> None:
    from murmur.backends._brokers import RedisBroker

    fake = _MockFSBroker()
    wrapper = RedisBroker(url="redis://localhost:6379", _fs_broker=fake)
    await wrapper.start()

    async def h(_: bytes) -> None:
        return None

    await wrapper.subscribe(
        "t", h, group="g", consumer_id="c0", reclaim_min_idle_ms=15_000
    )
    assert len(fake.subscriber_calls) == 2

    primary, sidecar = fake.subscriber_calls
    # Primary reads new entries (no min_idle_time → XREADGROUP > mode).
    assert primary["min_idle_time"] is None
    # Sidecar runs XAUTOCLAIM at the configured idle threshold.
    assert sidecar["min_idle_time"] == 15_000
    # Same consumer name — reclaimed ownership is durable across restarts
    # of the live worker.
    assert primary["consumer"] == sidecar["consumer"] == "c0"
    assert primary["group"] == sidecar["group"] == "g"


async def test_redis_subscribe_reclaim_zero_no_sidecar() -> None:
    """``reclaim_min_idle_ms=0`` is the operator's "off" value."""
    from murmur.backends._brokers import RedisBroker

    fake = _MockFSBroker()
    wrapper = RedisBroker(url="redis://localhost:6379", _fs_broker=fake)
    await wrapper.start()

    async def h(_: bytes) -> None:
        return None

    await wrapper.subscribe("t", h, group="g", consumer_id="c0", reclaim_min_idle_ms=0)
    assert len(fake.subscriber_calls) == 1


async def test_redis_subscribe_reclaim_ignored_without_group() -> None:
    """No group → no PEL → reclaim is meaningless. Must not register
    a sidecar even when the operator passes the option."""
    from murmur.backends._brokers import RedisBroker

    fake = _MockFSBroker()
    wrapper = RedisBroker(url="redis://localhost:6379", _fs_broker=fake)
    await wrapper.start()

    async def h(_: bytes) -> None:
        return None

    await wrapper.subscribe("t", h, group=None, reclaim_min_idle_ms=30_000)
    assert len(fake.subscriber_calls) == 1
