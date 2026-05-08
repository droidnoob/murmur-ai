"""Per-broker tests for ``FastStreamBroker``.

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

from murmur.backends._faststream_broker import FastStreamBroker

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
async def broker(request: pytest.FixtureRequest) -> AsyncIterator[FastStreamBroker]:
    scheme, url, broker_ctor, test_ctor = request.param()
    fs_broker = broker_ctor()
    async with test_ctor(fs_broker):
        wrapper = FastStreamBroker(scheme=scheme, url=url, _fs_broker=fs_broker)
        await wrapper.start()
        try:
            yield wrapper
        finally:
            await wrapper.stop()


# ---------------------------------------------------------------------------
# Tests — run once per broker scheme.
# ---------------------------------------------------------------------------


async def test_publish_subscribe_round_trip(broker: FastStreamBroker) -> None:
    received: list[bytes] = []

    async def handler(msg: bytes) -> None:
        received.append(msg)

    await broker.subscribe("topic-a", handler)
    await broker.publish("topic-a", b"hello")
    await asyncio.sleep(0.05)
    assert received == [b"hello"]


async def test_separate_topics_dont_cross(broker: FastStreamBroker) -> None:
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
    wrapper = FastStreamBroker(scheme="kafka", url="kafka://localhost:9092")
    with pytest.raises(RuntimeError, match="before start"):
        await wrapper.publish("t", b"x")


async def test_subscribe_before_start_raises() -> None:
    wrapper = FastStreamBroker(scheme="kafka", url="kafka://localhost:9092")

    async def h(_: bytes) -> None:
        return None

    with pytest.raises(RuntimeError, match="before start"):
        await wrapper.subscribe("t", h)


def test_unsupported_scheme_rejected() -> None:
    from murmur.core.errors import SpecValidationError

    with pytest.raises(SpecValidationError, match="unsupported broker scheme"):
        FastStreamBroker(scheme="ftp", url="ftp://example.com")


async def test_stop_is_idempotent(broker: FastStreamBroker) -> None:
    await broker.stop()
    await broker.stop()  # must not raise
