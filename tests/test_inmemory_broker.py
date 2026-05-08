"""Unit tests for ``murmur.backends._inmemory_broker.InMemoryBroker``."""

from __future__ import annotations

import asyncio

import pytest

from murmur.backends._inmemory_broker import InMemoryBroker


async def test_publish_before_start_raises() -> None:
    broker = InMemoryBroker()
    with pytest.raises(RuntimeError, match="before start"):
        await broker.publish("topic", b"x")


async def test_subscribe_then_publish_routes_to_handler() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await broker.subscribe("topic", handler)
    await broker.start()
    await broker.publish("topic", b"hello")

    # publish dispatches via create_task — give the loop a tick to run them.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert received == [b"hello"]
    await broker.stop()


async def test_multiple_handlers_same_topic_all_fire() -> None:
    broker = InMemoryBroker()
    seen_a: list[bytes] = []
    seen_b: list[bytes] = []

    async def a(p: bytes) -> None:
        seen_a.append(p)

    async def b(p: bytes) -> None:
        seen_b.append(p)

    await broker.subscribe("t", a)
    await broker.subscribe("t", b)
    await broker.start()
    await broker.publish("t", b"X")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen_a == [b"X"]
    assert seen_b == [b"X"]
    await broker.stop()


async def test_unrelated_topic_ignored() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(p: bytes) -> None:
        received.append(p)

    await broker.subscribe("topic-a", handler)
    await broker.start()
    await broker.publish("topic-b", b"X")
    await asyncio.sleep(0)
    assert received == []
    await broker.stop()


async def test_handler_failure_does_not_break_publisher() -> None:
    broker = InMemoryBroker()

    async def boom(_: bytes) -> None:
        raise RuntimeError("oops")

    await broker.subscribe("t", boom)
    await broker.start()
    # Publisher must not surface the handler's exception.
    await broker.publish("t", b"X")
    await asyncio.sleep(0)
    await broker.stop()


async def test_stop_is_idempotent() -> None:
    broker = InMemoryBroker()
    await broker.start()
    await broker.stop()
    await broker.stop()  # must not raise


async def test_competing_consumer_group_routes_to_one_handler() -> None:
    """``group=<str>`` puts subscribers into a competing-consumer pool —
    each published message reaches exactly one handler in the pool, not
    every handler. Without this, multi-Worker fan-out triples LLM cost
    and produces orphan results.

    Asserts the *delivery* invariants only: every published message
    lands in exactly one handler in the pool, no duplicates, no losses.
    Real brokers (Redis Streams, Kafka consumer groups, NATS queues)
    distribute by first-come-first-served, not round-robin — pinning
    exact per-handler counts here would test the in-memory implementation
    detail rather than the contract every concrete must honour.
    """
    broker = InMemoryBroker()
    received_a: list[bytes] = []
    received_b: list[bytes] = []
    received_c: list[bytes] = []

    async def a(p: bytes) -> None:
        received_a.append(p)

    async def b(p: bytes) -> None:
        received_b.append(p)

    async def c(p: bytes) -> None:
        received_c.append(p)

    await broker.subscribe("workers", a, group="g1")
    await broker.subscribe("workers", b, group="g1")
    await broker.subscribe("workers", c, group="g1")
    await broker.start()
    for i in range(6):
        await broker.publish("workers", f"msg-{i}".encode())
    for _ in range(8):
        await asyncio.sleep(0)
    # Each message landed in exactly one handler.
    union = sorted(received_a + received_b + received_c)
    assert union == [f"msg-{i}".encode() for i in range(6)]
    # No handler saw a duplicate.
    for bucket in (received_a, received_b, received_c):
        assert len(bucket) == len(set(bucket))
    await broker.stop()


async def test_broadcast_and_competing_consumer_coexist() -> None:
    """A topic can carry both broadcast and competing-consumer subscribers
    side-by-side. Production needs this: the events bridge keeps broadcast
    semantics for observers while Workers compete for tasks.
    """
    broker = InMemoryBroker()
    seen_broadcast: list[bytes] = []
    seen_pool: list[tuple[bytes, bytes]] = []

    async def observer(p: bytes) -> None:
        seen_broadcast.append(p)

    async def worker_a(p: bytes) -> None:
        seen_pool.append((b"a", p))

    async def worker_b(p: bytes) -> None:
        seen_pool.append((b"b", p))

    await broker.subscribe("t", observer)  # broadcast
    await broker.subscribe("t", worker_a, group="pool")
    await broker.subscribe("t", worker_b, group="pool")
    await broker.start()
    for i in range(4):
        await broker.publish("t", f"m{i}".encode())
    for _ in range(6):
        await asyncio.sleep(0)
    # Observer sees every message exactly once.
    assert sorted(seen_broadcast) == [f"m{i}".encode() for i in range(4)]
    # Pool delivered every message exactly once across its members,
    # without duplicates.
    pool_payloads = sorted(p for _, p in seen_pool)
    assert pool_payloads == [f"m{i}".encode() for i in range(4)]
    await broker.stop()


async def test_distinct_groups_each_get_every_message() -> None:
    """Two groups on the same topic each form their own competing pool —
    every message lands in each group exactly once, regardless of the
    other group.
    """
    broker = InMemoryBroker()
    g1: list[bytes] = []
    g2: list[bytes] = []

    async def h1(p: bytes) -> None:
        g1.append(p)

    async def h2(p: bytes) -> None:
        g2.append(p)

    await broker.subscribe("t", h1, group="g1")
    await broker.subscribe("t", h2, group="g2")
    await broker.start()
    for i in range(3):
        await broker.publish("t", f"m{i}".encode())
    for _ in range(4):
        await asyncio.sleep(0)
    assert g1 == [b"m0", b"m1", b"m2"]
    assert g2 == [b"m0", b"m1", b"m2"]
    await broker.stop()
