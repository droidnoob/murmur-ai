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
