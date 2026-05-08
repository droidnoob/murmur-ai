"""Tests for :class:`murmur.events.BrokerEventBridge`.

The bridge publishes :class:`RuntimeEvent` envelopes onto a broker topic
read from a per-task contextvar. These tests cover:

- No-op when no topic is bound (avoids accidental fan-out).
- Publishes to the bound topic when set.
- Round-trips through the wire format (model_dump_json / model_validate_json).
- Per-asyncio-task isolation (one task's binding doesn't leak into another).
- Swallows broker errors so observability never raises.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from tests.contracts.event_emitter_contract import EventEmitterContract

from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.events import BrokerEventBridge, EventType, RuntimeEvent
from murmur.events.broker import (
    bind_event_topic,
    current_event_topic,
    reset_event_topic,
)


def _event(name: str = "researcher", trace_id: str = "trace-1") -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name=name,
        trace_id=trace_id,
        payload={"backend": "thread", "trust_level": "sandbox"},
    )


# ---------------------------------------------------------------------------
# No-op behaviour
# ---------------------------------------------------------------------------


async def test_emit_is_noop_when_no_topic_bound() -> None:
    broker = InMemoryBroker()
    await broker.start()
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:  # pragma: no cover — must not fire
        received.append(payload)

    await broker.subscribe("murmur.events.rt-x", handler)
    bridge = BrokerEventBridge(broker)

    # No bind_event_topic call — bridge must not publish anywhere.
    await bridge.emit(_event())
    await asyncio.sleep(0)  # let any (mistakenly) scheduled handler run

    assert received == []
    await broker.stop()


# ---------------------------------------------------------------------------
# Publishes to bound topic
# ---------------------------------------------------------------------------


async def test_emit_publishes_to_bound_topic() -> None:
    broker = InMemoryBroker()
    await broker.start()
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:
        received.append(payload)

    await broker.subscribe("murmur.events.rt-1", handler)
    bridge = BrokerEventBridge(broker)

    token = bind_event_topic("murmur.events.rt-1")
    try:
        await bridge.emit(_event())
    finally:
        reset_event_topic(token)
    await asyncio.sleep(0)

    assert len(received) == 1
    decoded = RuntimeEvent.model_validate_json(received[0])
    assert decoded.event_type is EventType.AGENT_SPAWNED
    assert decoded.agent_name == "researcher"
    assert decoded.trace_id == "trace-1"
    await broker.stop()


# ---------------------------------------------------------------------------
# Per-task isolation
# ---------------------------------------------------------------------------


async def test_topic_binding_is_isolated_per_asyncio_task() -> None:
    """Two concurrent runs publish to their own topics without cross-contamination."""
    broker = InMemoryBroker()
    await broker.start()
    received_a: list[RuntimeEvent] = []
    received_b: list[RuntimeEvent] = []

    async def handler_a(payload: bytes) -> None:
        received_a.append(RuntimeEvent.model_validate_json(payload))

    async def handler_b(payload: bytes) -> None:
        received_b.append(RuntimeEvent.model_validate_json(payload))

    await broker.subscribe("murmur.events.rt-A", handler_a)
    await broker.subscribe("murmur.events.rt-B", handler_b)
    bridge = BrokerEventBridge(broker)

    barrier = asyncio.Event()

    async def run_one(topic: str, name: str) -> None:
        token = bind_event_topic(topic)
        try:
            await barrier.wait()
            await bridge.emit(_event(name=name, trace_id=f"trace-{name}"))
        finally:
            reset_event_topic(token)

    task_a = asyncio.create_task(run_one("murmur.events.rt-A", "agent-a"))
    task_b = asyncio.create_task(run_one("murmur.events.rt-B", "agent-b"))
    barrier.set()
    await asyncio.gather(task_a, task_b)
    await asyncio.sleep(0)

    assert [e.agent_name for e in received_a] == ["agent-a"]
    assert [e.agent_name for e in received_b] == ["agent-b"]
    await broker.stop()


# ---------------------------------------------------------------------------
# Error containment
# ---------------------------------------------------------------------------


class _ExplodingBroker:
    """Broker stand-in whose ``publish`` always raises."""

    async def start(self) -> None:  # pragma: no cover — Protocol bookkeeping
        return None

    async def stop(self) -> None:  # pragma: no cover
        return None

    async def publish(self, topic: str, payload: bytes) -> None:
        raise RuntimeError("broker is on fire")

    async def subscribe(
        self,
        topic: str,
        handler: Any,
        *,
        group: str | None = None,
        prefetch: int | None = None,
        consumer_id: str | None = None,
        reclaim_min_idle_ms: int | None = None,
    ) -> None:  # pragma: no cover
        return None


async def test_emit_swallows_broker_publish_errors() -> None:
    """Observability sinks must never take an agent run down."""
    bridge = BrokerEventBridge(_ExplodingBroker())  # test-only stand-in

    token = bind_event_topic("murmur.events.boom")
    try:
        # Must not raise even though the broker does.
        await bridge.emit(_event())
    finally:
        reset_event_topic(token)


# ---------------------------------------------------------------------------
# Helpers — visibility into the contextvar
# ---------------------------------------------------------------------------


async def test_current_event_topic_reflects_binding() -> None:
    assert current_event_topic() is None
    token = bind_event_topic("murmur.events.peek")
    try:
        assert current_event_topic() == "murmur.events.peek"
    finally:
        reset_event_topic(token)
    assert current_event_topic() is None


async def test_explicit_none_binding_suppresses_relay() -> None:
    """Passing ``None`` to ``bind_event_topic`` makes ``emit`` a no-op even
    inside a parent context that did bind a topic — useful for child runs
    that should be silent."""
    broker = InMemoryBroker()
    await broker.start()
    received: list[bytes] = []

    async def handler(payload: bytes) -> None:  # pragma: no cover — must not run
        received.append(payload)

    await broker.subscribe("murmur.events.rt-z", handler)
    bridge = BrokerEventBridge(broker)

    outer = bind_event_topic("murmur.events.rt-z")
    try:
        inner = bind_event_topic(None)
        try:
            await bridge.emit(_event())
        finally:
            reset_event_topic(inner)
    finally:
        reset_event_topic(outer)
    await asyncio.sleep(0)

    assert received == []
    await broker.stop()


def test_bind_reset_outside_event_loop_works() -> None:
    """Contextvars don't require a running loop; the helpers should still
    work (e.g. for sync setup code in tests)."""
    assert current_event_topic() is None
    token = bind_event_topic("murmur.events.sync")
    try:
        assert current_event_topic() == "murmur.events.sync"
    finally:
        reset_event_topic(token)
    assert current_event_topic() is None


# ---------------------------------------------------------------------------
# Shared contract suite (zxn.5)
# ---------------------------------------------------------------------------


class TestBrokerEventBridgeContract(EventEmitterContract):
    """Bridge without a bound contextvar: emit() is a no-op. Contract
    still applies — the Protocol shape doesn't require successful
    delivery, just safe ``await emit(event)``."""

    @pytest.fixture
    async def emitter(self) -> BrokerEventBridge:
        broker = InMemoryBroker()
        await broker.start()
        return BrokerEventBridge(broker)
