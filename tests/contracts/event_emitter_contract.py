"""Shared contract suite for the ``murmur.core.protocols.events.EventEmitter``
Protocol.

Subclass :class:`EventEmitterContract` and override the ``emitter`` fixture
for each concrete implementation. Every concrete (``LogEventEmitter``,
``SSEEventEmitter``, ``MultiEventEmitter``, ``BrokerEventBridge``) is
expected to pass the same suite — that is what keeps their behaviour
aligned under the Protocol.

Per-emitter behaviours (forwarding to structlog, fanning to subscribers,
relaying via contextvar to a broker) are still tested in the matching
``tests/events/test_*`` modules; this suite covers only the invariants
that *every* emitter must satisfy: Protocol shape, idempotent emit, no
backpressure under burst, no leaks across emit() calls.
"""

from __future__ import annotations

import asyncio

import pytest

from murmur.core.protocols.events import EventEmitter
from murmur.events.types import EventType, RuntimeEvent


def _event(
    *,
    event_type: EventType = EventType.AGENT_SPAWNED,
    agent_name: str = "researcher",
    trace_id: str = "trace-1",
    task_id: str | None = "task-1",
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=event_type,
        agent_name=agent_name,
        trace_id=trace_id,
        task_id=task_id,
        payload={"backend": "thread", "trust_level": "sandbox"},
    )


class EventEmitterContract:
    """Protocol-level invariants every :class:`EventEmitter` must satisfy."""

    @pytest.fixture
    async def emitter(self) -> EventEmitter:
        raise NotImplementedError(
            "subclass must override `emitter` fixture with a concrete instance"
        )

    # ---- Protocol shape ----------------------------------------------------

    async def test_satisfies_event_emitter_protocol(
        self, emitter: EventEmitter
    ) -> None:
        """The :class:`EventEmitter` Protocol is ``@runtime_checkable``,
        so every concrete must register as an instance — Pydantic field
        types and runtime ``isinstance`` checks rely on this."""
        assert isinstance(emitter, EventEmitter)

    # ---- Basic emit contract ----------------------------------------------

    async def test_emit_returns_none(self, emitter: EventEmitter) -> None:
        """``emit`` is fire-and-forget — the return value is unspecified
        and callers shouldn't depend on it."""
        result = await emitter.emit(_event())
        assert result is None

    async def test_emit_does_not_raise_on_normal_event(
        self, emitter: EventEmitter
    ) -> None:
        """A fully-populated :class:`RuntimeEvent` must not raise.

        Emitters that internally wrap delivery (broker publish, queue
        put, SSE write) are expected to swallow their own delivery
        errors per the Protocol docstring — observability never takes
        an agent run down.
        """
        await emitter.emit(_event())

    # ---- Burst / repeated emit --------------------------------------------

    async def test_emit_handles_burst_without_blocking_runtime(
        self, emitter: EventEmitter
    ) -> None:
        """A burst of events must complete in bounded time (smoke for
        backpressure) — observability emit is on the runtime's hot
        path."""
        events = [_event(trace_id=f"trace-{i}") for i in range(50)]
        # 50 emits should not take anywhere near 1s on any reasonable
        # in-memory emitter; this catches accidental sync sleeps or
        # unbounded blocking puts.
        await asyncio.wait_for(
            asyncio.gather(*(emitter.emit(e) for e in events)),
            timeout=2.0,
        )

    # ---- Distinct event types ---------------------------------------------

    async def test_emit_accepts_every_event_type(self, emitter: EventEmitter) -> None:
        """Each :class:`EventType` value must round-trip through emit
        without special-casing — the LogEventEmitter routes failure
        types to ``aerror`` rather than ``ainfo``, but every emitter
        must accept every type without raising."""
        for event_type in EventType:
            await emitter.emit(_event(event_type=event_type))

    # ---- Concurrency-safe -------------------------------------------------

    async def test_concurrent_emits_do_not_deadlock(
        self, emitter: EventEmitter
    ) -> None:
        """Concurrent emit() calls from many tasks must complete — emitters
        with internal locks must avoid deadlocks under fan-out."""

        async def _spam(label: str, count: int) -> None:
            for i in range(count):
                await emitter.emit(_event(trace_id=f"{label}-{i}"))

        await asyncio.wait_for(
            asyncio.gather(_spam("a", 20), _spam("b", 20), _spam("c", 20)),
            timeout=2.0,
        )
