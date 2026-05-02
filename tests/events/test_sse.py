"""Tests for ``SSEEventEmitter`` (zxn.1.3)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs
from tests.contracts.event_emitter_contract import EventEmitterContract

from murmur.core.protocols.events import EventEmitter
from murmur.events import EventType, RuntimeEvent, SSEEventEmitter


def _event(
    event_type: EventType = EventType.AGENT_SPAWNED,
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=event_type,
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        agent_name="r",
        task_id="t-1",
        trace_id="req-1",
    )


# ---- protocol satisfaction ------------------------------------------------


def test_sse_emitter_satisfies_protocol() -> None:
    emitter = SSEEventEmitter()
    assert isinstance(emitter, EventEmitter)


# ---- construction validation ----------------------------------------------


def test_construction_rejects_non_positive_heartbeat() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval"):
        SSEEventEmitter(heartbeat_interval=0)


def test_construction_rejects_zero_queue_max() -> None:
    with pytest.raises(ValueError, match="queue_max"):
        SSEEventEmitter(queue_max=0)


# ---- emit + subscribe ------------------------------------------------------


async def test_subscriber_receives_emitted_event() -> None:
    emitter = SSEEventEmitter(heartbeat_interval=60.0)

    async def consume() -> dict[str, str]:
        agen = emitter.subscribe()
        try:
            return await asyncio.wait_for(anext(agen), timeout=1.0)
        finally:
            await agen.aclose()

    consumer = asyncio.create_task(consume())
    # Give the subscriber a tick to register.
    await asyncio.sleep(0.01)
    await emitter.emit(_event(EventType.AGENT_SPAWNED))
    received = await consumer

    assert received["event"] == "agent_spawned"
    payload = json.loads(received["data"])
    assert payload["agent_name"] == "r"
    assert payload["trace_id"] == "req-1"


async def test_multiple_subscribers_each_receive_event() -> None:
    emitter = SSEEventEmitter(heartbeat_interval=60.0)

    async def first_event(emitter: SSEEventEmitter) -> dict[str, str]:
        agen = emitter.subscribe()
        try:
            return await asyncio.wait_for(anext(agen), timeout=1.0)
        finally:
            await agen.aclose()

    consumers = [asyncio.create_task(first_event(emitter)) for _ in range(3)]
    await asyncio.sleep(0.02)
    assert emitter.subscriber_count == 3

    await emitter.emit(_event())

    results = await asyncio.gather(*consumers)
    assert all(r["event"] == "agent_spawned" for r in results)


# ---- subscriber lifecycle --------------------------------------------------


async def test_subscriber_count_decreases_after_aclose() -> None:
    emitter = SSEEventEmitter(heartbeat_interval=60.0)
    agen = emitter.subscribe()
    # Force generator startup (lazy until first __anext__).
    started = asyncio.create_task(anext(agen))  # type: ignore[arg-type]
    await asyncio.sleep(0.01)
    assert emitter.subscriber_count == 1

    started.cancel()
    with contextlib_suppress():
        await started
    await agen.aclose()
    assert emitter.subscriber_count == 0


# ---- heartbeat -------------------------------------------------------------


async def test_heartbeat_yields_ping_after_interval() -> None:
    emitter = SSEEventEmitter(heartbeat_interval=0.05)
    agen = emitter.subscribe()
    try:
        item = await asyncio.wait_for(anext(agen), timeout=1.0)
        assert item == {"event": "ping", "data": ""}
    finally:
        await agen.aclose()


# ---- overflow handling -----------------------------------------------------


async def test_full_queue_drops_event_and_logs_warning() -> None:
    """Slow consumer's queue overflows; runtime stays unblocked."""
    emitter = SSEEventEmitter(heartbeat_interval=60.0, queue_max=2)

    # Subscribe but never consume — queue fills and starts dropping.
    agen = emitter.subscribe()
    started = asyncio.create_task(anext(agen))  # type: ignore[arg-type]
    await asyncio.sleep(0.01)

    try:
        # Fill queue (capacity 2)
        await emitter.emit(_event())
        await emitter.emit(_event())
        with capture_logs() as captured:
            await emitter.emit(_event())  # this one drops
        warnings = [c for c in captured if c["event"] == "sse_subscriber_overflow"]
        assert len(warnings) == 1
        assert warnings[0]["event_type"] == "agent_spawned"
    finally:
        started.cancel()
        with contextlib_suppress():
            await started
        await agen.aclose()


# ---- runtime integration ---------------------------------------------------


async def test_runtime_emits_events_to_sse_subscriber() -> None:
    """End-to-end: agent run pushes events to an SSE subscriber via Multi."""
    from typing import Any

    import pydantic_ai
    from pydantic import BaseModel
    from pydantic_ai.models.test import TestModel

    from murmur.agent import Agent
    from murmur.backends.thread import ThreadBackend
    from murmur.context.null import NullContextPasser
    from murmur.events import LogEventEmitter, MultiEventEmitter
    from murmur.runtime import AgentRuntime
    from murmur.types import TaskSpec, TrustLevel

    class _Out(BaseModel):
        text: str

    async def _stub(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    sse = SSEEventEmitter(heartbeat_interval=60.0)
    multi = MultiEventEmitter([LogEventEmitter(), sse])

    backend = ThreadBackend(event_emitter=multi)
    backend._build_pa_agent = _stub  # ty: ignore[invalid-assignment]
    runtime = AgentRuntime(backend=backend, event_emitter=multi)
    agent = Agent(
        name="r",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )

    # Subscribe FIRST so the queue catches the run's events.
    agen = sse.subscribe()
    pump = asyncio.create_task(_collect_n(agen, 2))
    await asyncio.sleep(0.01)

    await runtime.run(agent, TaskSpec(input="hi"))

    received = await asyncio.wait_for(pump, timeout=1.0)
    event_names = [item["event"] for item in received]
    assert event_names[0] == "agent_spawned"
    assert event_names[-1] == "agent_completed"

    await agen.aclose()


# ---- helpers ---------------------------------------------------------------


def contextlib_suppress():
    import contextlib

    return contextlib.suppress(asyncio.CancelledError, Exception)


async def _collect_n(
    agen,
    n: int,  # noqa: ANN001
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    async for item in agen:
        out.append(item)
        if len(out) >= n:
            break
    return out


# ---------------------------------------------------------------------------
# Shared contract suite (zxn.5)
# ---------------------------------------------------------------------------


class TestSSEEventEmitterContract(EventEmitterContract):
    """SSE without a subscriber: emit() still satisfies the Protocol —
    events route to zero queues, no raises, no leaks. Heartbeat tasks
    only spawn on subscribe, so the contract suite never starts one."""

    @pytest.fixture
    async def emitter(self) -> SSEEventEmitter:
        # 60 s heartbeat keeps idle behaviour out of the burst test.
        return SSEEventEmitter(heartbeat_interval=60.0)
