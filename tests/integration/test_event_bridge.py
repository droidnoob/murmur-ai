"""End-to-end: distributed event bridge round-trip.

Closes ``tsr``: with ``publish_events=True`` on the publisher's
:class:`AgentRuntime`, per-agent / per-tool events fired worker-side
flow back over the broker into the publisher's local emitter.

Uses :class:`InMemoryBroker` so the test exercises the full Protocol
surface (subscribe + publish + dispatch) without Docker or FastStream
being installed. The ``test_real_brokers.py`` integration tier covers
the real-FastStream variant under the ``integration`` marker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.events import EventType, RuntimeEvent
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker


class _Out(BaseModel):
    text: str


class _CollectingEmitter:
    """Captures every :class:`RuntimeEvent` for assertions."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def emit(self, event: RuntimeEvent) -> None:
        self.events.append(event)

    def types(self) -> list[EventType]:
        return [e.event_type for e in self.events]


async def _stub_pa_agent(
    agent: Agent,
    _allowed: frozenset[str],
    _task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_worker_runtime() -> AgentRuntime:
    backend = AsyncBackend()
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(backend=backend)


def _make_bridged_worker_runtime(broker: InMemoryBroker) -> AgentRuntime:
    """Worker runtime with the distributed event bridge wired *and* the
    PA-agent test stub so no real LLM is hit. Mirrors what the worker
    auto-constructs internally when ``runtime=None``, but adds the seam."""
    from murmur.events import BrokerEventBridge, LogEventEmitter, MultiEventEmitter

    emitter = MultiEventEmitter([LogEventEmitter(), BrokerEventBridge(broker)])
    backend = AsyncBackend(event_emitter=emitter)
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(backend=backend, event_emitter=emitter)


@pytest.fixture
def echo_agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel injected
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
async def wired(
    echo_agent: Agent,
) -> AsyncIterator[tuple[AgentRuntime, _CollectingEmitter]]:
    """Publisher with ``publish_events=True`` + worker over a shared broker."""
    broker = InMemoryBroker()
    emitter = _CollectingEmitter()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-bridge-1",
        event_emitter=emitter,
        publish_events=True,
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_bridged_worker_runtime(broker),
        concurrency=4,
    )
    await worker.start()
    try:
        yield publisher, emitter
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Publisher-side AGENT_DISPATCHED (always-on, even without the bridge)
# ---------------------------------------------------------------------------


async def test_dispatched_event_fires_publisher_side_on_run(
    echo_agent: Agent,
) -> None:
    """AGENT_DISPATCHED is publisher-side so the publisher gets immediate
    'task accepted by broker' visibility — independent of publish_events."""
    broker = InMemoryBroker()
    emitter = _CollectingEmitter()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-disp",
        event_emitter=emitter,
        # No publish_events — we want to confirm AGENT_DISPATCHED still fires.
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        await publisher.run(echo_agent, TaskSpec(input="hi", request_id="req-disp"))
    finally:
        await worker.stop()

    [dispatched] = [
        e for e in emitter.events if e.event_type is EventType.AGENT_DISPATCHED
    ]
    assert dispatched.agent_name == echo_agent.name
    assert dispatched.trace_id == "req-disp"
    assert dispatched.payload["backend"] == "job"
    assert dispatched.payload["trust_level"] == "sandbox"


async def test_dispatched_event_fires_per_task_on_gather(
    echo_agent: Agent,
) -> None:
    broker = InMemoryBroker()
    emitter = _CollectingEmitter()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-disp-batch",
        event_emitter=emitter,
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        tasks = [TaskSpec(input=f"q-{i}") for i in range(3)]
        await publisher.gather(echo_agent, tasks)
    finally:
        await worker.stop()

    dispatched = [
        e for e in emitter.events if e.event_type is EventType.AGENT_DISPATCHED
    ]
    assert len(dispatched) == 3
    # task_id propagates onto each dispatch event
    assert {e.task_id for e in dispatched} == {t.id for t in tasks}


# ---------------------------------------------------------------------------
# Bridge: worker-side events relay back to publisher
# ---------------------------------------------------------------------------


async def test_bridge_relays_worker_events_to_publisher(
    wired: tuple[AgentRuntime, _CollectingEmitter],
    echo_agent: Agent,
) -> None:
    publisher, emitter = wired
    result = await publisher.run(
        echo_agent, TaskSpec(input="hi", request_id="req-relay")
    )
    assert result.is_ok()

    types = emitter.types()
    # Publisher-local: AGENT_DISPATCHED.
    assert EventType.AGENT_DISPATCHED in types
    # Bridged from worker: AGENT_SPAWNED + AGENT_COMPLETED.
    assert EventType.AGENT_SPAWNED in types
    assert EventType.AGENT_COMPLETED in types
    # Order: dispatched comes before the bridged spawn (publisher publishes,
    # worker picks up, fires spawn, completes, all relayed back).
    assert types.index(EventType.AGENT_DISPATCHED) < types.index(
        EventType.AGENT_SPAWNED
    )
    assert types.index(EventType.AGENT_SPAWNED) < types.index(EventType.AGENT_COMPLETED)


async def test_bridge_preserves_trace_id_across_the_wire(
    wired: tuple[AgentRuntime, _CollectingEmitter],
    echo_agent: Agent,
) -> None:
    publisher, emitter = wired
    await publisher.run(echo_agent, TaskSpec(input="hi", request_id="req-known-trace"))

    bridged = [
        e
        for e in emitter.events
        if e.event_type in {EventType.AGENT_SPAWNED, EventType.AGENT_COMPLETED}
    ]
    assert bridged, "expected at least one bridged event"
    for ev in bridged:
        assert ev.trace_id == "req-known-trace"


async def test_bridge_isolates_concurrent_runs_by_topic(
    echo_agent: Agent,
) -> None:
    """Two publishers sharing one broker each see only their own bridged events."""
    broker = InMemoryBroker()
    emitter_a = _CollectingEmitter()
    emitter_b = _CollectingEmitter()
    publisher_a = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-A",
        event_emitter=emitter_a,
        publish_events=True,
    )
    publisher_b = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-B",
        event_emitter=emitter_b,
        publish_events=True,
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_bridged_worker_runtime(broker),
        concurrency=4,
    )
    await worker.start()
    try:
        await publisher_a.run(echo_agent, TaskSpec(input="from-a", request_id="r-a"))
        await publisher_b.run(echo_agent, TaskSpec(input="from-b", request_id="r-b"))
    finally:
        await worker.stop()

    # Each publisher only sees events tagged with its own trace_id, plus the
    # locally-fired AGENT_DISPATCHED for its own dispatch.
    a_traces = {e.trace_id for e in emitter_a.events}
    b_traces = {e.trace_id for e in emitter_b.events}
    assert a_traces == {"r-a"}
    assert b_traces == {"r-b"}


# ---------------------------------------------------------------------------
# Opt-out: publish_events=False means no relay (default)
# ---------------------------------------------------------------------------


async def test_no_relay_when_publish_events_off(
    echo_agent: Agent,
) -> None:
    """Without publish_events the publisher only sees its local events
    (AGENT_DISPATCHED + BATCH_*) — never the worker's per-agent events."""
    broker = InMemoryBroker()
    emitter = _CollectingEmitter()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-nobridge",
        event_emitter=emitter,
        # publish_events defaults to False
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        await publisher.run(echo_agent, TaskSpec(input="hi"))
    finally:
        await worker.stop()

    types = emitter.types()
    assert EventType.AGENT_DISPATCHED in types
    # No worker-side events relayed:
    assert EventType.AGENT_SPAWNED not in types
    assert EventType.AGENT_COMPLETED not in types


# ---------------------------------------------------------------------------
# Misconfiguration
# ---------------------------------------------------------------------------


def test_publish_events_without_broker_raises() -> None:
    from murmur.core.errors import SpecValidationError

    with pytest.raises(
        SpecValidationError, match="publish_events=True requires a broker"
    ):
        AgentRuntime(publish_events=True)
