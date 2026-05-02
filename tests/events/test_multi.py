"""Tests for ``MultiEventEmitter`` (zxn.1.4)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from tests.contracts.event_emitter_contract import EventEmitterContract

from murmur.core.protocols.events import EventEmitter
from murmur.events import EventType, LogEventEmitter, MultiEventEmitter, RuntimeEvent


class _Collecting:
    """Records every event for assertion."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def emit(self, event: RuntimeEvent) -> None:
        self.events.append(event)


class _Boom:
    """Always raises — used to verify error containment."""

    def __init__(self) -> None:
        self.calls = 0

    async def emit(self, event: RuntimeEvent) -> None:
        self.calls += 1
        raise RuntimeError("emitter exploded")


def _event() -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        timestamp=datetime(2026, 5, 1, 12, tzinfo=UTC),
        agent_name="r",
        task_id="t-1",
        trace_id="req-1",
    )


# ---- protocol satisfaction ------------------------------------------------


def test_multi_emitter_satisfies_protocol() -> None:
    multi = MultiEventEmitter([_Collecting()])
    assert isinstance(multi, EventEmitter)


# ---- broadcast ------------------------------------------------------------


async def test_emit_broadcasts_to_all_emitters() -> None:
    a, b, c = _Collecting(), _Collecting(), _Collecting()
    multi = MultiEventEmitter([a, b, c])
    ev = _event()

    await multi.emit(ev)

    assert a.events == [ev]
    assert b.events == [ev]
    assert c.events == [ev]


async def test_empty_emitter_list_is_a_noop() -> None:
    multi = MultiEventEmitter([])
    await multi.emit(_event())  # must not raise


async def test_emitters_property_preserves_order() -> None:
    a, b = _Collecting(), _Collecting()
    multi = MultiEventEmitter([a, b])
    assert multi.emitters == (a, b)


# ---- error containment ----------------------------------------------------


async def test_failing_emitter_does_not_stop_siblings() -> None:
    bad = _Boom()
    good_a = _Collecting()
    good_b = _Collecting()

    multi = MultiEventEmitter([good_a, bad, good_b])
    ev = _event()

    # MUST NOT raise.
    await multi.emit(ev)

    assert good_a.events == [ev]
    assert good_b.events == [ev]
    assert bad.calls == 1


async def test_multiple_failing_emitters_dont_propagate() -> None:
    """All failures swallowed — observability never takes a run down."""
    multi = MultiEventEmitter([_Boom(), _Boom(), _Boom()])
    await multi.emit(_event())  # must not raise


# ---- ordering -------------------------------------------------------------


async def test_emitters_run_concurrently_via_gather() -> None:
    """asyncio.gather kicks off all emitters; we just check they all complete."""
    import asyncio

    arrivals: list[int] = []

    class _Slow:
        def __init__(self, idx: int, delay: float) -> None:
            self.idx = idx
            self.delay = delay

        async def emit(self, event: RuntimeEvent) -> None:  # noqa: ARG002
            await asyncio.sleep(self.delay)
            arrivals.append(self.idx)

    multi = MultiEventEmitter([_Slow(0, 0.01), _Slow(1, 0.001), _Slow(2, 0.005)])
    await multi.emit(_event())
    # All three landed; concurrent dispatch lets the fastest finish first.
    assert sorted(arrivals) == [0, 1, 2]
    assert arrivals[0] == 1  # the 1ms emitter completes first


# ---- composition with LogEventEmitter -------------------------------------


async def test_log_emitter_composes_inside_multi() -> None:
    from murmur.events import LogEventEmitter

    collected = _Collecting()
    multi = MultiEventEmitter([LogEventEmitter(), collected])

    await multi.emit(_event())

    # The collecting emitter saw the event; the log emitter ran without
    # raising (its internal try/except guards against pipeline issues).
    assert len(collected.events) == 1


# ---- runtime integration --------------------------------------------------


async def test_runtime_accepts_multi_emitter() -> None:
    """End-to-end: AgentRuntime(event_emitter=MultiEventEmitter([...])) works."""
    from typing import Any

    import pydantic_ai
    from pydantic import BaseModel
    from pydantic_ai.models.test import TestModel

    from murmur.agent import Agent
    from murmur.backends.async_backend import AsyncBackend
    from murmur.context.null import NullContextPasser
    from murmur.events import LogEventEmitter
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

    collector = _Collecting()
    multi = MultiEventEmitter([LogEventEmitter(), collector])

    backend = AsyncBackend(event_emitter=multi)
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
    result = await runtime.run(agent, TaskSpec(input="hi"))
    assert result.is_ok()

    types = [e.event_type for e in collector.events]
    assert EventType.AGENT_SPAWNED in types
    assert EventType.AGENT_COMPLETED in types


# ---------------------------------------------------------------------------
# Shared contract suite (zxn.5)
# ---------------------------------------------------------------------------


class TestMultiEventEmitterContract(EventEmitterContract):
    @pytest.fixture
    async def emitter(self) -> MultiEventEmitter:
        return MultiEventEmitter([LogEventEmitter()])


class TestEmptyMultiEventEmitterContract(EventEmitterContract):
    """Empty Multi is valid — emit() is a no-op. Contract still applies."""

    @pytest.fixture
    async def emitter(self) -> MultiEventEmitter:
        return MultiEventEmitter([])
