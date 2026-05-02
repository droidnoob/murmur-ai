"""Tests for ``RuntimeEvent`` and ``EventType`` (zxn.1.1)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from murmur.events import EventType, RuntimeEvent
from murmur.events import (
    EventType as ReexportedEventType,
)
from murmur.events import (
    RuntimeEvent as ReexportedRuntimeEvent,
)
from murmur.events.types import EventType as DirectEventType
from murmur.events.types import RuntimeEvent as DirectRuntimeEvent

# ---- Re-exports ------------------------------------------------------------


def test_package_reexports() -> None:
    assert ReexportedRuntimeEvent is DirectRuntimeEvent
    assert ReexportedEventType is DirectEventType


# ---- EventType -------------------------------------------------------------


def test_event_type_values_are_lowercase_strings() -> None:
    """Wire-format stability: the enum values shouldn't drift."""
    assert EventType.AGENT_SPAWNED == "agent_spawned"
    assert EventType.TOOL_CALL_STARTED == "tool_call_started"
    assert EventType.BUDGET_EXCEEDED == "budget_exceeded"
    assert EventType.GROUP_COMPLETED == "group_completed"


def test_event_type_complete_set() -> None:
    """All event types declared in phase-2 design are present."""
    expected = {
        "agent_dispatched",
        "agent_spawned",
        "agent_completed",
        "agent_failed",
        "tool_call_started",
        "tool_call_completed",
        "tool_call_failed",
        "batch_started",
        "batch_completed",
        "group_started",
        "group_completed",
        "budget_exceeded",
        "depth_limit_exceeded",
    }
    assert {e.value for e in EventType} == expected


# ---- RuntimeEvent ----------------------------------------------------------


def test_minimal_runtime_event() -> None:
    ev = RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name="researcher",
        trace_id="req-123",
    )
    assert ev.event_type is EventType.AGENT_SPAWNED
    assert ev.agent_name == "researcher"
    assert ev.trace_id == "req-123"
    assert ev.task_id is None
    assert ev.parent_trace_id is None
    assert ev.payload == {}


def test_full_runtime_event() -> None:
    now = datetime.now(tz=UTC)
    ev = RuntimeEvent(
        event_type=EventType.TOOL_CALL_COMPLETED,
        timestamp=now,
        agent_name="r",
        task_id="t-99",
        trace_id="req-99",
        parent_trace_id="req-parent",
        payload={"tool_name": "echo", "duration_ms": 42},
    )
    assert ev.timestamp == now
    assert ev.task_id == "t-99"
    assert ev.parent_trace_id == "req-parent"
    assert ev.payload["tool_name"] == "echo"


def test_default_timestamp_is_recent_utc() -> None:
    ev = RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name="r",
        trace_id="req-1",
    )
    delta = datetime.now(tz=UTC) - ev.timestamp
    assert timedelta(seconds=0) <= delta < timedelta(seconds=5)
    assert ev.timestamp.tzinfo is UTC


def test_runtime_event_is_frozen() -> None:
    ev = RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name="r",
        trace_id="req-1",
    )
    with pytest.raises(ValidationError):
        ev.agent_name = "other"  # type: ignore[misc]


def test_runtime_event_requires_agent_name_and_trace_id() -> None:
    # Pydantic raises at runtime; ty would otherwise block the negative test.
    with pytest.raises(ValidationError):
        RuntimeEvent(event_type=EventType.AGENT_SPAWNED)  # ty: ignore[missing-argument]


def test_runtime_event_round_trips_through_json() -> None:
    """Crosses broker boundaries safely — payload values stay primitive."""
    ev = RuntimeEvent(
        event_type=EventType.TOOL_CALL_STARTED,
        agent_name="r",
        task_id="t-1",
        trace_id="req-1",
        payload={"tool_name": "echo", "trust_level": "medium"},
    )
    serialised = ev.model_dump_json()
    rehydrated = RuntimeEvent.model_validate_json(serialised)
    assert rehydrated == ev


# ---- Protocol surface ------------------------------------------------------


def test_protocol_emit_takes_runtime_event() -> None:
    """Sanity-check the Protocol method signature points at RuntimeEvent."""
    import inspect

    from murmur.core.protocols.events import EventEmitter

    sig = inspect.signature(EventEmitter.emit)
    params = list(sig.parameters.values())
    # self + event
    assert len(params) == 2
    assert params[1].name == "event"
