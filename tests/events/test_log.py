"""Tests for ``LogEventEmitter`` (zxn.1.2)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs
from tests.contracts.event_emitter_contract import EventEmitterContract

from murmur.core.protocols.events import EventEmitter
from murmur.events import EventType, LogEventEmitter, RuntimeEvent


@pytest.fixture
def emitter() -> LogEventEmitter:
    return LogEventEmitter()


def _event(
    event_type: EventType = EventType.AGENT_SPAWNED,
    *,
    payload: dict | None = None,
    task_id: str | None = "t-1",
    parent_trace_id: str | None = None,
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=event_type,
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        agent_name="researcher",
        task_id=task_id,
        trace_id="req-123",
        parent_trace_id=parent_trace_id,
        payload=payload or {},
    )


# ---- Protocol satisfaction -------------------------------------------------


def test_log_event_emitter_satisfies_protocol(emitter: LogEventEmitter) -> None:
    """Structural typing — concrete satisfies the Protocol without inheritance."""
    assert isinstance(emitter, EventEmitter)


# ---- emit forwards to structlog -------------------------------------------


async def test_emit_logs_event_type_as_event_string(
    emitter: LogEventEmitter,
) -> None:
    with capture_logs() as captured:
        await emitter.emit(_event(EventType.AGENT_SPAWNED))
    [record] = captured
    assert record["event"] == "agent_spawned"


async def test_emit_includes_top_level_fields(emitter: LogEventEmitter) -> None:
    with capture_logs() as captured:
        await emitter.emit(_event(EventType.AGENT_SPAWNED))
    [record] = captured
    assert record["agent_name"] == "researcher"
    assert record["task_id"] == "t-1"
    assert record["trace_id"] == "req-123"
    assert record["parent_trace_id"] is None
    assert record["timestamp"] == "2026-05-01T12:00:00+00:00"


async def test_emit_flattens_payload_into_kwargs(
    emitter: LogEventEmitter,
) -> None:
    with capture_logs() as captured:
        await emitter.emit(
            _event(
                EventType.TOOL_CALL_STARTED,
                payload={"tool_name": "echo", "trust_level": "medium"},
            )
        )
    [record] = captured
    assert record["tool_name"] == "echo"
    assert record["trust_level"] == "medium"


async def test_emit_routes_failures_to_error_level(
    emitter: LogEventEmitter,
) -> None:
    with capture_logs() as captured:
        await emitter.emit(
            _event(EventType.AGENT_FAILED, payload={"error": "boom"}),
        )
    [record] = captured
    assert record["log_level"] == "error"
    assert record["event"] == "agent_failed"


async def test_emit_routes_success_to_info_level(
    emitter: LogEventEmitter,
) -> None:
    with capture_logs() as captured:
        await emitter.emit(_event(EventType.AGENT_COMPLETED))
    [record] = captured
    assert record["log_level"] == "info"


@pytest.mark.parametrize(
    "event_type",
    [
        EventType.AGENT_FAILED,
        EventType.TOOL_CALL_FAILED,
        EventType.BUDGET_EXCEEDED,
        EventType.DEPTH_LIMIT_EXCEEDED,
    ],
)
async def test_failure_event_types_use_error_level(
    emitter: LogEventEmitter, event_type: EventType
) -> None:
    with capture_logs() as captured:
        await emitter.emit(_event(event_type))
    [record] = captured
    assert record["log_level"] == "error"


# ---- non-task events ------------------------------------------------------


async def test_emit_handles_none_task_id(emitter: LogEventEmitter) -> None:
    with capture_logs() as captured:
        await emitter.emit(_event(EventType.BATCH_STARTED, task_id=None))
    [record] = captured
    assert record["task_id"] is None
    assert record["event"] == "batch_started"


# ---------------------------------------------------------------------------
# Shared contract suite (zxn.5)
# ---------------------------------------------------------------------------


class TestLogEventEmitterContract(EventEmitterContract):
    @pytest.fixture
    async def emitter(self) -> LogEventEmitter:
        return LogEventEmitter()
