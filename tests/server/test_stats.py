"""Unit tests for :mod:`murmur.server.stats` rollups not covered elsewhere."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from murmur.events.types import EventType, RuntimeEvent
from murmur.server.stats import compute_workers_from_heartbeats


def _heartbeat(
    *,
    runtime_id: str,
    ts: datetime,
    in_flight: int = 0,
    concurrency_cap: int = 4,
    subscribed: list[str] | None = None,
    broker_scheme: str | None = "redis",
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.WORKER_HEARTBEAT,
        agent_name=runtime_id,
        trace_id=runtime_id,
        timestamp=ts,
        payload={
            "agent_subscriptions": subscribed if subscribed is not None else ["echo"],
            "in_flight": in_flight,
            "concurrency_cap": concurrency_cap,
            "broker_scheme": broker_scheme,
            "runtime_id": runtime_id,
        },
    )


def test_keeps_only_latest_heartbeat_per_runtime() -> None:
    now = datetime.now(tz=UTC)
    events = [
        _heartbeat(runtime_id="w0", ts=now - timedelta(seconds=120), in_flight=2),
        _heartbeat(runtime_id="w0", ts=now - timedelta(seconds=10), in_flight=5),
        _heartbeat(runtime_id="w1", ts=now - timedelta(seconds=15), in_flight=1),
    ]
    rows = compute_workers_from_heartbeats(events, now=now)
    by_id = {r.id: r for r in rows}
    assert by_id["w0"].in_flight == 5  # latest, not the older 2
    assert by_id["w1"].in_flight == 1


def test_status_thresholds_healthy_stale_down() -> None:
    now = datetime.now(tz=UTC)
    events = [
        _heartbeat(runtime_id="alive", ts=now - timedelta(seconds=10)),
        _heartbeat(runtime_id="stale", ts=now - timedelta(seconds=120)),
        _heartbeat(runtime_id="gone", ts=now - timedelta(seconds=600)),
    ]
    rows = compute_workers_from_heartbeats(events, now=now)
    by_id = {r.id: r.status for r in rows}
    assert by_id["alive"] == "healthy"
    assert by_id["stale"] == "stale"
    assert by_id["gone"] == "down"


def test_carries_subscriptions_concurrency_broker_scheme() -> None:
    now = datetime.now(tz=UTC)
    ev = _heartbeat(
        runtime_id="w0",
        ts=now,
        in_flight=3,
        concurrency_cap=8,
        subscribed=["head", "math-worker"],
        broker_scheme="redis",
    )
    [row] = compute_workers_from_heartbeats([ev], now=now)
    assert row.subscribed == ["head", "math-worker"]
    assert row.concurrency == 8
    assert row.in_flight == 3
    assert row.broker == "redis"


def test_ignores_non_heartbeat_events() -> None:
    spawned = RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name="a",
        trace_id="t",
        payload={"backend": "thread", "trust_level": "high"},
    )
    assert compute_workers_from_heartbeats([spawned], now=datetime.now(tz=UTC)) == []


def _stopped(*, runtime_id: str, ts: datetime) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.WORKER_STOPPED,
        agent_name=runtime_id,
        trace_id=runtime_id,
        timestamp=ts,
        payload={
            "runtime_id": runtime_id,
            "agents": ["echo"],
            "broker_scheme": "redis",
        },
    )


def test_worker_stopped_after_last_heartbeat_marks_down() -> None:
    now = datetime.now(tz=UTC)
    events = [
        _heartbeat(runtime_id="w0", ts=now - timedelta(seconds=10)),
        _stopped(runtime_id="w0", ts=now - timedelta(seconds=5)),
    ]
    [row] = compute_workers_from_heartbeats(events, now=now)
    assert row.status == "down"


def test_worker_stopped_before_last_heartbeat_ignored() -> None:
    """A restart pattern: STOPPED then a fresh HEARTBEAT — stays healthy."""
    now = datetime.now(tz=UTC)
    events = [
        _stopped(runtime_id="w0", ts=now - timedelta(seconds=20)),
        _heartbeat(runtime_id="w0", ts=now - timedelta(seconds=5)),
    ]
    [row] = compute_workers_from_heartbeats(events, now=now)
    assert row.status == "healthy"


def test_rows_sorted_by_id() -> None:
    now = datetime.now(tz=UTC)
    events = [
        _heartbeat(runtime_id="w2", ts=now),
        _heartbeat(runtime_id="w0", ts=now),
        _heartbeat(runtime_id="w1", ts=now),
    ]
    rows = compute_workers_from_heartbeats(events, now=now)
    assert [r.id for r in rows] == ["w0", "w1", "w2"]
