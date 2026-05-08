"""Unit tests for :func:`compute_tools` per-tool latency rollups."""

from __future__ import annotations

from datetime import UTC, datetime

from murmur.events.store.tools import compute_tools, parse_tools_group_by
from murmur.events.types import EventType, RuntimeEvent


def _completed(*, agent: str, tool: str, duration_ms: int) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.TOOL_CALL_COMPLETED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={"tool_name": tool, "duration_ms": duration_ms, "tokens_used": 0},
    )


def _failed(*, agent: str, tool: str, duration_ms: int) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.TOOL_CALL_FAILED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={"tool_name": tool, "error": "boom", "duration_ms": duration_ms},
    )


def test_compute_tools_groups_and_orders_by_calls() -> None:
    events = [
        _completed(agent="a", tool="search", duration_ms=10),
        _completed(agent="b", tool="search", duration_ms=30),
        _completed(agent="a", tool="calc", duration_ms=2),
    ]
    result = compute_tools(events, group_by="tool")
    assert result.group_by == "tool"
    assert [(r.tool_name, r.calls) for r in result.rows] == [
        ("search", 2),
        ("calc", 1),
    ]


def test_compute_tools_percentiles() -> None:
    durations = [5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
    events = [_completed(agent="a", tool="t", duration_ms=d) for d in durations]
    result = compute_tools(events)
    [row] = result.rows
    assert row.calls == 10
    assert row.failures == 0
    # Nearest-rank with banker's rounding on idx round(0.5*9)=4 → durations[4]=100
    assert row.p50_ms == 100
    # p95 = idx round(0.95*9)=9 → 5000 ; p99 same
    assert row.p95_ms == 5000
    assert row.p99_ms == 5000
    assert row.avg_ms == sum(durations) // len(durations)


def test_compute_tools_counts_failures_in_calls_and_failures() -> None:
    events = [
        _completed(agent="a", tool="x", duration_ms=10),
        _failed(agent="a", tool="x", duration_ms=5),
        _failed(agent="a", tool="x", duration_ms=8),
    ]
    [row] = compute_tools(events).rows
    assert row.calls == 3
    assert row.failures == 2


def test_compute_tools_group_by_agent_tool_splits_by_caller() -> None:
    events = [
        _completed(agent="a", tool="search", duration_ms=10),
        _completed(agent="b", tool="search", duration_ms=20),
    ]
    result = compute_tools(events, group_by="agent_tool")
    keys = sorted(r.key for r in result.rows)
    assert keys == ["a::search", "b::search"]
    by_key = {r.key: r for r in result.rows}
    assert by_key["a::search"].agent_name == "a"
    assert by_key["a::search"].tool_name == "search"


def test_compute_tools_ignores_other_event_types() -> None:
    spawned = RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name="a",
        trace_id="t",
        payload={"backend": "thread", "trust_level": "high"},
    )
    result = compute_tools([spawned])
    assert result.rows == []


def test_compute_tools_empty_returns_empty_rows() -> None:
    assert compute_tools([]).rows == []


def test_parse_tools_group_by() -> None:
    assert parse_tools_group_by("tool") == "tool"
    assert parse_tools_group_by("agent_tool") == "agent_tool"
    assert parse_tools_group_by("agent") is None
