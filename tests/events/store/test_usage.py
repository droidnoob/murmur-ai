"""Unit tests for :func:`compute_usage` token-rollups."""

from __future__ import annotations

from datetime import UTC, datetime

from murmur.events.store.usage import compute_usage
from murmur.events.types import EventType, RuntimeEvent


def _completed(*, agent: str, trace: str, tokens: int) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name=agent,
        trace_id=trace,
        timestamp=datetime.now(tz=UTC),
        payload={"tokens_used": tokens, "duration_ms": 1000, "backend": "thread"},
    )


def _spawned(*, agent: str, trace: str) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_SPAWNED,
        agent_name=agent,
        trace_id=trace,
        timestamp=datetime.now(tz=UTC),
        payload={"backend": "thread", "trust_level": "high"},
    )


def test_compute_usage_groups_by_agent_and_sorts_descending() -> None:
    events = [
        _completed(agent="code-reviewer", trace="t1", tokens=100),
        _completed(agent="security-auditor", trace="t2", tokens=400),
        _completed(agent="code-reviewer", trace="t3", tokens=200),
    ]
    result = compute_usage(events, group_by="agent")
    assert result.totals.tokens_used == 700
    assert result.totals.events == 3
    assert [(g.key, g.tokens_used, g.events) for g in result.groups] == [
        ("security-auditor", 400, 1),
        ("code-reviewer", 300, 2),
    ]


def test_compute_usage_groups_by_trace() -> None:
    events = [
        _completed(agent="a", trace="t1", tokens=100),
        _completed(agent="b", trace="t1", tokens=50),
        _completed(agent="c", trace="t2", tokens=20),
    ]
    result = compute_usage(events, group_by="trace")
    keys = {row.key: row.tokens_used for row in result.groups}
    assert keys == {"t1": 150, "t2": 20}


def test_compute_usage_ignores_non_completed_events() -> None:
    events = [
        _spawned(agent="a", trace="t1"),
        _completed(agent="a", trace="t1", tokens=100),
        _spawned(agent="b", trace="t2"),
    ]
    result = compute_usage(events, group_by="agent")
    assert result.totals.tokens_used == 100
    assert result.totals.events == 1


def test_compute_usage_handles_missing_tokens_field() -> None:
    ev = RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name="a",
        trace_id="t",
        payload={"backend": "thread"},
    )
    result = compute_usage([ev], group_by="agent")
    assert result.totals.tokens_used == 0
    assert result.totals.events == 1


def test_compute_usage_group_by_none_returns_only_totals() -> None:
    events = [_completed(agent="a", trace="t", tokens=42)]
    result = compute_usage(events, group_by="none")
    assert result.groups == []
    assert result.totals.tokens_used == 42
    assert result.totals.events == 1
