"""Unit tests for :func:`compute_usage` token-rollups."""

from __future__ import annotations

from datetime import UTC, datetime

from murmur.events.store.usage import compute_usage, parse_group_by
from murmur.events.types import EventType, RuntimeEvent


def _completed(
    *, agent: str, trace: str, tokens: int, model: str | None = None
) -> RuntimeEvent:
    payload: dict[str, object] = {
        "tokens_used": tokens,
        "duration_ms": 1000,
        "backend": "thread",
    }
    if model is not None:
        payload["model"] = model
    return RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name=agent,
        trace_id=trace,
        timestamp=datetime.now(tz=UTC),
        payload=payload,
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


def test_compute_usage_groups_by_model() -> None:
    events = [
        _completed(agent="a", trace="t1", tokens=100, model="anthropic:sonnet"),
        _completed(agent="b", trace="t2", tokens=400, model="openai:gpt-5.2"),
        _completed(agent="c", trace="t3", tokens=200, model="anthropic:sonnet"),
    ]
    result = compute_usage(events, group_by="model")
    assert result.totals.tokens_used == 700
    assert [(g.key, g.tokens_used, g.events) for g in result.groups] == [
        ("openai:gpt-5.2", 400, 1),
        ("anthropic:sonnet", 300, 2),
    ]


def test_compute_usage_group_by_model_falls_back_to_unknown() -> None:
    events = [
        _completed(agent="a", trace="t", tokens=10),  # no model
        _completed(agent="b", trace="t", tokens=20, model=""),  # empty
    ]
    result = compute_usage(events, group_by="model")
    assert [(g.key, g.tokens_used) for g in result.groups] == [("unknown", 30)]


def test_parse_group_by_accepts_model() -> None:
    assert parse_group_by("model") == "model"
    assert parse_group_by("bogus") is None
