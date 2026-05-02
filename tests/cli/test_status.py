"""Tests for ``murmur status`` — SSE consumer CLI.

The handler itself opens a real httpx stream against a URL; tests cover
the framing parser and the per-event formatter directly. The argparse
plumbing (subcommand registration, default URL, repeat-able filters)
gets exercised through ``murmur.cli.build_parser``.
"""

from __future__ import annotations

from typing import Any

import pytest

from murmur.cli import build_parser
from murmur.cli.status import _format_event, _parse_sse_frames

# ---------------------------------------------------------------------------
# argparse — subcommand wiring
# ---------------------------------------------------------------------------


def test_status_subcommand_registered() -> None:
    parser = build_parser()
    args = parser.parse_args(["status"])
    assert args.command == "status"
    assert args.url == "http://127.0.0.1:8420/events/stream"
    assert args.filter_event_type == []
    assert args.filter_agent == []
    assert args.no_reconnect is False


def test_status_url_override() -> None:
    parser = build_parser()
    args = parser.parse_args(["status", "--url", "http://prod:8420/events/stream"])
    assert args.url == "http://prod:8420/events/stream"


def test_status_filters_are_repeatable() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "status",
            "--filter-event-type",
            "agent_failed",
            "--filter-event-type",
            "tool_call_failed",
            "--filter-agent",
            "researcher",
        ]
    )
    assert args.filter_event_type == ["agent_failed", "tool_call_failed"]
    assert args.filter_agent == ["researcher"]


def test_status_no_reconnect_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(["status", "--no-reconnect"])
    assert args.no_reconnect is True


# ---------------------------------------------------------------------------
# SSE frame parser — event/data/blank-line semantics
# ---------------------------------------------------------------------------


async def _drain(lines: list[str]) -> list[dict[str, Any]]:
    async def gen() -> Any:
        for line in lines:
            yield line

    out: list[dict[str, Any]] = []
    async for body in _parse_sse_frames(gen()):
        out.append(body)
    return out


@pytest.mark.asyncio
async def test_parses_single_data_frame() -> None:
    out = await _drain(
        [
            "event: agent_spawned",
            'data: {"agent_name":"r","task_id":"t-1"}',
            "",
        ]
    )
    assert out == [
        {"agent_name": "r", "task_id": "t-1", "event_type": "agent_spawned"},
    ]


@pytest.mark.asyncio
async def test_parses_multiple_back_to_back_frames() -> None:
    out = await _drain(
        [
            "event: agent_spawned",
            'data: {"agent_name":"r"}',
            "",
            "event: agent_completed",
            'data: {"agent_name":"r","duration_ms":42}',
            "",
        ]
    )
    assert [e["event_type"] for e in out] == ["agent_spawned", "agent_completed"]
    assert out[1]["duration_ms"] == 42


@pytest.mark.asyncio
async def test_payload_embedded_event_type_wins_when_no_event_line() -> None:
    """If the SSE frame omits ``event:`` but the JSON body has
    ``event_type``, that's what we report. (LogEventEmitter / SSEEventEmitter
    both populate ``event:`` headers, but be permissive.)"""
    out = await _drain(
        [
            'data: {"event_type":"tool_call_started","tool_name":"web_search"}',
            "",
        ]
    )
    assert out[0]["event_type"] == "tool_call_started"


@pytest.mark.asyncio
async def test_event_line_does_not_overwrite_payload_field() -> None:
    """When body already has its own event_type, prefer it over the
    SSE event header — the payload is the source of truth for the
    semantic event type."""
    out = await _drain(
        [
            "event: ping",
            'data: {"event_type":"agent_failed"}',
            "",
        ]
    )
    assert out[0]["event_type"] == "agent_failed"


@pytest.mark.asyncio
async def test_skips_comments_and_pings() -> None:
    out = await _drain(
        [
            ": this is a comment",
            "event: ping",
            "",
            "event: agent_spawned",
            'data: {"agent_name":"r"}',
            "",
        ]
    )
    assert len(out) == 1
    assert out[0]["event_type"] == "agent_spawned"


@pytest.mark.asyncio
async def test_skips_malformed_json() -> None:
    out = await _drain(
        [
            "event: agent_spawned",
            "data: not-json",
            "",
            "event: agent_spawned",
            'data: {"agent_name":"ok"}',
            "",
        ]
    )
    assert len(out) == 1
    assert out[0]["agent_name"] == "ok"


@pytest.mark.asyncio
async def test_resets_event_type_after_blank_line() -> None:
    """A blank line terminates the SSE message. The next ``data:``
    without its own ``event:`` shouldn't inherit the previous header."""
    out = await _drain(
        [
            "event: agent_spawned",
            'data: {"agent_name":"r"}',
            "",
            'data: {"agent_name":"q","event_type":"agent_completed"}',
            "",
        ]
    )
    assert out[0]["event_type"] == "agent_spawned"
    assert out[1]["event_type"] == "agent_completed"


# ---------------------------------------------------------------------------
# Per-event tail formatter
# ---------------------------------------------------------------------------


def test_format_event_minimal() -> None:
    line = _format_event({"event_type": "agent_spawned"})
    assert line == "agent_spawned"


def test_format_event_with_agent_and_task() -> None:
    line = _format_event(
        {"event_type": "agent_spawned", "agent_name": "r", "task_id": "t-1"}
    )
    assert line == "agent_spawned agent=r task=t-1"


def test_format_event_with_payload_dict() -> None:
    line = _format_event(
        {
            "event_type": "tool_call_completed",
            "agent_name": "r",
            "payload": {"tool_name": "web_search", "duration_ms": 412},
        }
    )
    assert line.startswith("tool_call_completed agent=r [")
    assert "tool_name=web_search" in line
    assert "duration_ms=412" in line


def test_format_event_skips_empty_payload() -> None:
    line = _format_event(
        {"event_type": "agent_spawned", "agent_name": "r", "payload": {}}
    )
    assert "[" not in line  # empty payload doesn't render as []
