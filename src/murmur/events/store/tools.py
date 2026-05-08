"""Per-tool latency + call rollups computed from persisted events.

Source of truth: ``TOOL_CALL_COMPLETED`` and ``TOOL_CALL_FAILED`` events.
Same compute-on-query pattern as :mod:`murmur.events.store.usage` — pure
Python aggregation over an :class:`EventStore.query` result. Pushes down
into the store only if rollup latency becomes a problem.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterable

    from murmur.events.types import RuntimeEvent

ToolsGroupBy = Literal["tool", "agent_tool"]


class ToolStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    """Either tool name (group_by='tool') or 'agent_name::tool_name'
    (group_by='agent_tool')."""

    tool_name: str
    agent_name: str | None
    calls: int
    failures: int
    p50_ms: int
    p95_ms: int
    p99_ms: int
    avg_ms: int


class ToolsReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    group_by: ToolsGroupBy
    rows: list[ToolStats]


class _Bucket:
    __slots__ = ("agent_name", "calls", "durations", "failures", "tool_name")

    def __init__(self, *, tool_name: str, agent_name: str | None) -> None:
        self.tool_name = tool_name
        self.agent_name = agent_name
        self.calls: int = 0
        self.failures: int = 0
        self.durations: list[int] = []


def compute_tools(
    events: Iterable[RuntimeEvent],
    *,
    group_by: ToolsGroupBy = "tool",
) -> ToolsReport:
    """Aggregate tool-call lifecycle events into per-tool latency rows.

    Counts both ``TOOL_CALL_COMPLETED`` and ``TOOL_CALL_FAILED`` toward
    ``calls`` (a failure is still a call). ``failures`` only counts
    ``TOOL_CALL_FAILED``. Latency percentiles are computed across both
    paths since both carry ``duration_ms``.
    """
    from murmur.events.types import EventType

    buckets: dict[str, _Bucket] = {}
    for ev in events:
        if ev.event_type not in {
            EventType.TOOL_CALL_COMPLETED,
            EventType.TOOL_CALL_FAILED,
        }:
            continue
        tool = ev.payload.get("tool_name")
        if not isinstance(tool, str) or not tool:
            continue
        agent = ev.agent_name if group_by == "agent_tool" else None
        key = f"{ev.agent_name}::{tool}" if group_by == "agent_tool" else tool
        bucket = buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tool_name=tool, agent_name=agent)
            buckets[key] = bucket
        bucket.calls += 1
        if ev.event_type is EventType.TOOL_CALL_FAILED:
            bucket.failures += 1
        duration = ev.payload.get("duration_ms")
        if isinstance(duration, int):
            bucket.durations.append(duration)
        elif isinstance(duration, float):
            bucket.durations.append(int(duration))

    rows: list[ToolStats] = []
    for key, b in buckets.items():
        p50, p95, p99, avg = _summarise(b.durations)
        rows.append(
            ToolStats(
                key=key,
                tool_name=b.tool_name,
                agent_name=b.agent_name,
                calls=b.calls,
                failures=b.failures,
                p50_ms=p50,
                p95_ms=p95,
                p99_ms=p99,
                avg_ms=avg,
            )
        )
    rows.sort(key=lambda r: r.calls, reverse=True)
    return ToolsReport(group_by=group_by, rows=rows)


def parse_tools_group_by(value: str) -> ToolsGroupBy | None:
    if value == "tool":
        return "tool"
    if value == "agent_tool":
        return "agent_tool"
    return None


def _summarise(durations: list[int]) -> tuple[int, int, int, int]:
    if not durations:
        return 0, 0, 0, 0
    sorted_d = sorted(durations)
    p50 = _percentile(sorted_d, 0.50)
    p95 = _percentile(sorted_d, 0.95)
    p99 = _percentile(sorted_d, 0.99)
    avg = sum(sorted_d) // len(sorted_d)
    return p50, p95, p99, avg


def _percentile(sorted_values: list[int], q: float) -> int:
    """Nearest-rank percentile — sufficient for dashboard rollups."""
    if not sorted_values:
        return 0
    idx = max(0, min(len(sorted_values) - 1, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[idx]


__all__ = [
    "ToolStats",
    "ToolsGroupBy",
    "ToolsReport",
    "compute_tools",
    "parse_tools_group_by",
]
