"""Composite ``/runtime/stats`` model the dashboard renders against.

One endpoint, one round-trip, every panel covered. Keeps the dashboard
free of N parallel fetches and any client-side fan-in. Computed on demand
by aggregating over the :class:`EventStore` plus reading runtime config.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from murmur.events.types import EventType, RuntimeEvent

if TYPE_CHECKING:
    from murmur.mcp_server import MCPEnrollment
    from murmur.runtime import AgentRuntime


class RuntimeStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    broker: str | None
    broker_status: str
    sse_status: str
    spawn_count: int
    max_total_spawns: int | None
    tokens_used: int
    token_budget: int | None
    workers_count: int
    mcp_count: int


class WorkerInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    broker: str
    subscribed: list[str]
    concurrency: int
    in_flight: int
    last_hb: str
    status: str


class MCPServerInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    mode: str
    tools_count: int
    tools: list[str]
    eager: bool
    status: str


class ErrorGroupInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    error_class: str
    count_24h: int
    last: str
    top_agent: str


class RejectionCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    budget: int
    cycle: int
    depth: int
    cap: int
    timeout: int


class StatsResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    runtime: RuntimeStats
    burn_rate: list[int]
    rejection_counts: RejectionCounts
    error_groups: list[ErrorGroupInfo]
    workers: list[WorkerInfo]
    mcp_servers: list[MCPServerInfo]


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def _tokens_from(event: RuntimeEvent) -> int:
    v = event.payload.get("tokens_used")
    return v if isinstance(v, int) else 0


def _payload_str(event: RuntimeEvent, key: str) -> str:
    v = event.payload.get(key)
    return v if isinstance(v, str) else ""


def _humanise(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    if total < 60:
        return f"{total}s ago"
    if total < 3600:
        m = total // 60
        s = total % 60
        return f"{m}m {s}s ago" if s else f"{m}m ago"
    h = total // 3600
    m = (total % 3600) // 60
    return f"{h}h {m}m ago" if m else f"{h}h ago"


def compute_burn_rate(
    events: Iterable[RuntimeEvent], *, now: datetime, minutes: int = 60
) -> list[int]:
    """Tokens/min for the last ``minutes`` minutes, oldest bucket first.

    Bucket boundary: integer minute relative to ``now``. Empty minutes
    show as ``0`` so the sparkline keeps a constant width.
    """
    buckets = [0] * minutes
    cutoff = now - timedelta(minutes=minutes)
    for ev in events:
        if ev.event_type is not EventType.AGENT_COMPLETED:
            continue
        if ev.timestamp < cutoff:
            continue
        delta_min = int((now - ev.timestamp).total_seconds() // 60)
        idx = minutes - 1 - delta_min
        if 0 <= idx < minutes:
            buckets[idx] += _tokens_from(ev)
    return buckets


def compute_rejection_counts(events: Iterable[RuntimeEvent]) -> RejectionCounts:
    counts = {"budget": 0, "cycle": 0, "depth": 0, "cap": 0, "timeout": 0}
    for ev in events:
        if ev.event_type is EventType.BUDGET_EXCEEDED:
            counts["budget"] += 1
            continue
        if ev.event_type is EventType.DEPTH_LIMIT_EXCEEDED:
            counts["depth"] += 1
            continue
        if ev.event_type is EventType.AGENT_FAILED:
            reason = _payload_str(ev, "reason")
            if reason in counts:
                counts[reason] += 1
    return RejectionCounts(**counts)


def compute_error_groups(
    events: Iterable[RuntimeEvent], *, now: datetime
) -> list[ErrorGroupInfo]:
    """Group AGENT_FAILED events by error class (parsed from ``payload.error``).

    Class extraction: leading ``ClassName:`` prefix when present, else
    the first whitespace-bounded token. Falls back to
    ``"UnknownError"``.
    """
    cutoff = now - timedelta(hours=24)
    by_class: dict[str, list[RuntimeEvent]] = {}
    for ev in events:
        if ev.event_type is not EventType.AGENT_FAILED:
            continue
        if ev.timestamp < cutoff:
            continue
        err = _payload_str(ev, "error") or "UnknownError"
        cls = err.split(":", 1)[0].strip() or "UnknownError"
        by_class.setdefault(cls, []).append(ev)
    rows: list[ErrorGroupInfo] = []
    for cls, evs in by_class.items():
        evs.sort(key=lambda e: e.timestamp, reverse=True)
        agents = Counter(e.agent_name for e in evs)
        top_agent = agents.most_common(1)[0][0] if agents else ""
        last_delta = now - evs[0].timestamp
        rows.append(
            ErrorGroupInfo(
                error_class=cls,
                count_24h=len(evs),
                last=_humanise(last_delta),
                top_agent=top_agent,
            )
        )
    rows.sort(key=lambda r: r.count_24h, reverse=True)
    return rows


def compute_tokens_total(events: Iterable[RuntimeEvent]) -> int:
    return sum(
        _tokens_from(e) for e in events if e.event_type is EventType.AGENT_COMPLETED
    )


def compute_observed_spawn_count(events: Iterable[RuntimeEvent]) -> int:
    """Count of agents observed to have started (AGENT_SPAWNED events).

    The runtime's internal :attr:`AgentRuntime.spawn_count` only ticks on
    dispatches that go through :meth:`runtime.run`; events that arrive via
    a broker bridge or are appended directly to the store are not counted.
    The dashboard shows the observed count instead so it reflects whatever
    the event log actually saw.
    """
    return sum(1 for e in events if e.event_type is EventType.AGENT_SPAWNED)


def mcp_servers_from_enrollments(
    enrollments: Iterable[MCPEnrollment],
) -> list[MCPServerInfo]:
    """Map server-registered MCP enrollments into the dashboard's row shape."""
    rows: list[MCPServerInfo] = []
    for e in enrollments:
        rows.append(
            MCPServerInfo(
                name=e.tool_name,
                mode="embedded",
                tools_count=1,
                tools=[e.tool_name],
                eager=True,
                status="connected",
            )
        )
    return rows


def build_stats(
    *,
    runtime: AgentRuntime,
    events: list[RuntimeEvent],
    sse_active: bool,
    mcp_enrollments: Iterable[MCPEnrollment],
    workers: Iterable[WorkerInfo] = (),
    now: datetime | None = None,
) -> StatsResponse:
    """Assemble :class:`StatsResponse` from a runtime + event snapshot."""
    now = now or datetime.now(tz=UTC)
    backend = runtime.backend
    backend_name = backend.__class__.__name__
    broker_url: str | None = getattr(backend, "broker_url", None)
    broker_started = bool(getattr(backend, "started", False))
    broker_status = (
        "connected"
        if backend_name == "JobBackend" and broker_started
        else "connected"
        if backend_name != "JobBackend"
        else "failed"
    )
    sse_status = "connected" if sse_active else "failed"
    token_budget = (
        runtime.options.token_budget.limit
        if runtime.options.token_budget is not None
        else None
    )
    mcp_servers = mcp_servers_from_enrollments(mcp_enrollments)
    workers_list = list(workers)
    # Take whichever count is larger: the runtime's internal counter (correct
    # when dispatches go through ``runtime.run``) or the observed count from
    # the store (correct for broker-relayed or directly-appended events).
    spawn_count = max(runtime.spawn_count, compute_observed_spawn_count(events))
    runtime_stats = RuntimeStats(
        id=runtime.runtime_id,
        broker=broker_url,
        broker_status=broker_status,
        sse_status=sse_status,
        spawn_count=spawn_count,
        max_total_spawns=runtime.options.max_total_spawns,
        tokens_used=compute_tokens_total(events),
        token_budget=token_budget,
        workers_count=len(workers_list),
        mcp_count=len(mcp_servers),
    )
    return StatsResponse(
        runtime=runtime_stats,
        burn_rate=compute_burn_rate(events, now=now),
        rejection_counts=compute_rejection_counts(events),
        error_groups=compute_error_groups(events, now=now),
        workers=workers_list,
        mcp_servers=mcp_servers,
    )


__all__ = [
    "ErrorGroupInfo",
    "MCPServerInfo",
    "RejectionCounts",
    "RuntimeStats",
    "StatsResponse",
    "WorkerInfo",
    "build_stats",
    "compute_burn_rate",
    "compute_error_groups",
    "compute_observed_spawn_count",
    "compute_rejection_counts",
    "compute_tokens_total",
    "mcp_servers_from_enrollments",
]
