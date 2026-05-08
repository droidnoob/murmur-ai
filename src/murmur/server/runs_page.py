"""Server-side run-summary assembly + the ``/runs`` paginated response.

Folds the events for one trace into a :class:`RunSummary` so the
dashboard's History tab can render a paginated table without pulling
the entire event log into the browser. Mirrors the in-browser
``eventsToRuns`` transform; kept out of :class:`EventStore` so any
concrete store (in-memory, SQLite, future Postgres) gets it for free.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

from murmur.events.types import EventType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.core.protocols.event_store import EventStore
    from murmur.events.types import RuntimeEvent


RunStatus = Literal["spawned", "running", "completed", "failed", "rejected"]


def parse_run_status(value: str) -> RunStatus | None:
    """Narrow a query-string ``status`` value to the typed enum.

    Returns ``None`` for unknown inputs so callers decide on the error
    response shape (HTTP 400 vs CLI exit code) rather than this helper
    picking one. Mirrors :func:`parse_group_by`.
    """
    if value == "spawned":
        return "spawned"
    if value == "running":
        return "running"
    if value == "completed":
        return "completed"
    if value == "failed":
        return "failed"
    if value == "rejected":
        return "rejected"
    return None


_STATUS_BY_TYPE: dict[EventType, RunStatus] = {
    EventType.AGENT_DISPATCHED: "spawned",
    EventType.AGENT_SPAWNED: "running",
    EventType.AGENT_COMPLETED: "completed",
    EventType.AGENT_FAILED: "failed",
    EventType.BUDGET_EXCEEDED: "rejected",
    EventType.DEPTH_LIMIT_EXCEEDED: "rejected",
}


class RunSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    trace_id: str
    agent_name: str
    status: RunStatus
    started_at: str | None
    duration_ms: int
    tokens_used: int
    backend: str
    trust_level: str
    depth: int
    parent_agent: str | None


class RunsPage(BaseModel):
    model_config = ConfigDict(frozen=True)

    rows: list[RunSummary]
    total: int
    limit: int
    offset: int


def _payload_str(event: RuntimeEvent, key: str, default: str = "") -> str:
    v = event.payload.get(key)
    return v if isinstance(v, str) else default


def _payload_int(event: RuntimeEvent, key: str, default: int = 0) -> int:
    v = event.payload.get(key)
    return v if isinstance(v, int) and not isinstance(v, bool) else default


def summarise_trace(events: Sequence[RuntimeEvent]) -> RunSummary | None:
    """Fold a single trace's events into one row. ``events`` may be empty
    (returns ``None``); the caller filters those out."""
    if not events:
        return None
    ordered = sorted(events, key=lambda e: e.timestamp)
    first = ordered[0]
    status: RunStatus = "spawned"
    duration_ms = 0
    tokens_used = 0
    backend = ""
    trust_level = ""
    started_at: str | None = None
    for ev in ordered:
        next_status = _STATUS_BY_TYPE.get(ev.event_type)
        if next_status is not None:
            status = next_status
        b = _payload_str(ev, "backend")
        if b:
            backend = b
        t = _payload_str(ev, "trust_level")
        if t:
            trust_level = t
        if ev.event_type in (EventType.AGENT_SPAWNED, EventType.AGENT_DISPATCHED):
            started_at = ev.timestamp.isoformat()
        if ev.event_type in (
            EventType.AGENT_COMPLETED,
            EventType.AGENT_FAILED,
        ):
            duration_ms = _payload_int(ev, "duration_ms", duration_ms)
            tokens_used = _payload_int(ev, "tokens_used", tokens_used)
    return RunSummary(
        trace_id=first.trace_id,
        agent_name=first.agent_name,
        status=status,
        started_at=started_at,
        duration_ms=duration_ms,
        tokens_used=tokens_used,
        backend=backend,
        trust_level=trust_level,
        depth=0,  # this endpoint serves top-level traces only
        parent_agent=None,
    )


async def assemble_runs_page(
    store: EventStore,
    *,
    limit: int = 50,
    offset: int = 0,
    status: RunStatus | None = None,
) -> RunsPage:
    """Pull one page of top-level traces and fold each into a summary.

    ``status`` filtering is applied *after* assembly — the store doesn't
    know per-trace status without folding, so we'd need a different
    index to push it down. For dashboard scale (hundreds to thousands
    of runs) this is fine; revisit if pages start dropping below ``limit``
    after filtering.
    """
    trace_ids = await store.list_traces(limit=limit, offset=offset)
    rows: list[RunSummary] = []
    for tid in trace_ids:
        events = await store.query(trace_id=tid, limit=1000)
        summary = summarise_trace(events)
        if summary is None:
            continue
        if status is not None and summary.status != status:
            continue
        rows.append(summary)
    total = await store.count_traces()
    return RunsPage(rows=rows, total=total, limit=limit, offset=offset)


__all__ = [
    "RunStatus",
    "RunSummary",
    "RunsPage",
    "assemble_runs_page",
    "parse_run_status",
    "summarise_trace",
]
