"""Token-usage rollups computed from persisted :class:`RuntimeEvent` rows.

Pure-Python aggregation over an :class:`EventStore.query` result. Lives
outside the Protocol so every concrete store gets ``/usage`` for free —
the cost of one extra ``query()`` call vs a custom SQL projection. If
the dashboard ever needs sub-second rollups over millions of events,
push the aggregation down into the store; until then this is enough.

Source of truth: ``AGENT_COMPLETED.payload["tokens_used"]`` (the same
field :class:`CostTrackingMiddleware` populates). Other event types are
ignored — spawn / tool-call events don't carry token deltas in the
current contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterable

    from murmur.events.types import RuntimeEvent

GroupBy = Literal["agent", "trace", "none"]


class UsageTotals(BaseModel):
    model_config = ConfigDict(frozen=True)

    tokens_used: int
    events: int


class UsageGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    tokens_used: int
    events: int


class UsageReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    group_by: GroupBy
    totals: UsageTotals
    groups: list[UsageGroup]


class _Bucket:
    __slots__ = ("events", "tokens_used")

    def __init__(self) -> None:
        self.tokens_used: int = 0
        self.events: int = 0


def compute_usage(
    events: Iterable[RuntimeEvent],
    *,
    group_by: GroupBy = "agent",
) -> UsageReport:
    """Aggregate tokens_used across :class:`AGENT_COMPLETED` events.

    Returns overall totals and a per-group breakdown sorted by
    descending ``tokens_used``. ``group_by="none"`` returns just the
    totals (empty ``groups`` list).
    """
    from murmur.events.types import EventType

    totals = _Bucket()
    groups: dict[str, _Bucket] = {}

    for ev in events:
        if ev.event_type is not EventType.AGENT_COMPLETED:
            continue
        tokens = _coerce_int(ev.payload.get("tokens_used"))
        totals.tokens_used += tokens
        totals.events += 1
        if group_by == "none":
            continue
        key = ev.agent_name if group_by == "agent" else ev.trace_id
        bucket = groups.setdefault(key, _Bucket())
        bucket.tokens_used += tokens
        bucket.events += 1

    rows: list[UsageGroup] = [
        UsageGroup(key=k, tokens_used=b.tokens_used, events=b.events)
        for k, b in groups.items()
    ]
    rows.sort(key=lambda r: r.tokens_used, reverse=True)
    return UsageReport(
        group_by=group_by,
        totals=UsageTotals(tokens_used=totals.tokens_used, events=totals.events),
        groups=rows,
    )


def parse_group_by(value: str) -> GroupBy | None:
    """Narrow a query-string ``group_by`` value to the typed enum.

    Returns ``None`` for unknown inputs so the caller can decide on the
    error response shape (HTTP 400 vs CLI exit code) rather than this
    helper picking one.
    """
    if value == "agent":
        return "agent"
    if value == "trace":
        return "trace"
    if value == "none":
        return "none"
    return None


def _coerce_int(v: object) -> int:
    """Be tolerant of payloads that crossed a JSON boundary as ``float``."""
    if isinstance(v, bool):  # bool is a subclass of int — guard it.
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    return 0


__all__ = [
    "GroupBy",
    "UsageGroup",
    "UsageReport",
    "UsageTotals",
    "compute_usage",
    "parse_group_by",
]
