"""In-process :class:`EventStore` — bounded ring + parent_trace_id index.

Default for ``murmur serve`` when no persistence path is configured.
Lost on restart; suitable for ephemeral runs, tests, and single-process
demos. Operators who want history across restarts should use
:class:`SQLiteEventStore`.

Retention is a ring buffer keyed on ``max_rows`` plus a TTL prune over
``timestamp``. Both run synchronously inside :meth:`append` since the
data structures are in-process; no background task is required.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.events.types import EventType, RuntimeEvent


class InMemoryEventStore:
    """Bounded in-memory store. Satisfies :class:`EventStore` structurally."""

    def __init__(
        self,
        *,
        max_rows: int = 100_000,
        ttl: timedelta | None = timedelta(days=7),
    ) -> None:
        self._max_rows = max_rows
        self._ttl = ttl
        self._events: deque[RuntimeEvent] = deque(maxlen=max_rows)
        # Index parent_trace_id → set of child trace_ids for fast tree walks.
        self._children: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()

    async def append(self, event: RuntimeEvent) -> None:
        async with self._lock:
            self._events.append(event)
            if event.parent_trace_id is not None:
                self._children.setdefault(event.parent_trace_id, set()).add(
                    event.trace_id
                )
            self._prune_ttl()

    async def query(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        trace_id: str | None = None,
        event_types: Sequence[EventType] | None = None,
        limit: int = 200,
    ) -> list[RuntimeEvent]:
        async with self._lock:
            event_type_set = set(event_types) if event_types is not None else None
            out: list[RuntimeEvent] = []
            # Iterate newest-first.
            for ev in reversed(self._events):
                if since is not None and ev.timestamp < since:
                    continue
                if until is not None and ev.timestamp > until:
                    continue
                if trace_id is not None and ev.trace_id != trace_id:
                    continue
                if event_type_set is not None and ev.event_type not in event_type_set:
                    continue
                out.append(ev)
                if len(out) >= limit:
                    break
            return out

    async def tree(self, root_trace_id: str) -> list[RuntimeEvent]:
        async with self._lock:
            # BFS over the parent_trace_id graph to collect descendant trace_ids.
            reachable: set[str] = {root_trace_id}
            frontier: list[str] = [root_trace_id]
            while frontier:
                next_frontier: list[str] = []
                for tid in frontier:
                    for child in self._children.get(tid, ()):
                        if child not in reachable:
                            reachable.add(child)
                            next_frontier.append(child)
                frontier = next_frontier
            matched = [ev for ev in self._events if ev.trace_id in reachable]
            matched.sort(key=lambda e: e.timestamp)
            return matched

    async def list_traces(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[str]:
        async with self._lock:
            ordered = self._ordered_top_level(since=since)
            return [tid for tid, _ in ordered[offset : offset + limit]]

    async def count_traces(self, *, since: datetime | None = None) -> int:
        async with self._lock:
            return len(self._ordered_top_level(since=since))

    def _ordered_top_level(
        self, *, since: datetime | None
    ) -> list[tuple[str, datetime]]:
        seen: dict[str, datetime] = {}
        for ev in self._events:
            if ev.parent_trace_id is not None:
                continue
            if since is not None and ev.timestamp < since:
                continue
            existing = seen.get(ev.trace_id)
            if existing is None or ev.timestamp > existing:
                seen[ev.trace_id] = ev.timestamp
        return sorted(seen.items(), key=lambda kv: kv[1], reverse=True)

    def _prune_ttl(self) -> None:
        if self._ttl is None:
            return
        cutoff = datetime.now(tz=UTC) - self._ttl
        # ``deque`` doesn't support arbitrary removal cheaply, but events
        # arrive monotonically by wall-clock so old ones stack at the head.
        while self._events and self._events[0].timestamp < cutoff:
            self._events.popleft()


__all__ = ["InMemoryEventStore"]
