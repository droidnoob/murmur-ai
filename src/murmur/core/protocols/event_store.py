"""``EventStore`` Protocol — durable persistence for :class:`RuntimeEvent`.

Concrete stores (``InMemoryEventStore``, ``SQLiteEventStore``) implement
this structurally. The dashboard reads from a store rather than from a
live SSE stream alone, so history queries, the History tab, the run
inspector tree, and token-usage rollups all share one source of truth.

A store is *additive*: events are appended, never updated. Retention is a
store-side concern — operators set TTL + row cap at construction; the
store prunes in the background.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from murmur.events.types import EventType, RuntimeEvent


@runtime_checkable
class EventStore(Protocol):
    """Append-only persistence for :class:`RuntimeEvent`.

    Implementations must be safe to call concurrently from multiple
    coroutines. ``append`` is fire-and-forget from the runtime's
    perspective and must not raise on transient backend failures (a
    failing store should never take an agent run down with it).
    """

    async def append(self, event: RuntimeEvent) -> None:
        """Persist one event. Idempotent on identical events is *not* required."""
        ...

    async def query(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        trace_id: str | None = None,
        event_types: Sequence[EventType] | None = None,
        limit: int = 200,
    ) -> list[RuntimeEvent]:
        """Filtered scan, newest-first by ``timestamp``.

        ``since`` / ``until`` bound a time window; ``trace_id`` restricts
        to one run (does not include cascading descendants — use
        :meth:`tree` for that). Returns at most ``limit`` events.
        """
        ...

    async def tree(self, root_trace_id: str) -> list[RuntimeEvent]:
        """All events for the given root and every cascading descendant.

        Walks the ``parent_trace_id`` graph. Returns events for ``root_trace_id``
        plus every event whose ``trace_id`` is reachable from the root via
        descendant edges. Sorted oldest-first so callers can render a
        timeline directly.
        """
        ...

    async def list_traces(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[str]:
        """Top-level trace ids (``parent_trace_id IS NULL``), newest first.

        ``offset`` skips that many of the newest matching trace ids
        before returning the next ``limit``. Drives the History tab's
        paginated run table.
        """
        ...

    async def count_traces(self, *, since: datetime | None = None) -> int:
        """Total number of top-level traces matching the time window.

        Pairs with :meth:`list_traces` to drive paginated UIs that need
        to render an "X of N" counter and disable the next-page button
        at the boundary.
        """
        ...


__all__ = ["EventStore"]
