"""SQLite-backed :class:`EventStore`.

Stdlib ``sqlite3`` driven through :func:`asyncio.to_thread` so the
runtime stays pure-async without adding ``aiosqlite``. Pass
``path=":memory:"`` for an ephemeral store (used by tests).

Schema is intentionally narrow: one ``events`` table indexed on
``trace_id``, ``parent_trace_id``, and ``ts``. The tree walk uses a
recursive CTE so ``GET /runs/{trace_id}`` is a single round-trip.

Retention runs as a background task on a configurable interval. TTL
deletes by ``ts`` cutoff; the row cap deletes oldest rows beyond the
limit. Both run inside one transaction.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from murmur.events.types import EventType, RuntimeEvent

if TYPE_CHECKING:
    from collections.abc import Sequence

log: structlog.stdlib.BoundLogger = structlog.get_logger()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    agent_name      TEXT NOT NULL,
    task_id         TEXT,
    trace_id        TEXT NOT NULL,
    parent_trace_id TEXT,
    payload         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_trace      ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_parent     ON events(parent_trace_id);
CREATE INDEX IF NOT EXISTS idx_events_ts         ON events(ts);
"""


class SQLiteEventStore:
    """Durable :class:`EventStore` backed by SQLite. Structurally a Protocol fit."""

    def __init__(
        self,
        *,
        path: str | Path = ":memory:",
        max_rows: int = 1_000_000,
        ttl: timedelta | None = timedelta(days=7),
        prune_interval: timedelta = timedelta(minutes=5),
    ) -> None:
        self._path = str(path)
        self._max_rows = max_rows
        self._ttl = ttl
        self._prune_interval = prune_interval
        # ``check_same_thread=False`` because we drive the connection from
        # asyncio worker threads via ``to_thread``; the lock below
        # serialises access so we don't race the cursor.
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.executescript("PRAGMA journal_mode=WAL;")
        self._conn.executescript(_SCHEMA)
        self._lock = asyncio.Lock()
        self._prune_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ public

    async def append(self, event: RuntimeEvent) -> None:
        async with self._lock:
            await asyncio.to_thread(self._insert, event)

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
            return await asyncio.to_thread(
                self._query_sync, since, until, trace_id, event_types, limit
            )

    async def tree(self, root_trace_id: str) -> list[RuntimeEvent]:
        async with self._lock:
            return await asyncio.to_thread(self._tree_sync, root_trace_id)

    async def list_traces(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[str]:
        async with self._lock:
            return await asyncio.to_thread(self._list_traces_sync, since, limit, offset)

    async def count_traces(self, *, since: datetime | None = None) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._count_traces_sync, since)

    async def start_pruning(self) -> None:
        """Begin the background prune task. Idempotent."""
        if self._prune_task is not None and not self._prune_task.done():
            return
        self._prune_task = asyncio.create_task(self._prune_loop())

    async def stop_pruning(self) -> None:
        if self._prune_task is None:
            return
        self._prune_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._prune_task
        self._prune_task = None

    async def close(self) -> None:
        await self.stop_pruning()
        async with self._lock:
            await asyncio.to_thread(self._conn.close)

    # ------------------------------------------------------------------ private

    def _insert(self, event: RuntimeEvent) -> None:
        self._conn.execute(
            "INSERT INTO events "
            "(ts, event_type, agent_name, task_id, trace_id, parent_trace_id, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event.timestamp.isoformat(),
                event.event_type.value,
                event.agent_name,
                event.task_id,
                event.trace_id,
                event.parent_trace_id,
                json.dumps(dict(event.payload)),
            ),
        )
        self._conn.commit()

    def _query_sync(
        self,
        since: datetime | None,
        until: datetime | None,
        trace_id: str | None,
        event_types: Sequence[EventType] | None,
        limit: int,
    ) -> list[RuntimeEvent]:
        sql = "SELECT * FROM events WHERE 1=1"
        params: list[object] = []
        if since is not None:
            sql += " AND ts >= ?"
            params.append(since.isoformat())
        if until is not None:
            sql += " AND ts <= ?"
            params.append(until.isoformat())
        if trace_id is not None:
            sql += " AND trace_id = ?"
            params.append(trace_id)
        if event_types:
            placeholders = ",".join("?" * len(event_types))
            sql += f" AND event_type IN ({placeholders})"
            params.extend(et.value for et in event_types)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_event(r) for r in rows]

    def _tree_sync(self, root_trace_id: str) -> list[RuntimeEvent]:
        sql = """
            WITH RECURSIVE descendants(tid) AS (
                SELECT ?
                UNION
                SELECT e.trace_id
                FROM events e
                JOIN descendants d ON e.parent_trace_id = d.tid
            )
            SELECT * FROM events
            WHERE trace_id IN (SELECT tid FROM descendants)
            ORDER BY ts ASC
        """
        rows = self._conn.execute(sql, (root_trace_id,)).fetchall()
        return [_row_to_event(r) for r in rows]

    def _list_traces_sync(
        self, since: datetime | None, limit: int, offset: int
    ) -> list[str]:
        sql = (
            "SELECT trace_id, MAX(ts) AS last_ts "
            "FROM events WHERE parent_trace_id IS NULL"
        )
        params: list[object] = []
        if since is not None:
            sql += " AND ts >= ?"
            params.append(since.isoformat())
        sql += " GROUP BY trace_id ORDER BY last_ts DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = self._conn.execute(sql, params).fetchall()
        return [str(r[0]) for r in rows]

    def _count_traces_sync(self, since: datetime | None) -> int:
        sql = (
            "SELECT COUNT(DISTINCT trace_id) FROM events WHERE parent_trace_id IS NULL"
        )
        params: list[object] = []
        if since is not None:
            sql += " AND ts >= ?"
            params.append(since.isoformat())
        (count,) = self._conn.execute(sql, params).fetchone()
        return int(count)

    async def _prune_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._prune_interval.total_seconds())
                async with self._lock:
                    try:
                        await asyncio.to_thread(self._prune_sync)
                    except sqlite3.Error as exc:  # pragma: no cover — best effort
                        await log.awarning("event_store_prune_failed", error=str(exc))
        except asyncio.CancelledError:
            return

    def _prune_sync(self) -> None:
        if self._ttl is not None:
            cutoff = (datetime.now(tz=UTC) - self._ttl).isoformat()
            self._conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,))
        # Row cap: count then trim oldest beyond limit.
        (count,) = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()
        if count > self._max_rows:
            excess = count - self._max_rows
            self._conn.execute(
                "DELETE FROM events WHERE id IN "
                "(SELECT id FROM events ORDER BY ts ASC LIMIT ?)",
                (excess,),
            )
        self._conn.commit()


def _row_to_event(row: tuple[object, ...]) -> RuntimeEvent:
    """Materialise a typed event from one ``events`` row.

    Column order matches the ``SELECT *`` projection in :data:`_SCHEMA`:
    ``(id, ts, event_type, agent_name, task_id, trace_id,
    parent_trace_id, payload)``. The ``str(...)`` / ``json.loads(...)``
    calls double as runtime narrowing — RuntimeEvent's Pydantic model
    rejects bad types, so we don't need typing.cast on top.
    """
    (
        _id,
        ts,
        event_type,
        agent_name,
        task_id,
        trace_id,
        parent_trace_id,
        payload_json,
    ) = row
    return RuntimeEvent(
        timestamp=datetime.fromisoformat(str(ts)),
        event_type=EventType(str(event_type)),
        agent_name=str(agent_name),
        task_id=None if task_id is None else str(task_id),
        trace_id=str(trace_id),
        parent_trace_id=None if parent_trace_id is None else str(parent_trace_id),
        payload=json.loads(str(payload_json)),
    )


__all__ = ["SQLiteEventStore"]
