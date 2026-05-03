"""SQLite-backed :class:`murmur.runs.RunStore` (single-instance, file-backed).

Async-native via ``aiosqlite``. WAL mode for concurrent reads while one
writer is active. Listener wake-up is in-process via per-run
:class:`asyncio.Event` instances — there's no LISTEN/NOTIFY in SQLite,
so this store is intentionally single-instance. For multi-instance
deployments, use :class:`murmur.runs.RedisRunStore`.

Satisfies :class:`murmur.runs.RunStore` structurally.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite

from murmur.core.errors import RegistryError
from murmur.runs import (
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
    RunStatus,
)
from murmur.runs._serde import decode_result, encode_result

if TYPE_CHECKING:
    from pydantic import BaseModel

    from murmur.types import AgentResult, GroupResult


_TERMINAL_STATES: frozenset[RunState] = frozenset(
    {RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}
)

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        state TEXT NOT NULL,
        has_progress INTEGER NOT NULL DEFAULT 0,
        total INTEGER NOT NULL DEFAULT 0,
        completed INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0,
        running INTEGER NOT NULL DEFAULT 0,
        result_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        run_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        type TEXT NOT NULL,
        agent TEXT,
        task_id TEXT,
        error TEXT,
        timestamp TEXT NOT NULL,
        PRIMARY KEY (run_id, seq)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq)",
)


class SQLiteRunStore:
    """``aiosqlite``-backed :class:`murmur.runs.RunStore`.

    >>> store = SQLiteRunStore("runs.db")
    >>> await store.create("abc", target="researcher")
    """

    def __init__(self, path: str | Path) -> None:
        self._path: str = str(path)
        self._db: aiosqlite.Connection | None = None
        self._init_lock: asyncio.Lock = asyncio.Lock()
        self._listeners: dict[str, list[asyncio.Event]] = {}
        self._write_locks: dict[str, asyncio.Lock] = {}

    @property
    def path(self) -> str:
        return self._path

    async def close(self) -> None:
        """Close the underlying connection. Idempotent."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ---- internal helpers --------------------------------------------------

    async def _ensure_db(self) -> aiosqlite.Connection:
        if self._db is not None:
            return self._db
        async with self._init_lock:
            if self._db is not None:
                return self._db
            db = await aiosqlite.connect(self._path)
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            for stmt in _SCHEMA:
                await db.execute(stmt)
            await db.commit()
            self._db = db
            return db

    def _write_lock_for(self, run_id: str) -> asyncio.Lock:
        lock = self._write_locks.get(run_id)
        if lock is None:
            lock = asyncio.Lock()
            self._write_locks[run_id] = lock
        return lock

    def _wake_listeners(self, run_id: str) -> None:
        for ev in self._listeners.get(run_id, []):
            ev.set()

    async def _fetch_events_after(
        self, run_id: str, after_seq: int
    ) -> list[tuple[RunEvent, int]]:
        db = await self._ensure_db()
        async with db.execute(
            "SELECT seq, type, agent, task_id, error, timestamp FROM events"
            " WHERE run_id=? AND seq > ? ORDER BY seq",
            (run_id, after_seq),
        ) as cursor:
            rows = await cursor.fetchall()
        out: list[tuple[RunEvent, int]] = []
        for seq, type_value, agent, task_id, error, timestamp in rows:
            ev = RunEvent(
                type=RunEventType(type_value),
                run_id=run_id,
                agent=agent,
                task_id=task_id,
                error=error,
                timestamp=datetime.fromisoformat(timestamp).astimezone(UTC),
            )
            out.append((ev, seq))
        return out

    # ---- RunStore Protocol surface ----------------------------------------

    async def create(self, run_id: str, target: str) -> None:
        db = await self._ensure_db()
        try:
            await db.execute(
                "INSERT INTO runs (run_id, target, state) VALUES (?, ?, ?)",
                (run_id, target, RunState.PENDING.value),
            )
            await db.commit()
        except aiosqlite.IntegrityError as e:
            raise RegistryError(f"run_id {run_id!r} already exists") from e

    async def get_status(self, run_id: str) -> RunStatus:
        db = await self._ensure_db()
        async with db.execute(
            "SELECT state, has_progress, total, completed, failed, running"
            " FROM runs WHERE run_id=?",
            (run_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise RegistryError(f"run_id {run_id!r} not found")
        state_value, has_progress, total, completed, failed, running = row
        progress = (
            RunProgress(
                total=total,
                completed=completed,
                failed=failed,
                running=running,
            )
            if has_progress
            else None
        )
        return RunStatus(run_id=run_id, state=RunState(state_value), progress=progress)

    async def get_result(
        self, run_id: str
    ) -> AgentResult[BaseModel] | GroupResult | None:
        db = await self._ensure_db()
        async with db.execute(
            "SELECT result_json FROM runs WHERE run_id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise RegistryError(f"run_id {run_id!r} not found")
        (blob,) = row
        if blob is None:
            return None
        return decode_result(blob)

    async def update_progress(self, run_id: str, progress: RunProgress) -> None:
        db = await self._ensure_db()
        cursor = await db.execute(
            "UPDATE runs SET has_progress=1, total=?, completed=?, failed=?,"
            " running=? WHERE run_id=?",
            (
                progress.total,
                progress.completed,
                progress.failed,
                progress.running,
                run_id,
            ),
        )
        if cursor.rowcount == 0:
            raise RegistryError(f"run_id {run_id!r} not found")
        await db.commit()

    async def set_state(self, run_id: str, state: RunState) -> None:
        db = await self._ensure_db()
        cursor = await db.execute(
            "UPDATE runs SET state=? WHERE run_id=?", (state.value, run_id)
        )
        if cursor.rowcount == 0:
            raise RegistryError(f"run_id {run_id!r} not found")
        await db.commit()
        if state in _TERMINAL_STATES:
            self._wake_listeners(run_id)

    async def set_result(
        self, run_id: str, result: AgentResult[BaseModel] | GroupResult
    ) -> None:
        db = await self._ensure_db()
        cursor = await db.execute(
            "UPDATE runs SET result_json=? WHERE run_id=?",
            (encode_result(result), run_id),
        )
        if cursor.rowcount == 0:
            raise RegistryError(f"run_id {run_id!r} not found")
        await db.commit()

    async def push_event(self, run_id: str, event: RunEvent) -> None:
        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            async with db.execute(
                "SELECT COALESCE(MAX(seq), -1) + 1 FROM events WHERE run_id=?",
                (run_id,),
            ) as cursor:
                row = await cursor.fetchone()
            next_seq = row[0] if row is not None else 0
            await db.execute(
                "INSERT INTO events (run_id, seq, type, agent, task_id, error,"
                " timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    next_seq,
                    event.type.value,
                    event.agent,
                    event.task_id,
                    event.error,
                    event.timestamp.isoformat(),
                ),
            )
            await db.commit()
        self._wake_listeners(run_id)

    async def stream(self, run_id: str) -> AsyncIterator[RunEvent]:  # type: ignore[override]
        # Eager 404 — match InMemoryRunStore's behaviour.
        await self.get_status(run_id)

        wake = asyncio.Event()
        self._listeners.setdefault(run_id, []).append(wake)
        last_seen = -1
        try:
            while True:
                # Clear before reading: any push_event that fires between
                # read+wait is guaranteed to flip the event, so wait() won't
                # miss it. (If it fires before read, the read picks it up.)
                wake.clear()

                for ev, seq in await self._fetch_events_after(run_id, last_seen):
                    last_seen = seq
                    yield ev

                status = await self.get_status(run_id)
                if status.state in _TERMINAL_STATES:
                    return

                await wake.wait()
        finally:
            listeners = self._listeners.get(run_id, [])
            if wake in listeners:
                listeners.remove(wake)
            if not listeners:
                self._listeners.pop(run_id, None)


__all__ = ["SQLiteRunStore"]
