"""RocksDB-backed :class:`murmur.runs.RunStore` (single-instance, embedded).

Built on `rocksdict <https://github.com/Congyuwang/RocksDict>`_, a maintained
Python binding to facebook/rocksdb (the deprecated ``python-rocksdb``
package is not used). Single-process — no shared file locking — so this
store is fast and self-contained but not multi-instance. For
multi-instance deployments use :class:`murmur.runs.RedisRunStore`.

Layout
------

Two key prefixes share one column family — natural lex order of
zero-padded sequence numbers gives in-order range scans without any
secondary index:

* ``r/<run_id>``        → JSON-encoded run record (state + progress + target +
  optional result blob).
* ``e/<run_id>/<seq>``  → JSON-encoded :class:`murmur.runs.RunEvent`. ``seq``
  is a 20-digit zero-padded integer so iteration over the prefix returns
  events in append order.

Async wrapping
--------------

rocksdict is synchronous. Operations are dispatched via
:func:`asyncio.to_thread` so they don't block the event loop. The
default thread-pool executor is shared with the rest of the runtime;
heavy concurrency loads should bump
``loop.set_default_executor(ThreadPoolExecutor(max_workers=N))``.

Satisfies :class:`murmur.runs.RunStore` structurally.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rocksdict import Rdict, WriteBatch

from murmur.core.errors import RegistryError
from murmur.runs import (
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
    RunStatus,
)

if TYPE_CHECKING:
    from pydantic import BaseModel

    from murmur.types import AgentResult, GroupResult


_TERMINAL_STATES: frozenset[RunState] = frozenset(
    {RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}
)
_SEQ_WIDTH = 20  # enough for 2**64 events; lex order matches numeric order


def _run_key(run_id: str) -> bytes:
    return f"r/{run_id}".encode()


def _event_prefix(run_id: str) -> bytes:
    return f"e/{run_id}/".encode()


def _event_key(run_id: str, seq: int) -> bytes:
    return f"e/{run_id}/{seq:0{_SEQ_WIDTH}d}".encode()


class RocksDBRunStore:
    """``rocksdict``-backed :class:`murmur.runs.RunStore`.

    >>> store = RocksDBRunStore("./runs.db")
    >>> await store.create("abc", target="researcher")
    """

    def __init__(self, path: str | Path) -> None:
        self._path: str = str(path)
        self._db: Rdict | None = None
        self._init_lock: asyncio.Lock = asyncio.Lock()
        self._listeners: dict[str, list[asyncio.Event]] = {}
        self._write_locks: dict[str, asyncio.Lock] = {}
        self._seq_counters: dict[str, int] = {}

    @property
    def path(self) -> str:
        return self._path

    async def close(self) -> None:
        """Flush + close the underlying RocksDB. Idempotent."""
        if self._db is None:
            return
        db = self._db
        self._db = None
        await asyncio.to_thread(db.close)

    # ---- internal helpers --------------------------------------------------

    async def _ensure_db(self) -> Rdict:
        if self._db is not None:
            return self._db
        async with self._init_lock:
            if self._db is not None:
                return self._db
            db = await asyncio.to_thread(Rdict, self._path)
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

    # ---- sync helpers (run inside ``asyncio.to_thread``) -------------------

    def _read_record_sync(self, db: Rdict, run_id: str) -> dict[str, Any] | None:
        raw = db.get(_run_key(run_id))
        if raw is None:
            return None
        return json.loads(raw)

    def _write_record_sync(
        self, db: Rdict, run_id: str, record: dict[str, Any]
    ) -> None:
        db[_run_key(run_id)] = json.dumps(record).encode()

    def _create_sync(self, db: Rdict, run_id: str, target: str) -> bool:
        if db.get(_run_key(run_id)) is not None:
            return False
        record: dict[str, Any] = {
            "target": target,
            "state": RunState.PENDING.value,
            "progress": None,
            "result": None,
        }
        db[_run_key(run_id)] = json.dumps(record).encode()
        return True

    def _scan_max_seq_sync(self, db: Rdict, run_id: str) -> int:
        prefix = _event_prefix(run_id)
        it = db.iter()
        it.seek_for_prev(prefix + b"\xff" * _SEQ_WIDTH)
        if not it.valid():
            return -1
        key = it.key()
        if not key.startswith(prefix):
            return -1
        return int(key[len(prefix) :])

    def _read_events_after_sync(
        self, db: Rdict, run_id: str, after_seq: int
    ) -> list[tuple[RunEvent, int]]:
        prefix = _event_prefix(run_id)
        start = _event_key(run_id, after_seq + 1) if after_seq >= 0 else prefix
        it = db.iter()
        it.seek(start)
        out: list[tuple[RunEvent, int]] = []
        while it.valid():
            key = it.key()
            if not key.startswith(prefix):
                break
            seq = int(key[len(prefix) :])
            payload = json.loads(it.value())
            ev = RunEvent(
                type=RunEventType(payload["type"]),
                run_id=run_id,
                agent=payload.get("agent"),
                task_id=payload.get("task_id"),
                error=payload.get("error"),
                timestamp=datetime.fromisoformat(payload["timestamp"]).astimezone(UTC),
            )
            out.append((ev, seq))
            it.next()
        return out

    def _append_event_sync(
        self, db: Rdict, run_id: str, seq: int, event: RunEvent
    ) -> None:
        payload = {
            "type": event.type.value,
            "agent": event.agent,
            "task_id": event.task_id,
            "error": event.error,
            "timestamp": event.timestamp.isoformat(),
        }
        wb = WriteBatch()
        wb[_event_key(run_id, seq)] = json.dumps(payload).encode()
        db.write(wb)

    # ---- RunStore Protocol surface ----------------------------------------

    async def create(self, run_id: str, target: str) -> None:
        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            ok = await asyncio.to_thread(self._create_sync, db, run_id, target)
        if not ok:
            raise RegistryError(f"run_id {run_id!r} already exists")

    async def get_status(self, run_id: str) -> RunStatus:
        db = await self._ensure_db()
        record = await asyncio.to_thread(self._read_record_sync, db, run_id)
        if record is None:
            raise RegistryError(f"run_id {run_id!r} not found")
        progress_dict = record.get("progress")
        progress = RunProgress.model_validate(progress_dict) if progress_dict else None
        return RunStatus(
            run_id=run_id,
            state=RunState(record["state"]),
            progress=progress,
        )

    async def get_result(
        self, run_id: str
    ) -> AgentResult[BaseModel] | GroupResult | None:
        from murmur.runs._serde import decode_result

        db = await self._ensure_db()
        record = await asyncio.to_thread(self._read_record_sync, db, run_id)
        if record is None:
            raise RegistryError(f"run_id {run_id!r} not found")
        blob: str | None = record.get("result")
        if blob is None:
            return None
        return decode_result(blob)

    async def update_progress(self, run_id: str, progress: RunProgress) -> None:
        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            record = await asyncio.to_thread(self._read_record_sync, db, run_id)
            if record is None:
                raise RegistryError(f"run_id {run_id!r} not found")
            record["progress"] = progress.model_dump()
            await asyncio.to_thread(self._write_record_sync, db, run_id, record)

    async def set_state(self, run_id: str, state: RunState) -> None:
        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            record = await asyncio.to_thread(self._read_record_sync, db, run_id)
            if record is None:
                raise RegistryError(f"run_id {run_id!r} not found")
            record["state"] = state.value
            await asyncio.to_thread(self._write_record_sync, db, run_id, record)
        if state in _TERMINAL_STATES:
            self._wake_listeners(run_id)

    async def set_result(
        self, run_id: str, result: AgentResult[BaseModel] | GroupResult
    ) -> None:
        from murmur.runs._serde import encode_result

        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            record = await asyncio.to_thread(self._read_record_sync, db, run_id)
            if record is None:
                raise RegistryError(f"run_id {run_id!r} not found")
            record["result"] = encode_result(result)
            await asyncio.to_thread(self._write_record_sync, db, run_id, record)

    async def push_event(self, run_id: str, event: RunEvent) -> None:
        db = await self._ensure_db()
        async with self._write_lock_for(run_id):
            # Make sure the run exists, mirroring InMemoryRunStore.
            exists = await asyncio.to_thread(
                lambda: db.get(_run_key(run_id)) is not None
            )
            if not exists:
                raise RegistryError(f"run_id {run_id!r} not found")
            if run_id not in self._seq_counters:
                max_seq = await asyncio.to_thread(self._scan_max_seq_sync, db, run_id)
                self._seq_counters[run_id] = max_seq + 1
            seq = self._seq_counters[run_id]
            self._seq_counters[run_id] += 1
            await asyncio.to_thread(self._append_event_sync, db, run_id, seq, event)
        self._wake_listeners(run_id)

    async def stream(self, run_id: str) -> AsyncIterator[RunEvent]:  # type: ignore[override]
        await self.get_status(run_id)
        db = await self._ensure_db()

        wake = asyncio.Event()
        self._listeners.setdefault(run_id, []).append(wake)
        last_seen = -1
        try:
            while True:
                wake.clear()

                events = await asyncio.to_thread(
                    self._read_events_after_sync, db, run_id, last_seen
                )
                for ev, seq in events:
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


__all__ = ["RocksDBRunStore"]
