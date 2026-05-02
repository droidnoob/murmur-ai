"""Long-lived run handles for the submit / poll / stream pattern.

When an :class:`AgentServer` accepts a ``POST /submit`` it returns a
``run_id`` immediately and dispatches the actual work in a background task.
Clients then poll ``GET /runs/{run_id}/status``, fetch the final result via
``GET /runs/{run_id}/result``, or stream events from ``GET /runs/{run_id}/stream``.

Everything in this module is the *value* layer — frozen Pydantic models +
the :class:`RunStore` Protocol. The HTTP routes that drive these live in
``murmur.server``; the client-side wrapper that consumes them lives in
``murmur_client`` (separate package).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel, ConfigDict, Field

from murmur.core.errors import RegistryError

if TYPE_CHECKING:
    # Type-only imports of the lazy concretes so static checkers see them
    # statically — at runtime these resolve through ``__getattr__`` below
    # and don't pull in optional deps unless the user actually uses them.
    from murmur.runs.redis import RedisRunStore as RedisRunStore
    from murmur.runs.rocksdb import RocksDBRunStore as RocksDBRunStore
    from murmur.runs.sqlite import SQLiteRunStore as SQLiteRunStore
    from murmur.types import AgentResult


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------


class RunState(StrEnum):
    """Coarse-grained lifecycle state for a submitted run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunProgress(BaseModel):
    """Per-step counters reported during a run."""

    model_config = ConfigDict(frozen=True)

    total: int = 0
    """Total number of agent invocations expected for the run. ``0`` until
    the runner knows the count (e.g. after fan-out resolution)."""

    completed: int = 0
    """Successfully completed invocations so far."""

    failed: int = 0
    """Failed invocations so far. A run can finish in
    :attr:`RunState.COMPLETED` with non-zero ``failed`` if the topology
    tolerates partial failure."""

    running: int = 0
    """Currently in-flight invocations."""


class RunStatus(BaseModel):
    """A run's current state + progress snapshot."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    """The id returned by ``POST /submit``. Echoed back here so this status
    object stands alone."""

    state: RunState
    """Coarse-grained lifecycle state. See :class:`RunState`."""

    progress: RunProgress | None = None
    """Counter snapshot. ``None`` until the runner reports the first update."""


class RunEventType(StrEnum):
    """Discriminator for :class:`RunEvent` instances on the SSE stream."""

    AGENT_STARTED = "agent_started"
    """An agent invocation began. Payload: ``agent``, ``task_id``."""

    AGENT_COMPLETED = "agent_completed"
    """An agent invocation finished successfully. Payload: ``agent``, ``task_id``."""

    AGENT_FAILED = "agent_failed"
    """An agent invocation raised. Payload: ``agent``, ``task_id``, ``error``."""

    GROUP_COMPLETED = "group_completed"
    """The whole :class:`AgentGroup` run finished — terminal."""

    RUN_CANCELLED = "run_cancelled"
    """The run was cancelled by a client request — terminal."""


class RunEvent(BaseModel):
    """Stream event published over SSE for ``GET /runs/{run_id}/stream``."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: RunEventType
    """Discriminator. See :class:`RunEventType` for per-type payload contracts."""

    run_id: str
    """Run id this event belongs to. Always present so events stand alone
    when serialised."""

    agent: str | None = None
    """The :class:`Agent` name involved, when relevant.
    ``None`` for run-level events (``GROUP_COMPLETED``, ``RUN_CANCELLED``)."""

    task_id: str | None = None
    """The :class:`TaskSpec` id, when the event is about one task in
    particular. ``None`` for run-level events."""

    error: str | None = None
    """Stringified error message — populated only on :data:`AGENT_FAILED`."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    """UTC time of emission."""


# ---------------------------------------------------------------------------
# RunStore Protocol
# ---------------------------------------------------------------------------


class RunStore(Protocol):
    """Pluggable persistence for in-flight runs.

    The MVP ships an in-memory implementation; Redis / DB land later.
    Implementations must be safe to call concurrently from many tasks.
    """

    async def create(self, run_id: str, target: str) -> None:
        """Register a new run. ``target`` is the agent or group name."""
        ...

    async def get_status(self, run_id: str) -> RunStatus: ...

    async def get_result(self, run_id: str) -> AgentResult[BaseModel] | None: ...

    async def update_progress(self, run_id: str, progress: RunProgress) -> None: ...

    async def set_state(self, run_id: str, state: RunState) -> None: ...

    async def set_result(self, run_id: str, result: AgentResult[BaseModel]) -> None: ...

    async def push_event(self, run_id: str, event: RunEvent) -> None: ...

    def stream(self, run_id: str) -> AsyncIterator[RunEvent]:
        """Subscribe to ``run_id``'s event stream (SSE-friendly)."""
        ...


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------


class _RunRecord:
    __slots__ = (
        "_listeners",
        "events",
        "lock",
        "progress",
        "result",
        "state",
        "target",
    )

    def __init__(self, target: str) -> None:
        self.target = target
        self.state: RunState = RunState.PENDING
        self.progress: RunProgress | None = None
        self.result: AgentResult[BaseModel] | None = None
        self.events: list[RunEvent] = []
        self.lock = asyncio.Lock()
        self._listeners: list[asyncio.Queue[RunEvent | None]] = []

    def add_listener(self) -> asyncio.Queue[RunEvent | None]:
        q: asyncio.Queue[RunEvent | None] = asyncio.Queue()
        self._listeners.append(q)
        return q

    def drop_listener(self, q: asyncio.Queue[RunEvent | None]) -> None:
        if q in self._listeners:
            self._listeners.remove(q)

    def fanout(self, event: RunEvent) -> None:
        for q in self._listeners:
            q.put_nowait(event)

    def close_listeners(self) -> None:
        for q in self._listeners:
            q.put_nowait(None)


class InMemoryRunStore:
    """Per-process :class:`RunStore`. Suitable for single-host deployments."""

    def __init__(self) -> None:
        self._records: dict[str, _RunRecord] = {}

    @staticmethod
    def new_run_id() -> str:
        return str(uuid.uuid4())

    async def create(self, run_id: str, target: str) -> None:
        if run_id in self._records:
            raise RegistryError(f"run_id {run_id!r} already exists")
        self._records[run_id] = _RunRecord(target=target)

    async def get_status(self, run_id: str) -> RunStatus:
        rec = self._require(run_id)
        return RunStatus(run_id=run_id, state=rec.state, progress=rec.progress)

    async def get_result(self, run_id: str) -> AgentResult[BaseModel] | None:
        return self._require(run_id).result

    async def update_progress(self, run_id: str, progress: RunProgress) -> None:
        self._require(run_id).progress = progress

    async def set_state(self, run_id: str, state: RunState) -> None:
        rec = self._require(run_id)
        rec.state = state
        if state in {RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}:
            rec.close_listeners()

    async def set_result(self, run_id: str, result: AgentResult[BaseModel]) -> None:
        rec = self._require(run_id)
        rec.result = result

    async def push_event(self, run_id: str, event: RunEvent) -> None:
        rec = self._require(run_id)
        rec.events.append(event)
        rec.fanout(event)

    async def stream(self, run_id: str) -> AsyncIterator[RunEvent]:  # type: ignore[override]
        rec = self._require(run_id)
        # Replay buffered events first so late subscribers see history.
        for ev in list(rec.events):
            yield ev
        if rec.state in {RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}:
            return
        q = rec.add_listener()
        try:
            while True:
                ev = await q.get()
                if ev is None:
                    return
                yield ev
        finally:
            rec.drop_listener(q)

    def _require(self, run_id: str) -> _RunRecord:
        if run_id not in self._records:
            raise RegistryError(f"run_id {run_id!r} not found")
        return self._records[run_id]


# ---------------------------------------------------------------------------
# Lazy concretes — keep optional deps (aiosqlite / rocksdict / redis) out of
# the import path until they're actually requested. ``import murmur`` keeps
# working without any of these extras installed.
# ---------------------------------------------------------------------------


_LAZY_CONCRETES = {
    "SQLiteRunStore": ("murmur.runs.sqlite", "SQLiteRunStore"),
    "RocksDBRunStore": ("murmur.runs.rocksdb", "RocksDBRunStore"),
    "RedisRunStore": ("murmur.runs.redis", "RedisRunStore"),
}


def __getattr__(name: str) -> object:
    target = _LAZY_CONCRETES.get(name)
    if target is None:
        raise AttributeError(f"module 'murmur.runs' has no attribute {name!r}")
    module_name, attr = target
    import importlib

    module = importlib.import_module(module_name)
    return getattr(module, attr)


__all__ = [
    "InMemoryRunStore",
    "RedisRunStore",
    "RocksDBRunStore",
    "RunEvent",
    "RunEventType",
    "RunProgress",
    "RunState",
    "RunStatus",
    "RunStore",
    "SQLiteRunStore",
]
