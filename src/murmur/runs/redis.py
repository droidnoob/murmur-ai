"""Redis-backed :class:`murmur.runs.RunStore` (multi-instance).

The only :class:`RunStore` shipped here that supports **multiple
AgentServer instances behind a load balancer** sharing one run
database. Run state lives in a Redis hash, the event log lives in a
Redis stream — ``XADD`` appends and ``XREAD`` polls wake listeners
across processes.

A small Lua script enforces monotonic state transitions: once a run is
``COMPLETED`` / ``FAILED`` / ``CANCELLED`` it cannot regress to an
earlier state. This protects against late writers in distributed
deployments.

Satisfies :class:`murmur.runs.RunStore` structurally.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from redis.asyncio import Redis, from_url

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
_TERMINAL_STATE_VALUES: frozenset[str] = frozenset(s.value for s in _TERMINAL_STATES)
_DEFAULT_TTL_SECONDS = 3600
# Stream poll cadence. Polling (rather than ``xread block=...``) keeps the
# semantics identical between real Redis and ``fakeredis``, and avoids
# pinning the event loop on a single blocking call.
_STREAM_POLL_SECONDS = 0.05

# Lua: only update state if the run exists and the current state is not
# already terminal. Returns 1 on update, 0 if the new state was rejected,
# -1 if the run does not exist.
_SET_STATE_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
local cur = redis.call('HGET', KEYS[1], 'state')
if cur and (cur == 'completed' or cur == 'failed' or cur == 'cancelled') then
  return 0
end
redis.call('HSET', KEYS[1], 'state', ARGV[1])
return 1
"""


_T = TypeVar("_T")


def _aw(value: Awaitable[_T] | _T) -> Awaitable[_T]:
    """Cast a ``redis-py`` command return to ``Awaitable[T]`` for the type checker.

    The redis-py command typestubs declare ``Awaitable[T] | T`` because
    one class backs both sync and async clients. ``redis.asyncio.Redis``
    always returns awaitables — narrow the union here so callers can
    ``await`` the result without a per-call ``# ty: ignore``.
    """
    return cast("Awaitable[_T]", value)


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return value


class RedisRunStore:
    """Redis-backed :class:`murmur.runs.RunStore`.

    Either pass ``url`` (e.g. ``redis://localhost:6379/0``) and a client
    is created via :func:`redis.asyncio.from_url`, or pass ``client=``
    with an existing ``redis.asyncio.Redis`` instance — useful for
    pre-configured connection pools and for the test seam (``FakeRedis``
    backed by a shared ``FakeServer`` exercises cross-instance
    behaviour without Docker).

    >>> store = RedisRunStore(url="redis://localhost:6379/0")
    >>> await store.create("abc", target="researcher")
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        client: Redis | None = None,
        key_prefix: str = "murmur:runs",
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    ) -> None:
        if (url is None) == (client is None):
            raise ValueError("pass exactly one of `url=` or `client=`")
        self._client: Redis = client if client is not None else from_url(url or "")
        self._owns_client: bool = client is None
        self._key_prefix: str = key_prefix.rstrip(":")
        self._ttl_seconds: int = ttl_seconds
        self._set_state_script = self._client.register_script(_SET_STATE_LUA)

    @property
    def client(self) -> Redis:
        return self._client

    async def close(self) -> None:
        """Disconnect — only closes the client we constructed ourselves."""
        if self._owns_client:
            await self._client.aclose()

    # ---- key helpers -------------------------------------------------------

    def _run_key(self, run_id: str) -> str:
        return f"{self._key_prefix}:run:{run_id}"

    def _stream_key(self, run_id: str) -> str:
        return f"{self._key_prefix}:events:{run_id}"

    async def _expire_terminal(self, run_id: str) -> None:
        if self._ttl_seconds <= 0:
            return
        await _aw(self._client.expire(self._run_key(run_id), self._ttl_seconds))
        await _aw(self._client.expire(self._stream_key(run_id), self._ttl_seconds))

    # ---- RunStore Protocol surface ----------------------------------------

    async def create(self, run_id: str, target: str) -> None:
        key = self._run_key(run_id)
        # ``redis-py``'s typestubs declare ``Awaitable[T] | T`` for every
        # command (the same class backs both sync and async clients), so
        # ty cannot prove the awaitable branch is selected here. The
        # ``redis.asyncio.Redis`` client always returns awaitables.
        created = await _aw(self._client.hsetnx(key, "target", target))
        if not created:
            raise RegistryError(f"run_id {run_id!r} already exists")
        await _aw(
            self._client.hset(
                key,
                mapping={
                    "state": RunState.PENDING.value,
                    "has_progress": "0",
                },
            )
        )

    async def get_status(self, run_id: str) -> RunStatus:
        key = self._run_key(run_id)
        record = await _aw(self._client.hgetall(key))
        if not record:
            raise RegistryError(f"run_id {run_id!r} not found")
        record_decoded = {_decode(k): _decode(v) for k, v in record.items()}
        progress = (
            RunProgress(
                total=int(record_decoded.get("total", 0)),
                completed=int(record_decoded.get("completed", 0)),
                failed=int(record_decoded.get("failed", 0)),
                running=int(record_decoded.get("running", 0)),
            )
            if record_decoded.get("has_progress") == "1"
            else None
        )
        return RunStatus(
            run_id=run_id,
            state=RunState(record_decoded["state"]),
            progress=progress,
        )

    async def get_result(
        self, run_id: str
    ) -> AgentResult[BaseModel] | GroupResult | None:
        key = self._run_key(run_id)
        if not await _aw(self._client.exists(key)):
            raise RegistryError(f"run_id {run_id!r} not found")
        blob_raw = await _aw(self._client.hget(key, "result"))
        if blob_raw is None:
            return None
        return decode_result(_decode(blob_raw))

    async def update_progress(self, run_id: str, progress: RunProgress) -> None:
        key = self._run_key(run_id)
        if not await _aw(self._client.exists(key)):
            raise RegistryError(f"run_id {run_id!r} not found")
        await _aw(
            self._client.hset(
                key,
                mapping={
                    "has_progress": "1",
                    "total": progress.total,
                    "completed": progress.completed,
                    "failed": progress.failed,
                    "running": progress.running,
                },
            )
        )

    async def set_state(self, run_id: str, state: RunState) -> None:
        key = self._run_key(run_id)
        # ``Script.__call__``'s typestubs return ``Awaitable[T] | T`` (the
        # union flips T to a coroutine in the async client) and ty can't
        # see through it. Force-cast to ``int`` here — fakeredis returns
        # the same int the real Lua interpreter would.
        raw = await _aw(self._set_state_script(keys=[key], args=[state.value]))
        result_int = int(cast("int", raw))
        if result_int == -1:
            raise RegistryError(f"run_id {run_id!r} not found")
        # ``result_int == 0`` → already terminal; treat as no-op so callers
        # don't get spurious failures during cancellation races.
        if state in _TERMINAL_STATES:
            await self._expire_terminal(run_id)

    async def set_result(
        self, run_id: str, result: AgentResult[BaseModel] | GroupResult
    ) -> None:
        key = self._run_key(run_id)
        if not await _aw(self._client.exists(key)):
            raise RegistryError(f"run_id {run_id!r} not found")
        await _aw(self._client.hset(key, "result", encode_result(result)))

    async def push_event(self, run_id: str, event: RunEvent) -> None:
        run_key = self._run_key(run_id)
        if not await _aw(self._client.exists(run_key)):
            raise RegistryError(f"run_id {run_id!r} not found")
        fields: dict[str | bytes, str | bytes] = {
            "type": event.type.value,
            "timestamp": event.timestamp.isoformat(),
        }
        if event.agent is not None:
            fields["agent"] = event.agent
        if event.task_id is not None:
            fields["task_id"] = event.task_id
        if event.error is not None:
            fields["error"] = event.error
        await _aw(self._client.xadd(self._stream_key(run_id), fields))  # ty: ignore[invalid-argument-type]  # redis-py's xadd typestubs require bytes-only mappings; str values are encoded automatically.

    async def stream(self, run_id: str) -> AsyncIterator[RunEvent]:  # type: ignore[override]
        # Eager 404
        await self.get_status(run_id)
        stream_key = self._stream_key(run_id)
        last_id: str | bytes = "0-0"
        while True:
            resp = await _aw(
                self._client.xread(streams={stream_key: last_id}, count=100)
            )
            if resp:
                for _, entries in resp:
                    for entry_id, payload in entries:
                        last_id = entry_id
                        decoded = {_decode(k): _decode(v) for k, v in payload.items()}
                        yield RunEvent(
                            type=RunEventType(decoded["type"]),
                            run_id=run_id,
                            agent=decoded.get("agent"),
                            task_id=decoded.get("task_id"),
                            error=decoded.get("error"),
                            timestamp=_parse_timestamp(decoded["timestamp"]),
                        )
            try:
                status = await self.get_status(run_id)
            except RegistryError:
                return
            if status.state in _TERMINAL_STATES:
                # One more drain in case events landed between xread + status.
                tail = await _aw(
                    self._client.xread(streams={stream_key: last_id}, count=100)
                )
                if tail:
                    for _, entries in tail:
                        for entry_id, payload in entries:
                            last_id = entry_id
                            decoded = {
                                _decode(k): _decode(v) for k, v in payload.items()
                            }
                            yield RunEvent(
                                type=RunEventType(decoded["type"]),
                                run_id=run_id,
                                agent=decoded.get("agent"),
                                task_id=decoded.get("task_id"),
                                error=decoded.get("error"),
                                timestamp=_parse_timestamp(decoded["timestamp"]),
                            )
                return
            await asyncio.sleep(_STREAM_POLL_SECONDS)


def _parse_timestamp(value: str) -> Any:
    from datetime import UTC, datetime

    return datetime.fromisoformat(value).astimezone(UTC)


__all__ = ["RedisRunStore"]
