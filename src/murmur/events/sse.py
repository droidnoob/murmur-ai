"""SSEEventEmitter — fan-out runtime events to Server-Sent-Events subscribers.

Wraps an in-memory pub/sub of :class:`RuntimeEvent` for HTTP delivery.
Each call to :meth:`subscribe` allocates its own bounded
:class:`asyncio.Queue`; :meth:`emit` enqueues onto every subscriber's
queue without blocking. A heartbeat ping fires every
``heartbeat_interval`` seconds so idle SSE connections stay open through
intermediate proxies.

Combine with :class:`MultiEventEmitter` to keep ``LogEventEmitter``
running alongside an HTTP feed:

>>> emitter = MultiEventEmitter([LogEventEmitter(), SSEEventEmitter()])
>>> runtime = AgentRuntime(event_emitter=emitter)

Wire the subscribe generator into a FastAPI route via ``sse_starlette``:

>>> @app.get("/events/stream")
>>> async def stream() -> EventSourceResponse:
...     return EventSourceResponse(emitter.subscribe())

Slow consumers don't block the runtime: per-subscriber queues are
bounded (default 1024) and overflow drops the oldest event with a
``sse_subscriber_overflow`` log line. The runtime stays unblocked.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Final

import structlog

if TYPE_CHECKING:
    from murmur.events.types import RuntimeEvent


_log: structlog.stdlib.BoundLogger = structlog.get_logger()


class _Heartbeat:
    """Sentinel pushed into subscriber queues by the heartbeat task."""


_HEARTBEAT: Final = _Heartbeat()


class SSEEventEmitter:
    """:class:`EventEmitter` that fans events out to SSE subscribers."""

    def __init__(
        self,
        *,
        heartbeat_interval: float = 15.0,
        queue_max: int = 1024,
    ) -> None:
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be > 0")
        if queue_max < 1:
            raise ValueError("queue_max must be >= 1")
        self._heartbeat_interval = heartbeat_interval
        self._queue_max = queue_max
        self._subscribers: list[asyncio.Queue[RuntimeEvent | _Heartbeat]] = []
        self._lock = asyncio.Lock()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def emit(self, event: RuntimeEvent) -> None:
        """Enqueue ``event`` onto every subscriber's queue. Non-blocking.

        Slow consumers whose queue is full drop the *new* event (we'd
        rather lose telemetry than backpressure the runtime). The drop
        is logged once per occurrence so it's visible.
        """
        async with self._lock:
            subs = list(self._subscribers)
        for q in subs:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                await _log.awarning(
                    "sse_subscriber_overflow",
                    event_type=event.event_type.value,
                    queue_max=self._queue_max,
                )

    async def subscribe(self) -> AsyncGenerator[dict[str, str], None]:
        """Yield SSE-formatted dicts for an :class:`EventSourceResponse`.

        Each dict has ``event`` (the :class:`EventType` value) and
        ``data`` (JSON-serialised :class:`RuntimeEvent`). Heartbeats use
        ``event="ping"`` with empty data.

        Cleanly removes the subscriber's queue and cancels its heartbeat
        task on cancellation — closing the SSE connection client-side
        triggers exactly that.
        """
        q: asyncio.Queue[RuntimeEvent | _Heartbeat] = asyncio.Queue(
            maxsize=self._queue_max
        )
        async with self._lock:
            self._subscribers.append(q)
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(q),
            name="murmur-sse-heartbeat",
        )
        try:
            while True:
                item = await q.get()
                if isinstance(item, _Heartbeat):
                    yield {"event": "ping", "data": ""}
                else:
                    yield {
                        "event": item.event_type.value,
                        "data": item.model_dump_json(),
                    }
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            async with self._lock:
                if q in self._subscribers:
                    self._subscribers.remove(q)

    async def _heartbeat_loop(
        self, q: asyncio.Queue[RuntimeEvent | _Heartbeat]
    ) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            # If the consumer is so far behind that even a heartbeat can't
            # fit, the next real event will probably also drop — no point
            # logging twice.
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(_HEARTBEAT)


__all__ = ["SSEEventEmitter"]
