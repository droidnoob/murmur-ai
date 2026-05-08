"""``StoreEventEmitter`` — adapt an :class:`EventStore` to the EventEmitter Protocol.

Wire alongside :class:`LogEventEmitter` and :class:`SSEEventEmitter`
inside a :class:`MultiEventEmitter` so each runtime event lands in
structlog, the live SSE firehose, *and* the persistent store.

A failure to persist must not take an agent run down — the emitter
swallows store errors and logs them at warn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from murmur.core.protocols.event_store import EventStore
    from murmur.events.types import RuntimeEvent

log: structlog.stdlib.BoundLogger = structlog.get_logger()


class StoreEventEmitter:
    """Adapter: every emit appends to the wrapped :class:`EventStore`."""

    def __init__(self, store: EventStore) -> None:
        self._store = store

    @property
    def store(self) -> EventStore:
        return self._store

    async def emit(self, event: RuntimeEvent) -> None:
        try:
            await self._store.append(event)
        except Exception as exc:  # noqa: BLE001 — observability must not raise
            await log.awarning(
                "event_store_append_failed",
                event_type=event.event_type.value,
                trace_id=event.trace_id,
                error=str(exc),
            )


__all__ = ["StoreEventEmitter"]
