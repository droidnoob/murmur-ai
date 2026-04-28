"""In-memory broker — single-process pub/sub for tests and dev.

Used by:

- the unit tests that exercise :class:`JobBackend` / :class:`Worker` end-to-end
  without pulling in FastStream or a real message bus, and
- the ``memory://`` URL scheme, which lets users run the distributed-mode
  code path in a single process (useful for local debugging of broker-aware
  code without spinning up Kafka/NATS/Rabbit/Redis).

This module is **internal**. The leading underscore is intentional —
nothing here is exported from the public ``murmur`` namespace.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

import structlog

from murmur.core.protocols.broker import MessageHandler

log: structlog.stdlib.BoundLogger = structlog.get_logger()


class InMemoryBroker:
    """Asyncio pub/sub. Satisfies :class:`murmur.core.protocols.Broker`."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[MessageHandler]] = defaultdict(list)
        self._started: bool = False
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False
        # Drain any in-flight handler tasks so callers don't see stale work.
        pending = [t for t in self._tasks if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._tasks.clear()
        self._subscribers.clear()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._started:
            raise RuntimeError("InMemoryBroker.publish called before start()")
        handlers = list(self._subscribers.get(topic, ()))
        for handler in handlers:
            task = asyncio.create_task(self._dispatch(handler, payload))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        self._subscribers[topic].append(handler)

    @staticmethod
    async def _dispatch(handler: MessageHandler, payload: bytes) -> None:
        try:
            await handler(payload)
        except Exception as exc:
            await log.aerror("broker_handler_failed", error=str(exc))


__all__ = ["InMemoryBroker"]
