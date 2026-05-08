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
    """Asyncio pub/sub. Satisfies :class:`murmur.core.protocols.Broker`.

    Supports both broadcast (``group=None``) and competing-consumer
    (``group=<str>``) delivery — see the Protocol docstring. Competing-
    consumer pools route each message to exactly one handler via round-robin
    over the pool's registration order, so multiple Workers serving the same
    agent split work instead of duplicating it.
    """

    def __init__(self) -> None:
        self._broadcast: dict[str, list[MessageHandler]] = defaultdict(list)
        # ``_groups[topic][group]`` is the ordered list of handlers in that
        # competing-consumer pool, plus a round-robin cursor sitting next to
        # it. Keeping cursor and handlers in lockstep keeps ``publish``
        # branch-free.
        self._groups: dict[str, dict[str, list[MessageHandler]]] = defaultdict(dict)
        self._cursors: dict[tuple[str, str], int] = {}
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
        self._broadcast.clear()
        self._groups.clear()
        self._cursors.clear()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._started:
            raise RuntimeError("InMemoryBroker.publish called before start()")
        # Broadcast subscribers — every handler sees the message.
        for handler in list(self._broadcast.get(topic, ())):
            self._spawn(handler, payload)
        # Competing-consumer pools — each pool delivers to exactly one
        # handler, picked round-robin so a fleet of Workers shares load.
        for group, handlers in self._groups.get(topic, {}).items():
            if not handlers:
                continue
            key = (topic, group)
            idx = self._cursors.get(key, 0) % len(handlers)
            self._cursors[key] = idx + 1
            self._spawn(handlers[idx], payload)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
        prefetch: int | None = None,
        consumer_id: str | None = None,
        reclaim_min_idle_ms: int | None = None,
    ) -> None:
        # ``prefetch`` is a no-op here — InMemoryBroker dispatches one
        # message per ``publish`` call already, so per-poll batch size
        # has nothing to bound. ``consumer_id`` likewise has no role
        # without a persistent stream / pending-entries list.
        # ``reclaim_min_idle_ms`` is meaningless without a PEL — there's
        # no acknowledged-vs-pending distinction in this broker; messages
        # delivered are dropped from memory.
        # Accept the kwargs to satisfy the Protocol so production code
        # paths don't need a separate code branch when targeting
        # in-memory.
        del prefetch, consumer_id, reclaim_min_idle_ms
        if group is None:
            self._broadcast[topic].append(handler)
            return
        self._groups[topic].setdefault(group, []).append(handler)

    def _spawn(self, handler: MessageHandler, payload: bytes) -> None:
        task = asyncio.create_task(self._dispatch(handler, payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @staticmethod
    async def _dispatch(handler: MessageHandler, payload: bytes) -> None:
        try:
            await handler(payload)
        except Exception as exc:
            await log.aerror("broker_handler_failed", error=str(exc))


__all__ = ["InMemoryBroker"]
