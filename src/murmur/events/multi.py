"""MultiEventEmitter — fan-out across many sinks.

Used by :class:`AgentRuntime` when the host wants more than one
observability sink — typically ``LogEventEmitter`` plus an
``SSEEventEmitter`` for the dashboard, plus optionally a custom
collector for tests or metrics.

A failure in one sub-emitter must **not** stop the others from
delivering. We use ``asyncio.gather(..., return_exceptions=True)`` so
every sibling sees the event regardless of which one raises.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from murmur.core.protocols.events import EventEmitter
    from murmur.events.types import RuntimeEvent


class MultiEventEmitter:
    """Broadcast each emitted event to every wrapped emitter.

    Construction takes a sequence (typically a list or tuple) of emitters.
    Wrap as the runtime's ``event_emitter`` to attach more than one sink:

    >>> from murmur.events import LogEventEmitter, MultiEventEmitter
    >>> emitter = MultiEventEmitter([LogEventEmitter(), my_metrics_emitter])
    >>> runtime = AgentRuntime(event_emitter=emitter)
    """

    def __init__(self, emitters: Sequence[EventEmitter]) -> None:
        self._emitters: tuple[EventEmitter, ...] = tuple(emitters)

    @property
    def emitters(self) -> tuple[EventEmitter, ...]:
        """The wrapped emitters in declaration order."""
        return self._emitters

    async def emit(self, event: RuntimeEvent) -> None:
        if not self._emitters:
            return
        # ``return_exceptions=True`` ensures a failing sibling can't stop
        # the others — observability should never take an agent run down.
        await asyncio.gather(
            *(e.emit(event) for e in self._emitters),
            return_exceptions=True,
        )


__all__ = ["MultiEventEmitter"]
