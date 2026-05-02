"""EventEmitter Protocol — observability sink for runtime events.

Concrete emitters (``LogEventEmitter``, ``SSEEventEmitter``,
``MultiEventEmitter``) implement this structurally; the Protocol exists
so the rest of the code (runtime, executor, worker) can be designed
against the Protocol shape and parametrise the sink.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from murmur.events.types import RuntimeEvent


@runtime_checkable
class EventEmitter(Protocol):
    """Sink for typed :class:`RuntimeEvent` envelopes.

    Implementations must be safe to call concurrently. Emit is
    fire-and-forget from the runtime's perspective — emitters are
    expected to swallow their own delivery errors (a logging sink that
    raises would take the agent run down with it).
    """

    async def emit(self, event: RuntimeEvent) -> None:
        """Forward ``event`` to the underlying sink. Non-blocking."""
        ...


__all__ = ["EventEmitter"]
