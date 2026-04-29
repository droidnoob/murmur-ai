"""EventEmitter Protocol — observability sink for runtime events.

Concrete emitters (log, SSE, …) implement this; the Protocol exists so
the rest of the code can be designed around it without depending on a
particular sink.
"""

from __future__ import annotations

from typing import Protocol


class EventEmitter(Protocol):
    """Sink for structured runtime events."""

    async def emit(self, event_type: str, payload: dict[str, object]) -> None:
        """Emit a single event. Implementations must be non-blocking."""
        ...


__all__ = ["EventEmitter"]
