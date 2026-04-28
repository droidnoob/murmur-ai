"""EventEmitter Protocol — observability sink for runtime events.

Phase 2 ships concrete emitters (log, SSE, …); the Protocol exists in Phase 1
so the rest of the code can be designed around it.
"""

from __future__ import annotations

from typing import Protocol


class EventEmitter(Protocol):
    """Sink for structured runtime events."""

    async def emit(self, event_type: str, payload: dict[str, object]) -> None:
        """Emit a single event. Implementations must be non-blocking."""
        ...


__all__ = ["EventEmitter"]
