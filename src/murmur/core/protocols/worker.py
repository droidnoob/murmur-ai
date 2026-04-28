"""Worker Protocol — distributed-mode consumer.

A ``Worker`` subscribes to broker topics for a set of agents and dispatches
incoming tasks through a runtime. The ``on_task_*`` methods are decorator-style
hook registrations: they accept a coroutine and return it unchanged.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

OnStart = Callable[[str, str], Awaitable[None]]
"""``async def fn(task_id: str, agent_name: str) -> None``"""

OnComplete = Callable[[str, str, int], Awaitable[None]]
"""``async def fn(task_id: str, agent_name: str, duration_ms: int) -> None``"""

OnError = Callable[[str, str, BaseException], Awaitable[None]]
"""``async def fn(task_id: str, agent_name: str, error: BaseException) -> None``"""


class Worker(Protocol):
    """Distributed consumer Protocol."""

    async def start(self) -> None:
        """Begin consuming tasks. Returns when ``stop`` is called."""
        ...

    async def stop(self) -> None:
        """Drain in-flight tasks and stop consuming."""
        ...

    def on_task_start(self, fn: OnStart) -> OnStart:
        """Register a coroutine fired when a task starts. Returns ``fn`` unchanged."""
        ...

    def on_task_complete(self, fn: OnComplete) -> OnComplete:
        """Register a coroutine fired when a task completes."""
        ...

    def on_task_error(self, fn: OnError) -> OnError:
        """Register a coroutine fired when a task fails."""
        ...


__all__ = ["OnComplete", "OnError", "OnStart", "Worker"]
