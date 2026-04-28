"""Backend Protocol — execution unit for an agent run.

Required surface: ``spawn``, ``kill``, ``status``. ``result`` is added so the
runtime can retrieve a finished agent's output without the caller juggling
futures itself.

Concrete backends (``ThreadBackend``, ``JobBackend``, future ``ProcessBackend``
and ``ContainerBackend``) match this Protocol structurally — they do **not**
inherit from it.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pydantic import BaseModel

    from murmur.agent import Agent
    from murmur.types import AgentContext, AgentHandle, AgentResult, TaskSpec


class BackendStatus(StrEnum):
    """Coarse-grained execution state for a handle."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"


class Backend(Protocol):
    """Pluggable execution backend.

    Implementations must be safe to call concurrently from many tasks against
    a single instance. State per spawn is keyed by :class:`AgentHandle`.
    """

    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> AgentHandle:
        """Begin executing ``agent`` against ``task`` and return a handle."""
        ...

    async def status(self, handle: AgentHandle) -> BackendStatus:
        """Return the current execution state for ``handle``."""
        ...

    async def kill(self, handle: AgentHandle) -> None:
        """Terminate ``handle`` early. Idempotent."""
        ...

    async def result(self, handle: AgentHandle) -> AgentResult[BaseModel]:
        """Block until ``handle`` reaches a terminal state and return its result."""
        ...


__all__ = ["Backend", "BackendStatus"]
