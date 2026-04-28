"""Tool-system Protocols — `ToolProvider` and `ToolExecutor`.

`ToolProvider` decides which tools an agent is allowed to call for a given
task. `ToolExecutor` runs those calls under runtime policy. They are separate
concerns and have separate Protocols.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from murmur.types import TrustLevel


class ToolProvider(Protocol):
    """Resolves which tools an agent may call for a particular task."""

    def resolve(
        self,
        agent_name: str,
        requested: frozenset[str],
    ) -> frozenset[str]:
        """Return the subset of ``requested`` the agent is allowed to use."""
        ...


class ToolExecutor(Protocol):
    """Executes a tool call on behalf of an agent under policy."""

    async def execute(
        self,
        *,
        agent_name: str,
        task_id: str,
        trust_level: TrustLevel,
        allowed: frozenset[str],
        name: str,
        args: dict[str, object],
    ) -> object:
        """Validate the call against policy, execute, log, and return the result."""
        ...


__all__ = ["ToolExecutor", "ToolProvider"]
