"""ContextPasser Protocol — pluggable policy for cross-agent context.

Concrete passers (``FullContextPasser``, ``NullContextPasser``, future
``SummaryContextPasser`` and ``SelectiveContextPasser``) match this Protocol
structurally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from murmur.types import AgentContext, TaskSpec


@runtime_checkable
class ContextPasser(Protocol):
    """Strategy for preparing context before an agent spawn.

    Marked ``@runtime_checkable`` so Pydantic can validate ``Agent.context_passer``
    fields via ``isinstance``.
    """

    async def prepare(
        self,
        context: AgentContext,
        task: TaskSpec,
    ) -> AgentContext:
        """Return the context the next agent should see, given ``task``."""
        ...


__all__ = ["ContextPasser"]
