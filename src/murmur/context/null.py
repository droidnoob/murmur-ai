"""NullContextPasser — spawn the agent with no inherited context."""

from __future__ import annotations

from murmur.types import AgentContext, TaskSpec


class NullContextPasser:
    """Drop everything except the task itself. The cheapest, safest option."""

    async def prepare(
        self,
        context: AgentContext,  # noqa: ARG002 — protocol arg
        task: TaskSpec,  # noqa: ARG002 — protocol arg
    ) -> AgentContext:
        return AgentContext()


__all__ = ["NullContextPasser"]
