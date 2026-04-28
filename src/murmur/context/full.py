"""FullContextPasser — pass the entire context through unchanged."""

from __future__ import annotations

from murmur.types import AgentContext, TaskSpec


class FullContextPasser:
    """Forward the inbound context verbatim. The most expensive option."""

    async def prepare(
        self,
        context: AgentContext,
        task: TaskSpec,  # noqa: ARG002 — protocol arg
    ) -> AgentContext:
        return context


__all__ = ["FullContextPasser"]
