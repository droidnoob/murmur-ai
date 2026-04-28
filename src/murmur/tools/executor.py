"""Tool executor — proxies tool calls from agents through runtime policy.

Tools never execute inside the agent. The agent emits a request, the runtime
intercepts, applies trust-level policy, executes via :class:`ToolRegistry`,
logs, and returns the result.

Concrete satisfying :class:`murmur.core.protocols.tools.ToolExecutor`
structurally. Phase 1 stub — wired into :class:`murmur.AgentRuntime` once
the agent dispatch loop lands.
"""

from __future__ import annotations

import structlog

from murmur.core.errors import ToolExecutionError, TrustViolationError
from murmur.tools.registry import ToolRegistry
from murmur.types import TrustLevel

log: structlog.stdlib.BoundLogger = structlog.get_logger()


_READ_ONLY_TOOLS: frozenset[str] = frozenset({"read_file", "web_search"})
"""Tools considered safe under :attr:`TrustLevel.LOW`. Extend explicitly."""


class ToolExecutor:
    """Policy-aware tool dispatcher."""

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

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
        if trust_level is TrustLevel.SANDBOX:
            raise TrustViolationError(
                f"agent '{agent_name}' has SANDBOX trust — no tools permitted"
            )
        if trust_level is TrustLevel.LOW and name not in _READ_ONLY_TOOLS:
            raise TrustViolationError(
                f"agent '{agent_name}' has LOW trust — '{name}' is not read-only"
            )
        if name not in allowed:
            raise TrustViolationError(
                f"tool '{name}' is not in the allow-list for agent '{agent_name}'"
            )

        func = self._registry.get(name)

        await log.ainfo(
            "tool_call_started",
            agent_name=agent_name,
            task_id=task_id,
            tool_name=name,
            trust_level=trust_level.value,
        )
        try:
            result = await func(**args)
        except Exception as exc:
            await log.aerror(
                "tool_call_failed",
                agent_name=agent_name,
                task_id=task_id,
                tool_name=name,
                error=str(exc),
            )
            raise ToolExecutionError(f"tool '{name}' failed: {exc}") from exc

        await log.ainfo(
            "tool_call_completed",
            agent_name=agent_name,
            task_id=task_id,
            tool_name=name,
        )
        return result


__all__ = ["ToolExecutor"]
