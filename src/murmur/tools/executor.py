"""Tool executor — proxies tool calls from agents through runtime policy.

Tools never execute inside the agent. The agent emits a request, the runtime
intercepts, applies trust-level policy, executes via :class:`ToolRegistry`,
emits a :class:`RuntimeEvent` for each lifecycle phase, and returns the
result.

Concrete satisfying :class:`murmur.core.protocols.tools.ToolExecutor`
structurally. Wired into :class:`murmur.AgentRuntime` at dispatch time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from murmur.core.errors import ToolExecutionError, TrustViolationError
from murmur.events.log import LogEventEmitter
from murmur.events.types import EventType, RuntimeEvent
from murmur.tools.registry import ToolRegistry
from murmur.types import TrustLevel

if TYPE_CHECKING:
    from murmur.core.protocols.events import EventEmitter


_READ_ONLY_TOOLS: frozenset[str] = frozenset({"read_file", "web_search"})
"""Tools considered safe under :attr:`TrustLevel.LOW`. Extend explicitly."""


class ToolExecutor:
    """Policy-aware tool dispatcher."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        event_emitter: EventEmitter | None = None,
    ) -> None:
        self._registry = registry
        # Default emitter forwards every event to structlog with the same
        # event names previously used by direct ``log.ainfo`` calls — so
        # callers asserting on ``capture_logs()`` keep working without
        # opting into the event API.
        self._emitter: EventEmitter = event_emitter or LogEventEmitter()

    async def execute(
        self,
        *,
        agent_name: str,
        task_id: str,
        trust_level: TrustLevel,
        allowed: frozenset[str],
        name: str,
        args: dict[str, object],
        trace_id: str | None = None,
        external_call: Callable[..., Awaitable[object]] | None = None,
        low_trust_overrides: frozenset[str] = frozenset(),
    ) -> object:
        """Apply policy, emit lifecycle events, dispatch.

        ``external_call`` is the escape hatch for tools that don't live in the
        local :class:`ToolRegistry` — currently MCP-discovered tools, where the
        callable closes over the originating provider plus the tool name. The
        same trust + allow-list + lifecycle-event gate runs regardless of which
        dispatch path the call takes.

        ``low_trust_overrides`` is the per-call extension to
        :data:`_READ_ONLY_TOOLS`. MCP providers pass their explicit ``allow``
        list here so a user who opts a tool into ``LOW`` trust at the provider
        level isn't blocked by the global read-only set (which only knows
        about native tools).

        ``trace_id`` is forwarded into the emitted :class:`RuntimeEvent`. When
        ``None`` (the default for unwired callers), ``task_id`` substitutes —
        a tool call without a task lineage is rare but possible in tests.
        """
        if trust_level is TrustLevel.SANDBOX:
            raise TrustViolationError(
                f"agent '{agent_name}' has SANDBOX trust — no tools permitted"
            )
        if trust_level is TrustLevel.LOW and name not in (
            _READ_ONLY_TOOLS | low_trust_overrides
        ):
            raise TrustViolationError(
                f"agent '{agent_name}' has LOW trust — '{name}' is not read-only"
            )
        if name not in allowed:
            raise TrustViolationError(
                f"tool '{name}' is not in the allow-list for agent '{agent_name}'"
            )

        func: Callable[..., Awaitable[object]] = (
            external_call if external_call is not None else self._registry.get(name)
        )

        effective_trace_id = trace_id if trace_id is not None else task_id

        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.TOOL_CALL_STARTED,
                agent_name=agent_name,
                task_id=task_id,
                trace_id=effective_trace_id,
                payload={"tool_name": name, "trust_level": trust_level.value},
            )
        )
        try:
            result = await func(**args)
        except Exception as exc:
            await self._emitter.emit(
                RuntimeEvent(
                    event_type=EventType.TOOL_CALL_FAILED,
                    agent_name=agent_name,
                    task_id=task_id,
                    trace_id=effective_trace_id,
                    payload={"tool_name": name, "error": str(exc)},
                )
            )
            raise ToolExecutionError(f"tool '{name}' failed: {exc}") from exc

        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.TOOL_CALL_COMPLETED,
                agent_name=agent_name,
                task_id=task_id,
                trace_id=effective_trace_id,
                payload={"tool_name": name},
            )
        )
        return result


__all__ = ["ToolExecutor"]
