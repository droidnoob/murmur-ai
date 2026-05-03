"""``run_team`` — dispatch an :class:`AgentTeam` against a task.

Auto-registers the typed ``delegate(target, input)`` tool on the
runtime's tool registry under a per-run unique name, builds a
coordinator copy with that tool wired in, dispatches the coordinator
via :meth:`AgentRuntime.run`, and unregisters the tool on exit.

Per-run tool scope is critical: a long-lived runtime serving multiple
team runs concurrently must not see stale ``delegate`` tools from
prior runs leak across to unrelated coordinators. The unique name is
derived from the team name + a short UUID; the registration is
released in a ``finally`` block so even raised exceptions don't leave
orphans behind.

Cascading-spawn machinery (depth, ancestors, parent_trace_id, cycle
detection, spawn cap, token budget, signed envelopes) applies
uniformly because every ``delegate(target, input)`` call dispatches
through ``runtime.run`` — the same path single-agent runs take. No
team-specific bypass exists at the runtime level.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from pydantic import BaseModel

from murmur.backends.async_backend import AsyncBackend
from murmur.groups.team import AgentTeam, _make_delegate_tool
from murmur.tools.registry import ToolRegistry
from murmur.types import AgentResult, TaskSpec

if TYPE_CHECKING:
    from murmur.runtime import AgentRuntime


def _per_run_tool_name(team_name: str) -> str:
    """Build a UUID-tagged tool name unique across concurrent team runs."""
    return f"_team_{team_name}_delegate_{uuid.uuid4().hex[:8]}"


def _resolve_dispatch_registry(runtime: AgentRuntime) -> ToolRegistry:
    """Return the :class:`ToolRegistry` the runtime's backend will hit at dispatch.

    ``AgentRuntime`` constructs an :class:`AsyncBackend` sharing its own
    ``tool_registry`` — the two views are identical. But callers can
    inject a backend (``AgentRuntime(backend=AsyncBackend())``) whose
    registry is independent of ``runtime.tool_registry``; in that case
    a tool registered on the runtime's view is invisible at dispatch
    time. Resolve to the backend's registry so the per-run delegate
    tool always lands where the dispatch will look it up.
    """
    backend = runtime.backend
    if isinstance(backend, AsyncBackend):
        return backend.tool_registry
    return runtime.tool_registry


async def run_team(
    runtime: AgentRuntime,
    team: AgentTeam,
    task: TaskSpec,
) -> AgentResult[BaseModel]:
    """Execute ``team`` against ``task`` and return the coordinator's result.

    The coordinator runs as a single agent with the auto-generated
    ``delegate`` tool added to its tool surface and ``output_type``
    pinned to ``team.output_type``. The LLM picks targets from the
    closed enum, supplies typed input, and synthesises the final
    output.

    Per-call delegate failures surface as :class:`ToolExecutionError`
    inside the coordinator's tool loop — the LLM can choose to retry,
    route to a different delegate, or surface the error in the final
    synthesis. Top-level failures (the coordinator itself failing)
    return an :class:`AgentResult` with ``error`` set, same as any
    single-agent run.

    The tool registration is per-run: the unique name is released on
    exit (success, exception, or cancellation) so the runtime's tool
    registry stays free of orphans.

    Distributed dispatch (``JobBackend``) is **not** supported in this
    iteration. The modified coordinator and its synthesised
    ``delegate`` tool are constructed in-process on the publisher;
    ``JobBackend`` only ships ``Agent.name`` over the broker, so the
    worker — which resolves agents from its pre-registered map and
    runs them against its own ``ToolRegistry`` — never sees the
    delegate tool. ``run_team`` raises a clear ``NotImplementedError``
    when the runtime's backend is broker-backed; bridging the team
    spec across the broker is a follow-up work unit.
    """
    if not isinstance(runtime.backend, AsyncBackend):
        raise NotImplementedError(
            f"AgentTeam dispatch through {type(runtime.backend).__name__!r} "
            f"is not supported — the modified coordinator and per-run "
            f"delegate tool live in the publisher's process, but "
            f"broker-backed dispatch ships only Agent.name across the "
            f"wire. Use AsyncBackend for AgentTeam, or AgentGroup if "
            f"distributed dispatch is required."
        )
    tool_name = _per_run_tool_name(team.name)
    registry = _resolve_dispatch_registry(runtime)
    delegate_tool = _make_delegate_tool(
        runtime,
        team.delegates,
        retain_history=team.retain_delegate_history,
        max_rounds=team.max_rounds,
    )
    registry.register(tool_name, delegate_tool)
    try:
        modified_coordinator = team.coordinator.with_(
            tools=team.coordinator.tools | {tool_name},
            output_type=team.output_type,
        )
        return await runtime.run(modified_coordinator, task)
    finally:
        registry.unregister(tool_name)


__all__ = ["run_team"]
