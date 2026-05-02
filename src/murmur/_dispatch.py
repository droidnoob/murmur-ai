"""Internal dispatch helpers — the one non-interop place ``pydantic_ai`` lives.

:func:`build_pydantic_ai_agent` constructs a configured ``pydantic_ai.Agent``
from a Murmur :class:`murmur.Agent`. Each registered tool is wrapped through
the runtime :class:`murmur.tools.ToolExecutor` so policy enforcement (trust
level, allow-list, logging) always runs *before* the underlying callable. The
wrapper preserves the original callable's signature so PydanticAI's tool
schema introspection keeps working.

MCP-discovered tools route through the same gate via a
:class:`_PolicyMCPToolset` wrapper that intercepts ``call_tool`` on each
``MCPServer`` and delegates through ``ToolExecutor.execute`` — so MCP tools
emit the identical ``tool_call_started`` / ``tool_call_completed`` /
``tool_call_failed`` lifecycle as native tools.

This module is private (leading underscore). Both ``ThreadBackend`` (locally)
and the broker-side ``Worker`` (which runs the same code path on the consumer
end of ``JobBackend``) call into it. The symmetry collapses what would
otherwise be duplicated dispatch code into one place.
"""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pydantic_ai
from pydantic_ai.toolsets import AbstractToolset, WrapperToolset

from murmur.core.protocols.toolsets import ToolsetProvider
from murmur.types import TrustLevel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic_ai.tools import RunContext
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from murmur.agent import Agent
    from murmur.tools.executor import ToolExecutor
    from murmur.tools.registry import ToolFunc, ToolRegistry


async def build_pydantic_ai_agent(
    *,
    agent: Agent,
    allowed: frozenset[str],
    registry: ToolRegistry,
    executor: ToolExecutor,
    task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    """Build a ``pydantic_ai.Agent`` for one Murmur run.

    The returned agent's tools dispatch through ``executor`` — the original
    callables are never invoked directly from the agent's run loop. MCP
    toolsets, if any, are wrapped to route through the same executor.
    """
    pa_toolsets, mcp_tool_names = await _resolve_mcp_toolsets(
        agent=agent,
        allowed=allowed,
        executor=executor,
        task_id=task_id,
    )
    # ``model_settings`` is opt-in (None default); PydanticAI validates
    # provider-specific keys at request time. ``dict(...)`` defends against
    # later mutation of the user's mapping; PA's parameter is typed as a
    # TypedDict but accepts any Mapping at runtime.
    pa_model_settings = (
        dict(agent.model_settings) if agent.model_settings is not None else None
    )
    pa_agent: pydantic_ai.Agent[None, Any] = pydantic_ai.Agent(  # ty: ignore[invalid-assignment]
        model=agent.model,
        instructions=agent.instructions,
        output_type=agent.output_type,
        toolsets=pa_toolsets if pa_toolsets else None,
        model_settings=pa_model_settings,  # ty: ignore[invalid-argument-type]
    )
    for name in allowed:
        if name in mcp_tool_names:
            # MCP tools are dispatched via the wrapped toolset, not as
            # PydanticAI plain-tools — skip to avoid duplicate registration.
            continue
        original = registry.get(name)
        wrapper = _wrap_for_executor(
            name=name,
            original=original,
            agent_name=agent.name,
            task_id=task_id,
            trust_level=agent.trust_level,
            allowed=allowed,
            executor=executor,
        )
        pa_agent.tool_plain(wrapper)
    return pa_agent


async def _resolve_mcp_toolsets(
    *,
    agent: Agent,
    allowed: frozenset[str],
    executor: ToolExecutor,
    task_id: str,
) -> tuple[list[AbstractToolset[Any]], frozenset[str]]:
    """Discover each MCP provider's tools, wrap each in a policy toolset.

    Returns ``(toolsets, discovered_tool_names)``. ``discovered_tool_names``
    is the union of every name **after applying the trust filter** — that
    way :func:`build_pydantic_ai_agent`'s native-tool registration loop
    skips MCP names without re-checking trust.

    Trust-level semantics (mirrors :class:`MCPToolsetProvider` docstring):

    - ``SANDBOX``: no MCP tools at all — providers aren't even queried.
    - ``LOW`` + ``provider.allow is None``: provider skipped (LOW requires
      explicit opt-in; MCP self-declared ``readOnlyHint`` isn't a
      security boundary).
    - ``LOW`` + ``provider.allow=(...)``: only allow-listed names exposed;
      they're passed to the executor as ``low_trust_overrides`` so the
      readonly gate accepts them.
    - ``MEDIUM`` / ``HIGH`` + ``allow=None``: every tool the server reports.
    - ``MEDIUM`` / ``HIGH`` + ``allow=(...)``: only the listed subset.

    Discovery goes through the underlying ``MCPServer.list_tools()``
    directly when available — the latter manages its own
    ``__aenter__``/``__aexit__`` per call. The provider's
    wrapper-level lifecycle (``start`` / ``stop``) is opt-in pre-warming.
    """
    if not agent.mcp_servers or agent.trust_level is TrustLevel.SANDBOX:
        return [], frozenset()

    toolsets: list[AbstractToolset[Any]] = []
    discovered: set[str] = set()
    for provider in agent.mcp_servers:
        provider_allow = getattr(provider, "allow", None)
        if agent.trust_level is TrustLevel.LOW and provider_allow is None:
            # LOW trust without an explicit opt-in list — skip provider.
            continue

        underlying = _underlying_toolset(provider)
        all_names = await _discover_tool_names(underlying, provider)
        names = all_names & provider_allow if provider_allow is not None else all_names
        if not names:
            continue
        discovered |= names

        low_trust_overrides = (
            names if agent.trust_level is TrustLevel.LOW else frozenset()
        )

        toolsets.append(
            _PolicyMCPToolset(
                wrapped=underlying,
                agent_name=agent.name,
                task_id=task_id,
                trust_level=agent.trust_level,
                allowed=allowed | names,
                executor=executor,
                low_trust_overrides=low_trust_overrides,
            )
        )
    return toolsets, frozenset(discovered)


async def _discover_tool_names(
    underlying: AbstractToolset[Any],
    provider: ToolsetProvider,
) -> frozenset[str]:
    """Resolve tool names from the underlying MCP server.

    Prefers ``MCPServer.list_tools()`` (which auto-manages its context)
    when present; falls back to ``provider.list_tools()`` for non-MCP
    providers that may appear later. Either path returns the same set of
    names — the choice is purely about lifecycle ergonomics.
    """
    pa_list_tools = getattr(underlying, "list_tools", None)
    if callable(pa_list_tools):
        mcp_tools = await pa_list_tools()
        return frozenset(t.name for t in mcp_tools)
    descriptors = await provider.list_tools()
    return frozenset(d.name for d in descriptors)


def _underlying_toolset(provider: ToolsetProvider) -> AbstractToolset[Any]:
    """Pull the PydanticAI ``MCPServer`` out of a Murmur provider.

    Today the only concrete is :class:`murmur.tools.mcp.MCPToolsetProvider`
    which holds its server on ``_mcp``. We avoid a hard import to keep
    ``_dispatch`` decoupled from the concrete; if a non-MCP provider appears
    in future, add a Protocol method like ``inner_toolset()`` rather than
    importing more concretes here.
    """
    inner = getattr(provider, "_mcp", None)
    if inner is None or not isinstance(inner, AbstractToolset):
        raise TypeError(
            f"toolset provider {type(provider).__name__} does not expose a "
            "PydanticAI AbstractToolset on `_mcp`"
        )
    return inner


@dataclass(kw_only=True)
class _PolicyMCPToolset(WrapperToolset[Any]):
    """Wraps an MCP ``AbstractToolset`` so every ``call_tool`` flows through
    Murmur's :class:`ToolExecutor`.

    Constructed fresh per spawn (closure over ``task_id``, ``allowed``, etc.)
    so concurrent runs of the same agent don't share mutable state.

    ``low_trust_overrides`` is forwarded to the executor so MCP tools that
    were explicitly opted into ``TrustLevel.LOW`` via ``provider.allow``
    pass the read-only gate.
    """

    agent_name: str
    task_id: str
    trust_level: TrustLevel
    allowed: frozenset[str]
    executor: ToolExecutor
    low_trust_overrides: frozenset[str] = field(default_factory=frozenset)

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        async def _delegate(**kwargs: object) -> object:
            return await self.wrapped.call_tool(name, dict(kwargs), ctx, tool)

        return await self.executor.execute(
            agent_name=self.agent_name,
            task_id=self.task_id,
            trust_level=self.trust_level,
            allowed=self.allowed,
            name=name,
            args=tool_args,
            external_call=_delegate,
            low_trust_overrides=self.low_trust_overrides,
        )


def _wrap_for_executor(
    *,
    name: str,
    original: ToolFunc[Any],
    agent_name: str,
    task_id: str,
    trust_level: TrustLevel,
    allowed: frozenset[str],
    executor: ToolExecutor,
) -> Callable[..., Awaitable[object]]:
    sig = inspect.signature(original)

    @functools.wraps(original)
    async def wrapper(*args: object, **kwargs: object) -> object:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        return await executor.execute(
            agent_name=agent_name,
            task_id=task_id,
            trust_level=trust_level,
            allowed=allowed,
            name=name,
            args=dict(bound.arguments),
        )

    # PydanticAI introspects __signature__ to build the tool's JSON schema.
    wrapper.__signature__ = sig  # ty: ignore[unresolved-attribute]  # set on FunctionType
    return wrapper


__all__ = ["build_pydantic_ai_agent"]
