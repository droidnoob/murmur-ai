"""Internal dispatch helpers — the one non-interop place ``pydantic_ai`` lives.

:func:`build_pydantic_ai_agent` constructs a configured ``pydantic_ai.Agent``
from a Murmur :class:`murmur.Agent`. Each registered tool is wrapped through
the runtime :class:`murmur.tools.ToolExecutor` so policy enforcement (trust
level, allow-list, logging) always runs *before* the underlying callable. The
wrapper preserves the original callable's signature so PydanticAI's tool
schema introspection keeps working.

This module is private (leading underscore). Both ``ThreadBackend`` (locally)
and the broker-side ``Worker`` (which runs the same code path on the consumer
end of ``JobBackend``) call into it. The symmetry collapses what would
otherwise be duplicated dispatch code into one place.
"""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Any

import pydantic_ai

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from murmur.agent import Agent
    from murmur.tools.executor import ToolExecutor
    from murmur.tools.registry import ToolFunc, ToolRegistry
    from murmur.types import TrustLevel


def build_pydantic_ai_agent(
    *,
    agent: Agent,
    allowed: frozenset[str],
    registry: ToolRegistry,
    executor: ToolExecutor,
    task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    """Build a ``pydantic_ai.Agent`` for one Murmur run.

    The returned agent's tools dispatch through ``executor`` — the original
    callables are never invoked directly from the agent's run loop.
    """
    pa_agent: pydantic_ai.Agent[None, Any] = pydantic_ai.Agent(
        model=agent.model,
        instructions=agent.instructions,
        output_type=agent.output_type,
    )
    for name in allowed:
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


def _wrap_for_executor(
    *,
    name: str,
    original: ToolFunc,
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
