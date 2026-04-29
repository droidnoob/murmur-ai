"""FastStream interop — expose a Murmur ``Agent`` as a FastStream subscriber.

This module is the **only** place in the public package allowed to import
``faststream`` symbols. Use it to plug Murmur agents into an existing
FastStream application; greenfield code should rely on
:class:`murmur.AgentRuntime` and let it manage the broker internally.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import BaseModel

    from murmur.agent import Agent
    from murmur.runtime import AgentRuntime
    from murmur.types import AgentResult, TaskSpec


def as_faststream_handler(
    agent: Agent,
    *,
    runtime: AgentRuntime | None = None,
) -> Callable[[TaskSpec], Awaitable[AgentResult[BaseModel]]]:
    """Adapt ``agent`` to a FastStream message handler signature.

    Returns an async callable ``(task: TaskSpec) -> AgentResult[BaseModel]``
    that the user can register as a ``@broker.subscriber("topic")`` handler
    in their existing FastStream application. The agent runs through
    ``runtime`` (a fresh :class:`AgentRuntime` if not supplied — thread
    mode, no broker, no tools registered).

    For a fuller integration where Murmur owns broker lifecycle and
    workers, mount :class:`murmur.server.AgentRouter` instead.

    >>> from faststream.kafka import KafkaBroker
    >>> from murmur.interop import as_faststream_handler
    >>> broker = KafkaBroker("localhost:9092")
    >>> handler = as_faststream_handler(my_agent)
    >>> broker.subscriber("research.tasks")(handler)
    """
    # Lazy import — keep the module importable even when the runtime
    # subpackage hasn't been touched yet, and avoid an import cycle on
    # ``murmur.runtime`` → ``murmur.interop`` (which it doesn't have today,
    # but we want to keep optional).
    from murmur.runtime import AgentRuntime as _AgentRuntime

    rt: AgentRuntime = runtime if runtime is not None else _AgentRuntime()

    async def handler(task: TaskSpec) -> AgentResult[BaseModel]:
        return await rt.run(agent, task)

    return handler


__all__ = ["as_faststream_handler"]
