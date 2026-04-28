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
    from murmur.types import AgentResult, TaskSpec


def as_faststream_handler(
    agent: Agent,
) -> Callable[[TaskSpec], Awaitable[AgentResult[BaseModel]]]:
    """Adapt ``agent`` to a FastStream message handler signature.

    Phase 1 stub — wiring lands once :class:`murmur.AgentRuntime` exposes a
    public dispatch entry point.
    """
    raise NotImplementedError("as_faststream_handler — Phase 1 stub")


__all__ = ["as_faststream_handler"]
