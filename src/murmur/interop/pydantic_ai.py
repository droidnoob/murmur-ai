"""PydanticAI interop — wrap a user-supplied PydanticAI agent into a Murmur ``Agent``.

This module is the **only** place in the public package allowed to import
``pydantic_ai`` symbols. It exists to ease migration; new code should construct
:class:`murmur.Agent` directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic_ai import Agent as PydanticAIAgent

    from murmur.agent import Agent


def from_pydantic_ai(
    pydantic_ai_agent: PydanticAIAgent,
    *,
    name: str,
) -> Agent:
    """Wrap an existing PydanticAI ``Agent`` into a Murmur :class:`Agent`.

    Phase 1 stub — wiring lands once :class:`murmur.Agent` exposes its
    PydanticAI-driven internals.
    """
    raise NotImplementedError("from_pydantic_ai — Phase 1 stub")


__all__ = ["from_pydantic_ai"]
