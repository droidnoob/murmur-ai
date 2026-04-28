"""Depth-limit middleware — caps cascading agent spawns at the runtime level.

The agent itself is not trusted to enforce its own spawn budget; the runtime is.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from murmur.core.errors import DepthLimitError

if TYPE_CHECKING:
    from murmur.core.pipeline import NextStage, PipelineContext

T = TypeVar("T")


class DepthLimitMiddleware:
    """Reject runs whose ``agent_context.depth`` exceeds ``max_depth``."""

    def __init__(self, max_depth: int = 4) -> None:
        if max_depth < 1:
            raise ValueError("max_depth must be >= 1")
        self._max_depth = max_depth

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T:
        depth = context.agent_context.depth
        if depth >= self._max_depth:
            raise DepthLimitError(
                f"cascading-spawn depth {depth} exceeds limit {self._max_depth} "
                f"(agent={context.agent_name})"
            )
        return await next_stage(context)


__all__ = ["DepthLimitMiddleware"]
