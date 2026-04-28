"""Timeout middleware — wraps a stage in ``asyncio.timeout``."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar

from murmur.core.errors import SpawnError

if TYPE_CHECKING:
    from murmur.core.pipeline import NextStage, PipelineContext

T = TypeVar("T")


class TimeoutMiddleware:
    """Cancel the downstream stage if it takes longer than ``seconds``."""

    def __init__(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("timeout seconds must be > 0")
        self._seconds = seconds

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T:
        try:
            async with asyncio.timeout(self._seconds):
                return await next_stage(context)
        except TimeoutError as exc:
            raise SpawnError(
                f"stage timed out after {self._seconds}s "
                f"(agent={context.agent_name}, task={context.task.id})"
            ) from exc


__all__ = ["TimeoutMiddleware"]
