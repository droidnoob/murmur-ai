"""Retry middleware — re-runs the downstream stage on transient failure.

Only catches :class:`SpawnError` by default; broaden with care. Backoff is
multiplicative (``backoff_factor ** attempt``).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar

import structlog

from murmur.core.errors import SpawnError

if TYPE_CHECKING:
    from murmur.core.pipeline import NextStage, PipelineContext

T = TypeVar("T")
log: structlog.stdlib.BoundLogger = structlog.get_logger()


class RetryMiddleware:
    """Retry on :class:`SpawnError` with multiplicative backoff."""

    def __init__(self, max_attempts: int = 3, backoff_factor: float = 1.5) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_factor <= 0:
            raise ValueError("backoff_factor must be > 0")
        self._max_attempts = max_attempts
        self._backoff_factor = backoff_factor

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T:
        last_error: SpawnError | None = None
        for attempt in range(self._max_attempts):
            try:
                return await next_stage(context)
            except SpawnError as exc:
                last_error = exc
                await log.awarning(
                    "stage_retry",
                    agent_name=context.agent_name,
                    task_id=context.task.id,
                    attempt=attempt + 1,
                    max_attempts=self._max_attempts,
                    error=str(exc),
                )
                if attempt + 1 < self._max_attempts:
                    await asyncio.sleep(self._backoff_factor**attempt)

        assert last_error is not None  # exhausted only via the SpawnError branch
        raise last_error


__all__ = ["RetryMiddleware"]
