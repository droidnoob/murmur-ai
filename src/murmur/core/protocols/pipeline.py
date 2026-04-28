"""Pipeline / Stage / Middleware Protocols.

Stages and middleware share the same callable shape — the distinction is
intent. A *stage* produces or transforms the result; a *middleware* wraps a
stage for a cross-cutting concern (timeout, retry, logging, depth limit).
The runtime treats them identically.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    # The concrete PipelineContext lives in core/pipeline.py. We don't import
    # it at runtime here because Protocols are pure shape; concretes import
    # PipelineContext directly.
    from murmur.core.pipeline import PipelineContext

T = TypeVar("T")

NextStage = Callable[["PipelineContext"], Awaitable[T]]
"""Type of the ``next_stage`` callable a stage / middleware receives."""


class Stage(Protocol[T]):
    """A pipeline stage. Produces or transforms a result."""

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T: ...


class Middleware(Protocol[T]):
    """Cross-cutting wrapper around a stage. Same shape as Stage; different intent."""

    async def __call__(
        self,
        context: PipelineContext,
        next_stage: NextStage[T],
    ) -> T: ...


class Pipeline(Protocol[T]):
    """A composed chain of stages and middleware."""

    async def run(self, context: PipelineContext) -> T:
        """Execute the chain end-to-end and return the final result."""
        ...


__all__ = ["Middleware", "NextStage", "Pipeline", "Stage"]
