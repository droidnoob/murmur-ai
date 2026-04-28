"""Tests for ``DepthLimitMiddleware``."""

from __future__ import annotations

import pytest

from murmur.core.errors import DepthLimitError
from murmur.core.pipeline import PipelineContext
from murmur.middleware.depth_limit import DepthLimitMiddleware
from murmur.types import AgentContext, TaskSpec


def _ctx(depth: int) -> PipelineContext:
    return PipelineContext(
        task=TaskSpec(input="x"),
        agent_name="a",
        agent_context=AgentContext(depth=depth),
    )


async def test_passes_through_under_limit() -> None:
    async def stage(_: PipelineContext) -> int:
        return 1

    mw = DepthLimitMiddleware(max_depth=3)
    assert await mw(_ctx(depth=2), stage) == 1


async def test_rejects_at_or_above_limit() -> None:
    async def stage(_: PipelineContext) -> int:
        return 1

    mw = DepthLimitMiddleware(max_depth=3)
    with pytest.raises(DepthLimitError):
        await mw(_ctx(depth=3), stage)


def test_invalid_max_depth_rejected() -> None:
    with pytest.raises(ValueError, match="max_depth"):
        DepthLimitMiddleware(max_depth=0)
