"""Tests for ``TimeoutMiddleware``."""

from __future__ import annotations

import asyncio

import pytest

from murmur.core.errors import SpawnError
from murmur.core.pipeline import PipelineContext
from murmur.middleware.timeout import TimeoutMiddleware
from murmur.types import TaskSpec


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(task=TaskSpec(input="x"), agent_name="a")


async def test_passes_through_when_fast(ctx: PipelineContext) -> None:
    async def fast(_: PipelineContext) -> int:
        return 7

    mw = TimeoutMiddleware(seconds=1.0)
    assert await mw(ctx, fast) == 7


async def test_raises_spawn_error_on_timeout(ctx: PipelineContext) -> None:
    async def slow(_: PipelineContext) -> int:
        await asyncio.sleep(0.5)
        return 1

    mw = TimeoutMiddleware(seconds=0.05)
    with pytest.raises(SpawnError, match="timed out"):
        await mw(ctx, slow)


def test_zero_or_negative_seconds_rejected() -> None:
    with pytest.raises(ValueError, match="> 0"):
        TimeoutMiddleware(seconds=0)
    with pytest.raises(ValueError, match="> 0"):
        TimeoutMiddleware(seconds=-1.0)
