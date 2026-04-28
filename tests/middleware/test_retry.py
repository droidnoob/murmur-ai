"""Tests for ``RetryMiddleware``."""

from __future__ import annotations

import pytest

from murmur.core.errors import SpawnError
from murmur.core.pipeline import PipelineContext
from murmur.middleware.retry import RetryMiddleware
from murmur.types import TaskSpec


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(task=TaskSpec(input="x"), agent_name="a")


async def test_succeeds_on_first_try(ctx: PipelineContext) -> None:
    calls = 0

    async def ok(_: PipelineContext) -> int:
        nonlocal calls
        calls += 1
        return 1

    mw = RetryMiddleware(max_attempts=3, backoff_factor=0.01)
    assert await mw(ctx, ok) == 1
    assert calls == 1


async def test_retries_on_spawn_error(ctx: PipelineContext) -> None:
    calls = 0

    async def flaky(_: PipelineContext) -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise SpawnError("transient")
        return 7

    mw = RetryMiddleware(max_attempts=3, backoff_factor=0.001)
    assert await mw(ctx, flaky) == 7
    assert calls == 3


async def test_raises_after_max_attempts(ctx: PipelineContext) -> None:
    async def always_fail(_: PipelineContext) -> int:
        raise SpawnError("nope")

    mw = RetryMiddleware(max_attempts=2, backoff_factor=0.001)
    with pytest.raises(SpawnError, match="nope"):
        await mw(ctx, always_fail)


def test_invalid_args_rejected() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryMiddleware(max_attempts=0)
    with pytest.raises(ValueError, match="backoff_factor"):
        RetryMiddleware(backoff_factor=0)
