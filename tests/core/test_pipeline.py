"""Tests for the concrete ``Pipeline`` composer."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from murmur.core.pipeline import Pipeline, PipelineContext
from murmur.types import TaskSpec


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(task=TaskSpec(input="x"), agent_name="a")


async def test_single_terminal_stage_returns_value(ctx: PipelineContext) -> None:
    async def terminal(
        context: PipelineContext,  # noqa: ARG001
        next_stage: Callable[[PipelineContext], Awaitable[int]],  # noqa: ARG001
    ) -> int:
        return 42

    pipe = Pipeline[int]([terminal])
    assert await pipe.run(ctx) == 42


async def test_stages_run_in_order_and_can_transform(ctx: PipelineContext) -> None:
    log: list[str] = []

    async def outer(
        context: PipelineContext,
        next_stage: Callable[[PipelineContext], Awaitable[int]],
    ) -> int:
        log.append("outer-before")
        value = await next_stage(context)
        log.append("outer-after")
        return value + 1

    async def inner(
        context: PipelineContext,  # noqa: ARG001
        next_stage: Callable[[PipelineContext], Awaitable[int]],  # noqa: ARG001
    ) -> int:
        log.append("inner")
        return 10

    pipe = Pipeline[int]([outer, inner])
    result = await pipe.run(ctx)

    assert result == 11
    assert log == ["outer-before", "inner", "outer-after"]


async def test_stage_can_short_circuit(ctx: PipelineContext) -> None:
    async def short(
        context: PipelineContext,  # noqa: ARG001
        next_stage: Callable[[PipelineContext], Awaitable[int]],  # noqa: ARG001
    ) -> int:
        return -1

    async def never_reached(
        context: PipelineContext,  # noqa: ARG001
        next_stage: Callable[[PipelineContext], Awaitable[int]],  # noqa: ARG001
    ) -> int:
        raise AssertionError("should not run")

    pipe = Pipeline[int]([short, never_reached])
    assert await pipe.run(ctx) == -1


def test_empty_pipeline_rejected() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        Pipeline[int]([])


async def test_terminal_call_to_next_stage_raises(ctx: PipelineContext) -> None:
    async def bad(
        context: PipelineContext,
        next_stage: Callable[[PipelineContext], Awaitable[int]],
    ) -> int:
        return await next_stage(context)

    pipe = Pipeline[int]([bad])
    with pytest.raises(RuntimeError, match="terminal stage"):
        await pipe.run(ctx)
