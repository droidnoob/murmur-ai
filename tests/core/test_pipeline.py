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


# ---------------------------------------------------------------------------
# state freezing
# ---------------------------------------------------------------------------


def test_state_is_read_only_after_construction() -> None:
    """In-place mutation of ``state`` must fail loudly."""
    ctx = PipelineContext(task=TaskSpec(input="x"), agent_name="a", state={"k": "v"})
    with pytest.raises(TypeError):
        ctx.state["k2"] = "boom"  # ty: ignore[invalid-assignment]  # test point


def test_state_round_trips_through_model_copy() -> None:
    """The supported update path is ``model_copy(update={'state': {...}})``."""
    ctx = PipelineContext(task=TaskSpec(input="x"), agent_name="a", state={"k": 1})
    next_ctx = ctx.model_copy(update={"state": {**ctx.state, "k2": 2}})
    assert next_ctx.state["k"] == 1
    assert next_ctx.state["k2"] == 2
    # original is untouched
    assert "k2" not in ctx.state


def test_state_default_is_empty_and_read_only() -> None:
    ctx = PipelineContext(task=TaskSpec(input="x"), agent_name="a")
    assert dict(ctx.state) == {}
    with pytest.raises(TypeError):
        ctx.state["k"] = "boom"  # ty: ignore[invalid-assignment]  # test point
