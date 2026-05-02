"""AsyncBackend — contract suite + lifecycle tests.

The contract suite (:class:`BackendContract`) verifies the Protocol surface.
Below it, lifecycle tests exercise spawn → result, kill, and the failure
path. ``pydantic_ai.models.test.TestModel`` is injected via a method override
on the backend so tests never reach a real LLM provider.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel
from tests.contracts.backend_contract import BackendContract

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.core.errors import SpawnError
from murmur.core.protocols.backend import BackendStatus
from murmur.types import AgentContext, TaskSpec, TrustLevel


class _Out(BaseModel):
    text: str


async def _stub_pa_agent(
    agent: Agent,
    _allowed: frozenset[str],
    _task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_backend() -> AsyncBackend:
    backend = AsyncBackend()
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return backend


async def _drain(backend: AsyncBackend) -> None:
    """Cancel + await every spawned task so pytest's loop teardown is quiet."""
    pending = [t for t in backend._tasks.values() if not t.done()]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


# ---------------------------------------------------------------------------
# Contract suite
# ---------------------------------------------------------------------------


class TestAsyncBackendContract(BackendContract):
    @pytest.fixture
    async def backend(self) -> AsyncIterator[AsyncBackend]:
        b = _make_backend()
        try:
            yield b
        finally:
            await _drain(b)


# ---------------------------------------------------------------------------
# Lifecycle / behaviour tests
# ---------------------------------------------------------------------------


@pytest.fixture
async def async_backend() -> AsyncIterator[AsyncBackend]:
    b = _make_backend()
    try:
        yield b
    finally:
        await _drain(b)


@pytest.fixture
def echo_agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel is injected
        instructions="Echo the input.",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


async def test_result_returns_typed_output(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    handle = await async_backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    result = await async_backend.result(handle)
    assert result.is_ok()
    assert isinstance(result.output, _Out)
    assert result.agent_name == echo_agent.name
    assert result.metadata.backend == "thread"
    assert result.metadata.duration_ms >= 0
    assert result.metadata.tokens_used > 0


async def test_status_completed_after_result(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    handle = await async_backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    await async_backend.result(handle)
    assert await async_backend.status(handle) is BackendStatus.COMPLETED


async def test_kill_before_result(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    handle = await async_backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    await async_backend.kill(handle)
    assert await async_backend.status(handle) is BackendStatus.KILLED
    result = await async_backend.result(handle)
    assert not result.is_ok()
    assert isinstance(result.error, SpawnError)


async def test_kill_is_idempotent(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    handle = await async_backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    await async_backend.kill(handle)
    await async_backend.kill(handle)  # must not raise
    assert await async_backend.status(handle) is BackendStatus.KILLED


async def test_failed_run_yields_failed_result(echo_agent: Agent) -> None:
    backend = AsyncBackend()

    async def boom(
        _agent: Agent,
        _allowed: frozenset[str],
        _task_id: str,
    ) -> pydantic_ai.Agent[None, Any]:
        raise RuntimeError("boom")

    backend._build_pa_agent = boom  # ty: ignore[invalid-assignment]  # test seam

    handle = await backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    result = await backend.result(handle)
    assert not result.is_ok()
    assert isinstance(result.error, SpawnError)
    assert "boom" in str(result.error)
    assert await backend.status(handle) is BackendStatus.FAILED


async def test_status_unknown_handle_raises(async_backend: AsyncBackend) -> None:
    from murmur.types import AgentHandle

    bogus = AgentHandle(agent_name="x", task_id="y", backend="thread")
    with pytest.raises(SpawnError, match="unknown handle"):
        await async_backend.status(bogus)


async def test_result_unknown_handle_raises(async_backend: AsyncBackend) -> None:
    from murmur.types import AgentHandle

    bogus = AgentHandle(agent_name="x", task_id="y", backend="thread")
    with pytest.raises(SpawnError, match="unknown handle"):
        await async_backend.result(bogus)


# ---------------------------------------------------------------------------
# Pre/post hook tests
# ---------------------------------------------------------------------------


def _strip(payload: str) -> str:
    return payload.strip()


def _exclaim(payload: str) -> str:
    return payload + "!"


def _wrap_output(out: _Out) -> _Out:
    return out.model_copy(update={"text": f"[{out.text}]"})


def _double_wrap_output(out: _Out) -> _Out:
    return out.model_copy(update={"text": f"<<{out.text}>>"})


async def test_pre_process_runs_in_order_on_string_input(
    async_backend: AsyncBackend,
) -> None:
    seen: list[str] = []

    def capture(payload: str) -> str:
        seen.append(payload)
        return payload

    agent = Agent(
        name="hookecho",
        model="anthropic:claude-sonnet-4-6",
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
        pre_process=(_strip, _exclaim, capture),
    )
    handle = await async_backend.spawn(agent, TaskSpec(input="  hi  "), AgentContext())
    result = await async_backend.result(handle)
    assert result.is_ok()
    assert seen == ["hi!"]  # capture sees post-strip post-exclaim


async def test_post_process_runs_in_order(
    async_backend: AsyncBackend,
) -> None:
    agent = Agent(
        name="postecho",
        model="anthropic:claude-sonnet-4-6",
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
        post_process=(_wrap_output, _double_wrap_output),
    )
    handle = await async_backend.spawn(agent, TaskSpec(input="hi"), AgentContext())
    result = await async_backend.result(handle)
    assert result.is_ok()
    assert isinstance(result.output, _Out)
    # TestModel emits 'a' for str outputs; wrapped then double-wrapped:
    assert result.output.text == "<<[a]>>"


async def test_no_hooks_is_identity(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    handle = await async_backend.spawn(echo_agent, TaskSpec(input="hi"), AgentContext())
    result = await async_backend.result(handle)
    assert result.is_ok()
    assert isinstance(result.output, _Out)


# ---------------------------------------------------------------------------
# request_id propagation
# ---------------------------------------------------------------------------


async def test_request_id_bound_to_structlog_during_run(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    import structlog.contextvars

    captured_request_ids: list[str | None] = []

    def grab(payload: str) -> str:
        ctx = structlog.contextvars.get_contextvars()
        captured_request_ids.append(ctx.get("request_id"))
        return payload

    agent = echo_agent.with_(pre_process=(grab,))
    handle = await async_backend.spawn(
        agent, TaskSpec(input="x", request_id="req-known-42"), AgentContext()
    )
    await async_backend.result(handle)
    assert captured_request_ids == ["req-known-42"]
    # Cleanup verified: outside the run, request_id is unbound.
    assert "request_id" not in structlog.contextvars.get_contextvars()


# ---------------------------------------------------------------------------
# gather
# ---------------------------------------------------------------------------


async def test_gather_returns_results_in_input_order(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    tasks = [TaskSpec(input=f"q-{i}") for i in range(5)]
    results = await async_backend.gather(echo_agent, tasks, max_concurrency=3)
    assert len(results) == 5
    assert all(r.is_ok() for r in results)
    assert [r.task_id for r in results] == [t.id for t in tasks]


async def test_gather_partial_failure_does_not_raise(echo_agent: Agent) -> None:
    backend = AsyncBackend()

    call_count = {"n": 0}

    async def maybe_boom(
        agent: Agent,
        _allowed: frozenset[str],
        _task_id: str,
    ) -> pydantic_ai.Agent[None, Any]:
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("boom")
        return pydantic_ai.Agent(
            model=TestModel(),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    backend._build_pa_agent = maybe_boom  # ty: ignore[invalid-assignment]

    tasks = [TaskSpec(input=f"q-{i}") for i in range(3)]
    results = await backend.gather(echo_agent, tasks, max_concurrency=1)
    assert len(results) == 3
    # call #2 (index 1) is the failing one; pool_size=1 keeps order deterministic
    assert results[0].is_ok()
    assert not results[1].is_ok()
    assert results[2].is_ok()


async def test_gather_empty_returns_empty(
    async_backend: AsyncBackend,
    echo_agent: Agent,
) -> None:
    results = await async_backend.gather(echo_agent, [], max_concurrency=10)
    assert results == []
