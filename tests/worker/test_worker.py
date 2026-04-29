"""End-to-end: ``Worker`` + ``JobBackend`` + ``InMemoryBroker``.

Closes the loop: a publisher-side ``JobBackend`` publishes a ``TaskMessage``
onto an in-process broker; a ``Worker`` consumes it, dispatches via its
inner ``ThreadBackend`` runtime (with ``TestModel`` injected — no real LLM),
and publishes a ``ResultMessage`` back. The original ``runtime.run`` /
``runtime.gather`` call resolves with the structured output.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker


class _Out(BaseModel):
    text: str


def _stub_pa_agent(
    agent: Agent,
    _allowed: frozenset[str],
    _task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_worker_runtime() -> AgentRuntime:
    backend = ThreadBackend()
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(backend=backend)


@pytest.fixture
def echo_agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel injected
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
async def wired(
    echo_agent: Agent,
) -> AsyncIterator[tuple[AgentRuntime, Worker]]:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-e2e")
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
        concurrency=4,
    )
    await worker.start()
    try:
        yield publisher, worker
    finally:
        await worker.stop()


async def test_run_round_trips_through_broker(
    wired: tuple[AgentRuntime, Worker],
    echo_agent: Agent,
) -> None:
    publisher, _ = wired
    result = await publisher.run(echo_agent, TaskSpec(input="hi"))
    assert result.is_ok()
    assert isinstance(result.output, _Out)
    assert result.task_id  # id propagated all the way back


async def test_gather_round_trips_through_broker(
    wired: tuple[AgentRuntime, Worker],
    echo_agent: Agent,
) -> None:
    publisher, _ = wired
    tasks = [TaskSpec(input=f"q-{i}") for i in range(5)]
    results = await publisher.gather(echo_agent, tasks)
    assert len(results) == 5
    assert all(r.is_ok() for r in results)
    # task_ids preserved across the round trip
    assert {r.task_id for r in results} == {t.id for t in tasks}


async def test_lifecycle_hooks_fire(
    echo_agent: Agent,
) -> None:
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-hooks")
    started: list[tuple[str, str]] = []
    completed: list[tuple[str, str, int]] = []

    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )

    @worker.on_task_start
    async def _on_start(task_id: str, agent_name: str) -> None:
        started.append((task_id, agent_name))

    @worker.on_task_complete
    async def _on_complete(task_id: str, agent_name: str, duration_ms: int) -> None:
        completed.append((task_id, agent_name, duration_ms))

    await worker.start()
    try:
        await publisher.run(echo_agent, TaskSpec(input="hi"))
    finally:
        await worker.stop()

    assert len(started) == 1
    assert len(completed) == 1
    assert started[0][1] == echo_agent.name
    assert completed[0][1] == echo_agent.name
    assert completed[0][2] >= 0


async def test_worker_rebinds_request_id_contextvar_from_task_message(
    echo_agent: Agent,
) -> None:
    """:class:`Worker` re-binds ``request_id`` from the :class:`TaskMessage`
    before invoking the inner runtime, so every structlog entry emitted
    server-side carries the same correlation id the publisher attached
    to the task."""
    import structlog.contextvars

    captured_request_ids: list[str | None] = []

    def grab_rid(payload: str) -> str:
        ctx = structlog.contextvars.get_contextvars()
        captured_request_ids.append(ctx.get("request_id"))
        return payload

    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-rid")
    agent_with_hook = echo_agent.with_(pre_process=(grab_rid,))

    worker = Worker(
        broker=broker,
        agents={agent_with_hook.name: agent_with_hook},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        await publisher.run(
            agent_with_hook, TaskSpec(input="hi", request_id="req-known-99")
        )
    finally:
        await worker.stop()

    # Worker re-bound the publisher's request_id before dispatching.
    assert captured_request_ids == ["req-known-99"]
