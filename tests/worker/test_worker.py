"""End-to-end: ``Worker`` + ``JobBackend`` + ``InMemoryBroker``.

Closes the loop: a publisher-side ``JobBackend`` publishes a ``TaskMessage``
onto an in-process broker; a ``Worker`` consumes it, dispatches via its
inner ``AsyncBackend`` runtime (with ``TestModel`` injected — no real LLM),
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
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker


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


def _make_worker_runtime() -> AgentRuntime:
    backend = AsyncBackend()
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


# ---------------------------------------------------------------------------
# Startup banner (bi8)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Distributed event bridge auto-wiring (tsr)
# ---------------------------------------------------------------------------


def test_worker_auto_installs_event_bridge_when_runtime_is_default(
    echo_agent: Agent,
) -> None:
    """``Worker(runtime=None)`` constructs an internal runtime with
    :class:`BrokerEventBridge` already in its emitter chain so per-task
    events relay back when a TaskMessage carries an ``events_topic``."""
    from murmur.events import BrokerEventBridge, MultiEventEmitter

    broker = InMemoryBroker()
    worker = Worker(broker=broker, agents={echo_agent.name: echo_agent})

    emitter = worker._runtime.event_emitter
    assert isinstance(emitter, MultiEventEmitter)
    bridges = [e for e in emitter.emitters if isinstance(e, BrokerEventBridge)]
    assert len(bridges) == 1


def test_worker_does_not_mutate_user_supplied_runtime(echo_agent: Agent) -> None:
    """User-supplied runtimes keep their emitter — Worker doesn't auto-wrap.

    Documented escape hatch: users wanting the bridge with a custom runtime
    must wire ``BrokerEventBridge`` into their emitter chain themselves."""
    broker = InMemoryBroker()
    user_runtime = _make_worker_runtime()
    original_emitter = user_runtime.event_emitter

    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=user_runtime,
    )
    assert worker._runtime.event_emitter is original_emitter


async def test_worker_binds_event_topic_contextvar_per_task(
    echo_agent: Agent,
) -> None:
    """The per-task ``events_topic`` from :class:`TaskMessage` flows into
    the contextvar so any :class:`BrokerEventBridge` in the chain knows
    where to publish; resets after the run so the next task starts clean.
    """
    import structlog.contextvars

    from murmur.events.broker import current_event_topic

    captured_topics: list[str | None] = []

    def grab_topic(payload: str) -> str:
        captured_topics.append(current_event_topic())
        # Also confirm structlog ctx still wired — proves we didn't break
        # the existing pattern when adding the contextvar bind.
        ctx = structlog.contextvars.get_contextvars()
        assert ctx.get("agent_name") == echo_agent.name
        return payload

    broker = InMemoryBroker()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-ctxvar",
        publish_events=True,
    )
    agent_with_hook = echo_agent.with_(pre_process=(grab_topic,))
    worker = Worker(
        broker=broker,
        agents={agent_with_hook.name: agent_with_hook},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        await publisher.run(agent_with_hook, TaskSpec(input="hi", request_id="r-1"))
    finally:
        await worker.stop()

    assert captured_topics == ["murmur.events.rt-ctxvar"]
    # And the contextvar is reset back outside the run.
    assert current_event_topic() is None


# ---------------------------------------------------------------------------
# Startup banner (bi8)
# ---------------------------------------------------------------------------


async def test_worker_start_writes_banner_with_subscriptions(
    echo_agent: Agent,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Multi-line banner shows broker repr, runtime id, and per-agent topics.

    Replaces FastStream's per-broker subscriber chatter with one Murmur-
    branded block. ``capsys`` captures stderr — structlog-based assertion
    avoided because some prior CLI tests call ``structlog.configure`` with
    ``cache_logger_on_first_use=True``, which makes ``capture_logs``
    test-order-dependent.
    """
    broker = InMemoryBroker()
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        captured = capsys.readouterr()
        assert "Murmur worker" in captured.err
        assert "memory://" in captured.err
        assert "echo" in captured.err
        assert "murmur.echo.tasks" in captured.err
    finally:
        await worker.stop()
