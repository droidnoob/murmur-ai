"""Unit tests for ``murmur.runs`` — value types + ``InMemoryRunStore``."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from murmur.core.errors import RegistryError
from murmur.runs import (
    InMemoryRunStore,
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
)
from murmur.types import AgentResult, ResultMetadata


class _Out(BaseModel):
    text: str


def _ok(name: str, task_id: str) -> AgentResult[BaseModel]:
    return AgentResult[BaseModel](
        output=_Out(text="ok"),
        metadata=ResultMetadata(backend="thread"),
        agent_name=name,
        task_id=task_id,
    )


async def test_create_then_status_returns_pending() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="agent-x")
    status = await store.get_status(run_id)
    assert status.state is RunState.PENDING
    assert status.run_id == run_id


async def test_set_state_progresses_to_completed() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="x")
    await store.set_state(run_id, RunState.RUNNING)
    await store.set_state(run_id, RunState.COMPLETED)
    status = await store.get_status(run_id)
    assert status.state is RunState.COMPLETED


async def test_get_result_returns_set_result() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="x")
    await store.set_result(run_id, _ok("x", run_id))
    result = await store.get_result(run_id)
    assert result is not None
    assert result.is_ok()


async def test_unknown_run_raises_registry_error() -> None:
    store = InMemoryRunStore()
    with pytest.raises(RegistryError, match="not found"):
        await store.get_status("ghost")


async def test_progress_round_trip() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="x")
    await store.update_progress(
        run_id, RunProgress(total=10, completed=3, failed=1, running=6)
    )
    status = await store.get_status(run_id)
    assert status.progress is not None
    assert status.progress.completed == 3


async def test_stream_replays_buffered_events() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="x")
    await store.push_event(
        run_id, RunEvent(type=RunEventType.AGENT_STARTED, run_id=run_id)
    )
    await store.push_event(
        run_id, RunEvent(type=RunEventType.AGENT_COMPLETED, run_id=run_id)
    )
    await store.set_state(run_id, RunState.COMPLETED)

    seen = [ev async for ev in store.stream(run_id)]
    assert [e.type for e in seen] == [
        RunEventType.AGENT_STARTED,
        RunEventType.AGENT_COMPLETED,
    ]


async def test_stream_live_events_close_on_terminal_state() -> None:
    store = InMemoryRunStore()
    run_id = store.new_run_id()
    await store.create(run_id, target="x")

    async def producer() -> None:
        await asyncio.sleep(0)
        await store.push_event(
            run_id, RunEvent(type=RunEventType.AGENT_STARTED, run_id=run_id)
        )
        await store.push_event(
            run_id, RunEvent(type=RunEventType.GROUP_COMPLETED, run_id=run_id)
        )
        await store.set_state(run_id, RunState.COMPLETED)

    asyncio.create_task(producer())
    seen: list[RunEvent] = []
    async for ev in store.stream(run_id):
        seen.append(ev)
    types = [e.type for e in seen]
    assert RunEventType.AGENT_STARTED in types
    assert RunEventType.GROUP_COMPLETED in types
