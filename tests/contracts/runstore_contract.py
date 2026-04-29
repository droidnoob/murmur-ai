"""Shared contract suite for the ``murmur.runs.RunStore`` Protocol.

Subclass :class:`RunStoreContract` and override the ``store`` fixture for
each concrete implementation. Every concrete (``InMemoryRunStore``,
``SQLiteRunStore``, ``RocksDBRunStore``, ``RedisRunStore``) is expected
to pass the same suite — that is what keeps their behaviour aligned
under the Protocol.

The fixture must be **async** so concretes that need async setup (e.g.
opening an aiosqlite connection) can do it. Tests that do not need
the agent / task fixtures can declare only ``store``.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from pydantic import BaseModel

from murmur.core.errors import RegistryError
from murmur.runs import (
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
    RunStore,
)
from murmur.types import AgentResult, ResultMetadata


class _Out(BaseModel):
    text: str


def _ok_result(name: str, task_id: str) -> AgentResult[BaseModel]:
    return AgentResult[BaseModel](
        output=_Out(text="ok"),
        metadata=ResultMetadata(backend="thread"),
        agent_name=name,
        task_id=task_id,
    )


class RunStoreContract:
    """Behavioural contract every ``RunStore`` must satisfy."""

    @pytest.fixture
    async def store(self) -> RunStore:
        raise NotImplementedError(
            "subclass must override `store` fixture with a concrete instance"
        )

    @pytest.fixture
    def run_id(self) -> str:
        return str(uuid.uuid4())

    # ---- create / get_status -----------------------------------------------

    async def test_create_then_status_returns_pending(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="agent-x")
        status = await store.get_status(run_id)
        assert status.run_id == run_id
        assert status.state is RunState.PENDING

    async def test_get_status_unknown_raises_registry_error(
        self, store: RunStore
    ) -> None:
        with pytest.raises(RegistryError):
            await store.get_status("ghost")

    async def test_create_duplicate_raises_registry_error(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        with pytest.raises(RegistryError):
            await store.create(run_id, target="y")

    # ---- state transitions -------------------------------------------------

    async def test_set_state_progresses_through_lifecycle(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        await store.set_state(run_id, RunState.RUNNING)
        assert (await store.get_status(run_id)).state is RunState.RUNNING
        await store.set_state(run_id, RunState.COMPLETED)
        assert (await store.get_status(run_id)).state is RunState.COMPLETED

    async def test_set_state_supports_failed_terminal(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        await store.set_state(run_id, RunState.RUNNING)
        await store.set_state(run_id, RunState.FAILED)
        assert (await store.get_status(run_id)).state is RunState.FAILED

    # ---- progress ----------------------------------------------------------

    async def test_progress_round_trip_is_monotonic(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        prior = -1
        for completed in (0, 1, 5, 10):
            await store.update_progress(
                run_id,
                RunProgress(
                    total=10,
                    completed=completed,
                    failed=0,
                    running=10 - completed,
                ),
            )
            status = await store.get_status(run_id)
            assert status.progress is not None
            assert status.progress.completed == completed
            assert status.progress.completed > prior
            prior = status.progress.completed

    # ---- result ------------------------------------------------------------

    async def test_set_result_get_result_round_trip(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        result = _ok_result("x", run_id)
        await store.set_result(run_id, result)
        round_trip = await store.get_result(run_id)
        assert round_trip is not None
        assert round_trip.is_ok()
        assert round_trip.agent_name == "x"

    async def test_get_result_before_set_returns_none(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        assert await store.get_result(run_id) is None

    # ---- events / streaming ------------------------------------------------

    async def test_push_event_replays_in_order_after_terminal(
        self, store: RunStore, run_id: str
    ) -> None:
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

    async def test_stream_live_events_close_on_terminal_state(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")

        async def producer() -> None:
            await asyncio.sleep(0)
            await store.push_event(
                run_id,
                RunEvent(type=RunEventType.AGENT_STARTED, run_id=run_id),
            )
            await store.push_event(
                run_id,
                RunEvent(type=RunEventType.GROUP_COMPLETED, run_id=run_id),
            )
            await store.set_state(run_id, RunState.COMPLETED)

        producer_task = asyncio.create_task(producer())
        seen: list[RunEvent] = []
        async for ev in store.stream(run_id):
            seen.append(ev)
        await producer_task

        types = [e.type for e in seen]
        assert RunEventType.AGENT_STARTED in types
        assert RunEventType.GROUP_COMPLETED in types

    # ---- concurrency -------------------------------------------------------

    async def test_concurrent_create_distinct_run_ids(self, store: RunStore) -> None:
        ids = [str(uuid.uuid4()) for _ in range(10)]
        await asyncio.gather(*(store.create(rid, target="x") for rid in ids))
        for rid in ids:
            status = await store.get_status(rid)
            assert status.state is RunState.PENDING

    # ---- cancellation ------------------------------------------------------

    async def test_cancellation_records_state_and_event(
        self, store: RunStore, run_id: str
    ) -> None:
        await store.create(run_id, target="x")
        await store.set_state(run_id, RunState.RUNNING)
        await store.push_event(
            run_id, RunEvent(type=RunEventType.RUN_CANCELLED, run_id=run_id)
        )
        await store.set_state(run_id, RunState.CANCELLED)

        status = await store.get_status(run_id)
        assert status.state is RunState.CANCELLED

        events = [ev async for ev in store.stream(run_id)]
        assert any(e.type is RunEventType.RUN_CANCELLED for e in events)
