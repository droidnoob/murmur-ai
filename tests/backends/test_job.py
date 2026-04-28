"""JobBackend — contract suite + lifecycle tests.

Exercises the backend over an :class:`InMemoryBroker` so no FastStream extra
or real broker is required. End-to-end dispatch (Worker consuming, JobBackend
publishing, ResultCollector routing replies) lives in
``tests/worker/test_worker.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel
from tests.contracts.backend_contract import BackendContract

from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.job import JobBackend
from murmur.core.errors import SpawnError


class _Out(BaseModel):
    text: str


class TestJobBackendContract(BackendContract):
    @pytest.fixture
    async def backend(self) -> AsyncIterator[JobBackend]:
        broker = InMemoryBroker()
        b = JobBackend(broker=broker, runtime_id="rt-test")
        try:
            yield b
        finally:
            await b.stop()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture
async def job_backend() -> AsyncIterator[JobBackend]:
    broker = InMemoryBroker()
    b = JobBackend(broker=broker, runtime_id="rt-test-2")
    try:
        yield b
    finally:
        await b.stop()


async def test_start_is_idempotent(job_backend: JobBackend) -> None:
    await job_backend.start()
    await job_backend.start()  # must not raise


async def test_status_unknown_handle_raises(job_backend: JobBackend) -> None:
    from murmur.types import AgentHandle

    bogus = AgentHandle(agent_name="x", task_id="y", backend="job")
    with pytest.raises(SpawnError, match="unknown handle"):
        await job_backend.status(bogus)


async def test_kill_unknown_handle_is_silent(job_backend: JobBackend) -> None:
    from murmur.types import AgentHandle

    bogus = AgentHandle(agent_name="x", task_id="y", backend="job")
    # Best-effort kill: silently ignored if the handle was never spawned here.
    await job_backend.kill(bogus)
