"""RedisRunStore — runs the shared :class:`RunStoreContract` suite.

Uses ``fakeredis`` with a shared :class:`fakeredis.FakeServer` so the
suite exercises real Redis semantics (XADD / XREAD streams + Lua
scripting) without needing Docker. The multi-instance test below sends
two ``RedisRunStore`` instances at the same fake server and verifies
writes from one are visible to the other.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis, FakeServer
from tests.contracts.runstore_contract import RunStoreContract

from murmur.runs import (
    RedisRunStore,
    RunEvent,
    RunEventType,
    RunState,
    RunStore,
)


def _make_client(server: FakeServer) -> FakeAsyncRedis:
    return FakeAsyncRedis(server=server)


class TestRedisRunStore(RunStoreContract):
    @pytest.fixture
    async def store(self) -> AsyncIterator[RunStore]:
        server = FakeServer()
        client = _make_client(server)
        s = RedisRunStore(client=client)
        try:
            yield s
        finally:
            await s.close()
            await client.aclose()


async def test_multi_instance_writes_are_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two RedisRunStores against one server share state."""
    server = FakeServer()
    writer = RedisRunStore(client=_make_client(server))
    reader = RedisRunStore(client=_make_client(server))

    await writer.create("multi", target="x")
    await reader.set_state("multi", RunState.RUNNING)
    status_from_writer = await writer.get_status("multi")
    assert status_from_writer.state is RunState.RUNNING

    await writer.push_event(
        "multi", RunEvent(type=RunEventType.AGENT_STARTED, run_id="multi")
    )
    await writer.set_state("multi", RunState.COMPLETED)

    seen = [ev async for ev in reader.stream("multi")]
    assert any(e.type is RunEventType.AGENT_STARTED for e in seen)


async def test_terminal_state_is_monotonic() -> None:
    """Once terminal, the Lua script rejects a regression to RUNNING."""
    server = FakeServer()
    store = RedisRunStore(client=_make_client(server))

    await store.create("mono", target="x")
    await store.set_state("mono", RunState.COMPLETED)
    # Regression attempt is silently swallowed — state remains COMPLETED.
    await store.set_state("mono", RunState.RUNNING)
    status = await store.get_status("mono")
    assert status.state is RunState.COMPLETED


async def test_url_or_client_required() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        RedisRunStore()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="exactly one"):
        RedisRunStore(url="redis://x", client=FakeAsyncRedis())


async def test_lazy_attr_access() -> None:
    from murmur import runs

    server = FakeServer()
    store = runs.RedisRunStore(client=_make_client(server))
    await store.create("a", target="x")
    assert (await store.get_status("a")).state is RunState.PENDING


# Keep ``asyncio`` referenced; some test environments mark it as unused.
_ = asyncio
