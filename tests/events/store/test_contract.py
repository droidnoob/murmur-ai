"""Shared contract tests for :class:`EventStore` concretes.

Both :class:`InMemoryEventStore` and :class:`SQLiteEventStore` run
through the same suite — appending, filtered queries, cascading-tree
walks, top-level trace listing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest

from murmur.core.protocols.event_store import EventStore
from murmur.events.store.memory import InMemoryEventStore
from murmur.events.store.sqlite import SQLiteEventStore
from murmur.events.types import EventType, RuntimeEvent


def _ev(
    *,
    trace: str,
    parent: str | None = None,
    agent: str = "code-reviewer",
    type_: EventType = EventType.AGENT_SPAWNED,
    ts: datetime | None = None,
    tokens: int | None = None,
) -> RuntimeEvent:
    payload: dict[str, object] = {"backend": "thread", "trust_level": "high"}
    if tokens is not None:
        payload["tokens_used"] = tokens
    return RuntimeEvent(
        event_type=type_,
        agent_name=agent,
        trace_id=trace,
        parent_trace_id=parent,
        timestamp=ts or datetime.now(tz=UTC),
        payload=payload,
    )


StoreFactory = Callable[[], Awaitable[EventStore]]


@pytest.fixture(params=["memory", "sqlite"])
async def store(request: pytest.FixtureRequest) -> AsyncIterator[EventStore]:
    """Parametrise every contract test over both concretes."""
    s: EventStore
    if request.param == "memory":
        s = InMemoryEventStore()
        yield s
    else:
        sqlite_store = SQLiteEventStore(path=":memory:")
        try:
            yield sqlite_store
        finally:
            await sqlite_store.close()


async def test_append_and_query_returns_event(store: EventStore) -> None:
    ev = _ev(trace="t1")
    await store.append(ev)
    rows = await store.query(trace_id="t1")
    assert len(rows) == 1
    assert rows[0].trace_id == "t1"
    assert rows[0].event_type == EventType.AGENT_SPAWNED


async def test_query_orders_newest_first(store: EventStore) -> None:
    base = datetime.now(tz=UTC)
    await store.append(_ev(trace="a", ts=base - timedelta(seconds=2)))
    await store.append(_ev(trace="b", ts=base - timedelta(seconds=1)))
    await store.append(_ev(trace="c", ts=base))
    rows = await store.query()
    assert [r.trace_id for r in rows[:3]] == ["c", "b", "a"]


async def test_query_respects_limit(store: EventStore) -> None:
    for i in range(5):
        await store.append(_ev(trace=f"t{i}"))
    rows = await store.query(limit=2)
    assert len(rows) == 2


async def test_query_filters_by_trace_id(store: EventStore) -> None:
    await store.append(_ev(trace="x"))
    await store.append(_ev(trace="y"))
    rows = await store.query(trace_id="y")
    assert {r.trace_id for r in rows} == {"y"}


async def test_query_filters_by_event_type(store: EventStore) -> None:
    await store.append(_ev(trace="t", type_=EventType.AGENT_SPAWNED))
    await store.append(_ev(trace="t", type_=EventType.AGENT_COMPLETED))
    await store.append(_ev(trace="t", type_=EventType.TOOL_CALL_STARTED))
    rows = await store.query(event_types=[EventType.TOOL_CALL_STARTED])
    assert len(rows) == 1
    assert rows[0].event_type == EventType.TOOL_CALL_STARTED


async def test_query_filters_by_time_window(store: EventStore) -> None:
    base = datetime.now(tz=UTC)
    await store.append(_ev(trace="old", ts=base - timedelta(hours=2)))
    await store.append(_ev(trace="new", ts=base))
    rows = await store.query(since=base - timedelta(minutes=5))
    assert {r.trace_id for r in rows} == {"new"}


async def test_tree_walks_cascading_descendants(store: EventStore) -> None:
    # root → child → grandchild plus a sibling that shares the root.
    await store.append(_ev(trace="root"))
    await store.append(_ev(trace="child-a", parent="root"))
    await store.append(_ev(trace="child-b", parent="root"))
    await store.append(_ev(trace="grand", parent="child-a"))
    await store.append(_ev(trace="unrelated"))

    tree = await store.tree("root")
    trace_ids = {r.trace_id for r in tree}
    assert trace_ids == {"root", "child-a", "child-b", "grand"}


async def test_tree_returns_oldest_first(store: EventStore) -> None:
    base = datetime.now(tz=UTC)
    await store.append(_ev(trace="root", ts=base))
    await store.append(
        _ev(trace="child", parent="root", ts=base + timedelta(seconds=1))
    )
    await store.append(
        _ev(
            trace="root",
            type_=EventType.AGENT_COMPLETED,
            ts=base + timedelta(seconds=2),
        )
    )
    tree = await store.tree("root")
    assert [r.timestamp for r in tree] == sorted(r.timestamp for r in tree)


async def test_list_traces_top_level_only(store: EventStore) -> None:
    base = datetime.now(tz=UTC)
    await store.append(_ev(trace="root-1", ts=base - timedelta(seconds=2)))
    await store.append(_ev(trace="root-2", ts=base - timedelta(seconds=1)))
    await store.append(_ev(trace="child", parent="root-1", ts=base))
    traces = await store.list_traces()
    # Children excluded; newest top-level first.
    assert traces == ["root-2", "root-1"]


async def test_list_traces_dedupes_per_trace(store: EventStore) -> None:
    base = datetime.now(tz=UTC)
    await store.append(_ev(trace="r", ts=base))
    await store.append(
        _ev(trace="r", type_=EventType.AGENT_COMPLETED, ts=base + timedelta(seconds=1))
    )
    traces = await store.list_traces()
    assert traces == ["r"]
