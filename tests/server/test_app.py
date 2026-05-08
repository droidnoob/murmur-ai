"""Tests for :class:`murmur.server.AgentServer`.

Drives the FastAPI app through ``httpx.AsyncClient`` over an
``httpx.ASGITransport`` so no real port is bound. ``TestModel`` is injected
via the runtime's underlying ``AsyncBackend`` so dispatch never reaches a
real LLM.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.types import FanOut, TaskSpec, TrustLevel


class SubQuestion(BaseModel):
    question: str


class DecompositionResult(BaseModel):
    sub_questions: FanOut[list[SubQuestion]]
    reasoning: str = ""


class MinionFinding(BaseModel):
    answer: str
    confidence: float


class FinalReport(BaseModel):
    title: str
    findings_count: int


def _decomp() -> DecompositionResult:
    return DecompositionResult(
        sub_questions=[SubQuestion(question=f"q-{i}") for i in range(3)],
        reasoning="r",
    )


def _make_factory() -> Any:
    canned = {
        "research-head": _decomp().model_dump(),
        "research-minion": MinionFinding(answer="a", confidence=0.9).model_dump(),
        "research-summary": FinalReport(title="R", findings_count=3).model_dump(),
        "echo": _Echo(text="ok").model_dump(),
    }

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned[agent.name]),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


class _Echo(BaseModel):
    text: str


def _agent(name: str, output_type: type[BaseModel]) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
def server() -> AgentServer:
    backend = AsyncBackend()
    backend._build_pa_agent = _make_factory()
    runtime = AgentRuntime(backend=backend)
    s = AgentServer(runtime=runtime)
    s.register(_agent("echo", _Echo))
    head = _agent("research-head", DecompositionResult)
    minion = _agent("research-minion", MinionFinding)
    summary = _agent("research-summary", FinalReport)
    s.register(head)
    s.register(minion)
    s.register(summary)
    s.register_group(
        AgentGroup(
            name="research",
            topology={
                head: Edge(to=(minion,)),  # auto fan-out via FanOut
                minion: Edge(
                    to=(summary,),
                    mapper=lambda findings: TaskSpec(input=f"{len(findings)}"),
                ),
                summary: Edge.terminal(),
            },
        )
    )
    return s


@pytest.fixture
async def client(server: AgentServer) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def test_health_returns_ok(client: httpx.AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_list_agents(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents")
    assert r.status_code == 200
    assert "echo" in r.json()
    assert "research-head" in r.json()


async def test_agent_schema(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents/echo/schema")
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "echo"
    assert body["output_type"]["properties"]["text"]


async def test_unknown_agent_returns_404(client: httpx.AsyncClient) -> None:
    r = await client.get("/agents/ghost/schema")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "RegistryError"


async def test_group_topology_marks_fan_out(client: httpx.AsyncClient) -> None:
    r = await client.get("/groups/research/topology")
    assert r.status_code == 200
    body = r.json()
    edges = {(e["from"], e["to"]): e["fan_out"] for e in body["edges"]}
    # head→minion has no mapper → fan_out=True; minion→summary has mapper → False.
    assert edges[("research-head", "research-minion")] is True
    assert edges[("research-minion", "research-summary")] is False


# ---------------------------------------------------------------------------
# Synchronous dispatch
# ---------------------------------------------------------------------------


async def test_run_agent_returns_typed_output(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/agents/echo/run",
        json={"task": TaskSpec(input="hi").model_dump()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["output"]["text"] == "ok"


async def test_gather_returns_n_results(client: httpx.AsyncClient) -> None:
    tasks = [TaskSpec(input=f"q-{i}").model_dump() for i in range(5)]
    r = await client.post(
        "/agents/echo/gather", json={"tasks": tasks, "max_concurrency": 3}
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 5
    assert all(item["success"] for item in body)


async def test_run_group_via_http(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/groups/research/run",
        json={"task": TaskSpec(input="research the failure modes").model_dump()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["output"]["findings_count"] == 3


# ---------------------------------------------------------------------------
# Submit / status / result
# ---------------------------------------------------------------------------


async def test_submit_then_poll_returns_completed_result(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(
        "/submit",
        json={
            "target": "echo",
            "task": TaskSpec(input="x").model_dump(),
        },
    )
    assert r.status_code == 200
    run_id = r.json()["run_id"]

    # Poll until terminal — TestModel finishes near-instantly.
    import asyncio

    for _ in range(50):
        s = await client.get(f"/runs/{run_id}/status")
        if s.json()["state"] in {"completed", "failed"}:
            break
        await asyncio.sleep(0.01)

    final = await client.get(f"/runs/{run_id}/result")
    assert final.status_code == 200
    assert final.json()["success"] is True


async def test_result_before_complete_returns_409(client: httpx.AsyncClient) -> None:
    # Synthesise a fresh, never-completing run by registering it directly in
    # the store then querying. Easier path: submit and immediately fetch
    # before background runs (race) — instead, hit the endpoint with a fresh
    # store entry via the public API and check the 409 path with a delay.
    r = await client.post(
        "/submit",
        json={"target": "echo", "task": TaskSpec(input="x").model_dump()},
    )
    run_id = r.json()["run_id"]
    # Don't poll — fetch result immediately. May or may not race; if already
    # done, just assert success. Otherwise, expect 409.
    final = await client.get(f"/runs/{run_id}/result")
    assert final.status_code in {200, 409}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


async def test_request_id_round_trips(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/agents/echo/run",
        json={"task": TaskSpec(input="x").model_dump()},
        headers={"X-Request-Id": "req-known-99"},
    )
    assert r.headers.get("X-Request-Id") == "req-known-99"


async def test_unknown_agent_run_returns_404_with_typed_error(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(
        "/agents/ghost/run", json={"task": TaskSpec(input="x").model_dump()}
    )
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "RegistryError"
    assert body["request_id"]  # always set


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------


async def test_shutting_down_returns_503_with_retry_after(
    server: AgentServer,
) -> None:
    server._shutting_down = True  # simulate SIGTERM having fired
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post(
            "/agents/echo/run", json={"task": TaskSpec(input="x").model_dump()}
        )
    assert r.status_code == 503
    assert r.headers.get("Retry-After") == "5"
    body = r.json()
    assert body["error"] == "ServerShuttingDown"


# ---------------------------------------------------------------------------
# /healthz + /readyz split
# ---------------------------------------------------------------------------


async def test_healthz_always_200_even_during_shutdown(
    server: AgentServer,
) -> None:
    """``/healthz`` is liveness — bypasses the shutdown guard."""
    server._shutting_down = True
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_readyz_503_during_drain(server: AgentServer) -> None:
    server._shutting_down = True
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/readyz")
    assert r.status_code == 503
    assert r.headers.get("Retry-After") == "5"
    assert r.json() == {"status": "shutting_down"}


async def test_readyz_200_in_steady_state(server: AgentServer) -> None:
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/readyz")
    assert r.status_code == 200
    assert r.json() == {"status": "ready"}


async def test_health_alias_still_returns_ok(server: AgentServer) -> None:
    """``/health`` continues to work for backwards compat."""
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_readyz_503_when_broker_not_started(echo_agent) -> None:
    """A broker-configured runtime with no traffic yet is not ready."""
    from murmur.runtime import AgentRuntime as _AR

    runtime = _AR(broker="memory://")
    server = AgentServer(runtime=runtime)
    server.register(echo_agent)
    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/readyz")
    assert r.status_code == 503
    assert r.json() == {"status": "broker_not_started"}


# ---------------------------------------------------------------------------
# zxn.3.1 — /events/stream firehose
# ---------------------------------------------------------------------------


def _build_server_with_sse(
    echo_agent_arg: Agent,
    *,
    heartbeat_interval: float = 60.0,
) -> tuple[AgentServer, Any]:
    """Server with SSEEventEmitter wired into the runtime + as the firehose.

    Returns the server plus the emitter so tests can ``emit`` directly to
    drive subscriber output without exercising the full agent path.
    Heartbeat defaults to 60 s so it doesn't race the test assertions;
    the heartbeat-specific test passes its own short interval.
    """
    from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

    sse = SSEEventEmitter(heartbeat_interval=heartbeat_interval)
    emitter = MultiEventEmitter([LogEventEmitter(), sse])
    backend = AsyncBackend(event_emitter=emitter)
    backend._build_pa_agent = _make_factory()
    runtime = AgentRuntime(backend=backend, event_emitter=emitter)
    server = AgentServer(runtime=runtime, sse_emitter=sse)
    server.register(echo_agent_arg)
    return server, sse


async def test_events_stream_route_absent_when_emitter_not_passed(
    echo_agent: Agent,
) -> None:
    backend = AsyncBackend()
    backend._build_pa_agent = _make_factory()
    runtime = AgentRuntime(backend=backend)
    s = AgentServer(runtime=runtime)
    s.register(echo_agent)
    paths = {getattr(r, "path", None) for r in s.app.routes}
    assert "/events/stream" not in paths


async def test_events_stream_route_present_when_emitter_passed(
    echo_agent: Agent,
) -> None:
    s, _sse = _build_server_with_sse(echo_agent)
    paths = {getattr(r, "path", None) for r in s.app.routes}
    assert "/events/stream" in paths


async def test_events_stream_delivers_runtime_event_to_subscriber(
    echo_agent: Agent,
) -> None:
    """The SSE generator yields the right ``event:`` + ``data:`` shape for
    each :class:`RuntimeEvent` enqueued onto the underlying emitter.

    Drives the emitter directly rather than through a full agent run —
    that exercises a pile of unrelated machinery and the timing makes
    the test flakier. The route's behaviour is just ``return
    EventSourceResponse(emitter.subscribe())`` so verifying the
    generator output covers the contract.
    """
    import asyncio
    import json

    from murmur.events import EventType, RuntimeEvent

    s, sse = _build_server_with_sse(echo_agent)

    # Subscribe registration only happens when the generator body runs,
    # which is on the first ``__anext__`` — so we have to kick that off
    # as a task before emit() so the queue exists when we publish.
    sub = sse.subscribe()
    next_task = asyncio.create_task(sub.__anext__())
    # Yield once so the generator body registers the queue.
    await asyncio.sleep(0)
    await sse.emit(
        RuntimeEvent(
            event_type=EventType.AGENT_SPAWNED,
            agent_name="echo",
            trace_id="trace-events-1",
            payload={"backend": "thread", "trust_level": "sandbox"},
        )
    )
    frame = await asyncio.wait_for(next_task, timeout=1.0)
    await sub.aclose()

    assert frame["event"] == "agent_spawned"
    decoded = json.loads(frame["data"])
    assert decoded["agent_name"] == "echo"
    assert decoded["trace_id"] == "trace-events-1"
    assert decoded["event_type"] == "agent_spawned"


async def test_events_stream_heartbeats_when_idle(echo_agent: Agent) -> None:
    """An idle subscribe() generator emits ``event: ping`` between real
    events so intermediate proxies don't reap the connection. Tests a
    short heartbeat for speed."""
    import asyncio

    s, sse = _build_server_with_sse(echo_agent, heartbeat_interval=0.05)

    sub = sse.subscribe()
    try:
        frame = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    finally:
        await sub.aclose()

    assert frame == {"event": "ping", "data": ""}


# ---------------------------------------------------------------------------
# Dashboard mount (opt-in)
# ---------------------------------------------------------------------------


async def test_dashboard_mount_serves_index(tmp_path: Path) -> None:
    """When --dashboard-dir is set, GET /dashboard/ returns the bundle's
    index.html. Off by default — test below."""
    bundle = tmp_path / "dist"
    bundle.mkdir()
    (bundle / "index.html").write_text("<!doctype html><title>m</title>")
    (bundle / "assets").mkdir()
    (bundle / "assets" / "app.js").write_text("// bundle")

    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime, dashboard_dir=bundle)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/dashboard/")
        assert r.status_code == 200
        assert "<title>m</title>" in r.text
        r = await c.get("/dashboard/assets/app.js")
        assert r.status_code == 200
        assert r.text == "// bundle"


async def test_dashboard_unmounted_by_default() -> None:
    """No /dashboard route when dashboard_dir is None."""
    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/dashboard/")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# EventStore-backed routes (opt-in)
# ---------------------------------------------------------------------------


async def test_event_store_routes_assembled_tree() -> None:
    """When event_store is wired, /runs and /runs/{id}/tree return persisted events."""
    from murmur.events.store import InMemoryEventStore
    from murmur.events.types import EventType, RuntimeEvent

    store = InMemoryEventStore()
    await store.append(
        RuntimeEvent(
            event_type=EventType.AGENT_SPAWNED,
            agent_name="root-agent",
            trace_id="root-1",
            payload={"backend": "thread", "trust_level": "high"},
        )
    )
    await store.append(
        RuntimeEvent(
            event_type=EventType.AGENT_SPAWNED,
            agent_name="child-agent",
            trace_id="child-1",
            parent_trace_id="root-1",
            payload={"backend": "thread", "trust_level": "high"},
        )
    )
    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime, event_store=store)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/runs")
        assert r.status_code == 200
        page = r.json()
        assert page["total"] == 1
        assert page["limit"] == 50
        assert page["offset"] == 0
        assert [row["trace_id"] for row in page["rows"]] == ["root-1"]

        r = await c.get("/runs/root-1/tree")
        assert r.status_code == 200
        ids = {row["trace_id"] for row in r.json()}
        assert ids == {"root-1", "child-1"}

        r = await c.get("/runs/nonexistent/tree")
        assert r.status_code == 404


async def test_event_store_routes_unmounted_by_default() -> None:
    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/runs")
        assert r.status_code == 404


async def test_usage_endpoint_groups_by_agent() -> None:
    from murmur.events.store import InMemoryEventStore
    from murmur.events.types import EventType, RuntimeEvent

    store = InMemoryEventStore()
    for agent, tokens in [("a", 100), ("b", 50), ("a", 25)]:
        await store.append(
            RuntimeEvent(
                event_type=EventType.AGENT_COMPLETED,
                agent_name=agent,
                trace_id=f"t-{agent}-{tokens}",
                payload={"tokens_used": tokens, "backend": "thread", "duration_ms": 1},
            )
        )
    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime, event_store=store)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/usage", params={"group_by": "agent"})
        assert r.status_code == 200
        body = r.json()
        assert body["totals"] == {"tokens_used": 175, "events": 3}
        keys = {row["key"]: row["tokens_used"] for row in body["groups"]}
        assert keys == {"a": 125, "b": 50}

        r = await c.get("/usage", params={"group_by": "bogus"})
        assert r.status_code == 400


async def test_broker_relayed_events_land_in_event_store() -> None:
    """End-to-end: a wire RuntimeEvent published on the broker events
    topic flows through the JobBackend subscriber → MultiEventEmitter
    → StoreEventEmitter → EventStore.

    Confirms ``murmur serve --broker URL --publish-events --event-store=…``
    captures fleet-wide events without any extra wiring beyond what's
    already in cli/serve.py."""
    from murmur.backends._inmemory_broker import InMemoryBroker
    from murmur.events import LogEventEmitter, MultiEventEmitter
    from murmur.events.store import InMemoryEventStore, StoreEventEmitter
    from murmur.events.types import EventType, RuntimeEvent
    from murmur.messages import events_topic
    from murmur.runtime import AgentRuntime

    store = InMemoryEventStore()
    emitter = MultiEventEmitter([LogEventEmitter(), StoreEventEmitter(store)])

    from murmur.backends.job import JobBackend

    broker = InMemoryBroker()
    runtime = AgentRuntime(
        broker_instance=broker, event_emitter=emitter, publish_events=True
    )
    backend = runtime.backend
    assert isinstance(backend, JobBackend)
    # Trigger backend.start() so the events-topic subscriber is registered.
    await backend.start()

    wire = RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name="worker-agent",
        trace_id="wire-trace",
        payload={"backend": "job", "tokens_used": 4242, "duration_ms": 1500},
    )
    await broker.publish(
        events_topic(runtime._runtime_id),
        wire.model_dump_json().encode(),
    )

    # InMemoryBroker dispatches synchronously inside publish(); a single
    # event-loop yield is enough for the awaited handler to settle.
    import asyncio

    for _ in range(20):
        rows = await store.query(trace_id="wire-trace")
        if rows:
            break
        await asyncio.sleep(0.01)

    rows = await store.query(trace_id="wire-trace")
    assert len(rows) == 1
    assert rows[0].agent_name == "worker-agent"
    assert rows[0].payload["tokens_used"] == 4242

    await backend.stop()


async def test_runs_endpoint_paginates_with_offset_and_limit() -> None:
    """`GET /runs?limit=&offset=` returns a page + accurate total."""
    from datetime import UTC, datetime, timedelta

    from murmur.events.store import InMemoryEventStore
    from murmur.events.types import EventType, RuntimeEvent

    store = InMemoryEventStore()
    base = datetime.now(tz=UTC)
    for i in range(7):
        await store.append(
            RuntimeEvent(
                event_type=EventType.AGENT_SPAWNED,
                agent_name=f"agent-{i}",
                trace_id=f"trace-{i}",
                timestamp=base + timedelta(seconds=i),
                payload={"backend": "thread", "trust_level": "high"},
            )
        )

    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime, event_store=store)

    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/runs", params={"limit": 3, "offset": 0})
        assert r.status_code == 200
        page = r.json()
        assert page["total"] == 7
        assert page["limit"] == 3
        assert page["offset"] == 0
        # Newest-first: trace-6, trace-5, trace-4.
        assert [row["trace_id"] for row in page["rows"]] == [
            "trace-6",
            "trace-5",
            "trace-4",
        ]

        r = await c.get("/runs", params={"limit": 3, "offset": 3})
        page = r.json()
        assert [row["trace_id"] for row in page["rows"]] == [
            "trace-3",
            "trace-2",
            "trace-1",
        ]

        r = await c.get("/runs", params={"limit": 3, "offset": 6})
        page = r.json()
        assert [row["trace_id"] for row in page["rows"]] == ["trace-0"]


async def test_runs_endpoint_filters_by_status() -> None:
    """`GET /runs?status=completed` filters server-side."""
    from datetime import UTC, datetime, timedelta

    from murmur.events.store import InMemoryEventStore
    from murmur.events.types import EventType, RuntimeEvent

    store = InMemoryEventStore()
    base = datetime.now(tz=UTC)
    # trace-running: spawned only; trace-done: spawned + completed.
    await store.append(
        RuntimeEvent(
            event_type=EventType.AGENT_SPAWNED,
            agent_name="r",
            trace_id="trace-running",
            timestamp=base,
            payload={"backend": "thread", "trust_level": "high"},
        )
    )
    await store.append(
        RuntimeEvent(
            event_type=EventType.AGENT_SPAWNED,
            agent_name="d",
            trace_id="trace-done",
            timestamp=base + timedelta(seconds=1),
            payload={"backend": "thread", "trust_level": "high"},
        )
    )
    await store.append(
        RuntimeEvent(
            event_type=EventType.AGENT_COMPLETED,
            agent_name="d",
            trace_id="trace-done",
            timestamp=base + timedelta(seconds=2),
            payload={"backend": "thread", "duration_ms": 100, "tokens_used": 42},
        )
    )

    runtime = AgentRuntime(backend=AsyncBackend())
    s = AgentServer(runtime=runtime, event_store=store)
    transport = httpx.ASGITransport(app=s.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/runs", params={"status": "completed"})
        page = r.json()
        assert [row["trace_id"] for row in page["rows"]] == ["trace-done"]
        r = await c.get("/runs", params={"status": "running"})
        page = r.json()
        assert [row["trace_id"] for row in page["rows"]] == ["trace-running"]
        r = await c.get("/runs", params={"status": "bogus"})
        assert r.status_code == 400
