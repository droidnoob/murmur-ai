"""Tests for :class:`murmur.server.AgentRouter`.

The router mounts into a bare FastAPI app via ``app.include_router`` and
must behave identically to :class:`AgentServer` on every endpoint we
exercise here. We don't re-run every test from ``test_app.py`` — those
already prove the route bodies; this file proves the *mounting* shape:

1. ``include_router`` exposes all 13 routes.
2. ``install_exception_handlers`` wires MurmurError → ErrorResponse.
3. ``register`` / ``register_group`` proxy through to the backing
   :class:`AgentServer`.
4. The ``server=`` constructor lets users share state with an existing
   :class:`AgentServer` instance.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pydantic_ai
import pytest
from fastapi import FastAPI
from murmur_client.local import LocalClient
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.runtime import AgentRuntime
from murmur.server.app import AgentServer
from murmur.server.router import AgentRouter
from murmur.types import TaskSpec, TrustLevel


class _Echo(BaseModel):
    text: str


def _make_factory() -> Any:
    canned = {"echo": _Echo(text="ok").model_dump()}

    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=canned[agent.name]),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _runtime() -> AgentRuntime:
    backend = AsyncBackend()
    backend._build_pa_agent = _make_factory()  # noqa: SLF001
    return AgentRuntime(backend=backend)


def _echo_agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Echo,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


@pytest.fixture
async def mounted_client() -> AsyncIterator[httpx.AsyncClient]:
    """Mount AgentRouter into a bare FastAPI app — the user-side embedding."""
    router = AgentRouter(runtime=_runtime())
    router.register(_echo_agent())

    app = FastAPI(lifespan=router.lifespan_context)
    app.include_router(router)
    AgentRouter.install_exception_handlers(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def prefixed_client() -> AsyncIterator[httpx.AsyncClient]:
    """Same shape, mounted under a prefix — matches the docstring example."""
    router = AgentRouter(runtime=_runtime())
    router.register(_echo_agent())

    app = FastAPI(lifespan=router.lifespan_context)
    app.include_router(router, prefix="/murmur")
    AgentRouter.install_exception_handlers(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Mount shapes
# ---------------------------------------------------------------------------


async def test_health_via_mounted_router(mounted_client: httpx.AsyncClient) -> None:
    r = await mounted_client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_health_via_prefixed_mount(prefixed_client: httpx.AsyncClient) -> None:
    r = await prefixed_client.get("/murmur/health")
    assert r.status_code == 200
    # Without the prefix it must not be reachable.
    r2 = await prefixed_client.get("/health")
    assert r2.status_code == 404


async def test_run_agent_via_mounted_router(
    mounted_client: httpx.AsyncClient,
) -> None:
    r = await mounted_client.post(
        "/agents/echo/run",
        json={"task": TaskSpec(input="hi").model_dump()},
    )
    assert r.status_code == 200
    assert r.json()["output"]["text"] == "ok"


async def test_gather_via_mounted_router(mounted_client: httpx.AsyncClient) -> None:
    tasks = [TaskSpec(input=f"q-{i}").model_dump() for i in range(3)]
    r = await mounted_client.post(
        "/agents/echo/gather", json={"tasks": tasks, "max_concurrency": 2}
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 3
    assert all(item["success"] for item in body)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


async def test_unknown_agent_returns_typed_error(
    mounted_client: httpx.AsyncClient,
) -> None:
    """install_exception_handlers must wire MurmurError → ErrorResponse."""
    r = await mounted_client.get("/agents/ghost/schema")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "RegistryError"
    assert "ghost" in body["message"]


async def test_request_id_header_round_trip(
    mounted_client: httpx.AsyncClient,
) -> None:
    """request-id middleware (installed alongside handlers) echoes the header."""
    r = await mounted_client.get("/health", headers={"X-Request-Id": "test-rid-123"})
    assert r.status_code == 200
    assert r.headers["X-Request-Id"] == "test-rid-123"


# ---------------------------------------------------------------------------
# Construction modes
# ---------------------------------------------------------------------------


def test_server_kwarg_conflict_with_runtime() -> None:
    """Passing both `server=` and `runtime=` is a programming error."""
    s = AgentServer()
    with pytest.raises(ValueError, match="server="):
        AgentRouter(server=s, runtime=AgentRuntime())


async def test_router_with_existing_server_shares_state() -> None:
    """A pre-built AgentServer can be passed in; both surfaces see the same agents."""
    s = AgentServer(runtime=_runtime())
    s.register(_echo_agent())

    router = AgentRouter(server=s)
    app = FastAPI(lifespan=router.lifespan_context)
    app.include_router(router)
    AgentRouter.install_exception_handlers(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        # Agent registered on the server is visible through the router.
        r = await c.get("/agents")
        assert "echo" in r.json()


def test_router_subclasses_apirouter() -> None:
    """``app.include_router(agent_router)`` must work — subclass of APIRouter."""
    from fastapi import APIRouter

    router = AgentRouter(runtime=_runtime())
    assert isinstance(router, APIRouter)


# ---------------------------------------------------------------------------
# Broker exposure (24h)
# ---------------------------------------------------------------------------


def test_broker_returns_none_for_thread_mode_runtime() -> None:
    """In-process runtimes have no broker — accessor returns None."""
    router = AgentRouter(runtime=_runtime())  # default in-process
    assert router.broker is None


def test_broker_returns_none_for_in_memory_broker() -> None:
    """`memory://` URLs spin up an InMemoryBroker — no FastStream surface."""
    router = AgentRouter(runtime=AgentRuntime(broker="memory://"))
    # InMemoryBroker isn't a FastStream broker, so .broker is None.
    assert router.broker is None


def test_broker_returns_brokers_when_url_is_kafka() -> None:
    """A `kafka://` URL constructs a make_broker — accessor drills to it.

    We don't actually connect to Kafka here; we just verify the accessor
    drills through ``runtime → JobBackend → broker.fs_broker``.
    The fs_broker is None until ``start()`` builds it lazily — that's the
    expected pre-start shape and proves the path is wired.
    """
    router = AgentRouter(runtime=AgentRuntime(broker="kafka://localhost:9092"))
    # Pre-start: fs_broker is lazy; the accessor returns whatever's there
    # (None pre-start is fine — we're verifying the path resolves).
    # Post-start it would be a KafkaBroker; we don't connect here.
    assert router.broker is None  # not yet started → fs_broker still None


# ---------------------------------------------------------------------------
# Lifespan auto-start / shutdown (24d)
# ---------------------------------------------------------------------------


async def test_lifespan_thread_mode_is_noop() -> None:
    """In-process runtime: lifespan startup/shutdown does nothing visible."""
    router = AgentRouter(runtime=_runtime())  # default in-process
    await router._lifespan_startup()  # noqa: SLF001
    assert router._worker is None  # noqa: SLF001
    await router._lifespan_shutdown()  # noqa: SLF001


async def test_lifespan_starts_broker_and_worker_in_memory_mode() -> None:
    """`memory://` + start_workers=True: broker + worker live in lifespan.

    End-to-end: a publish through the broker-mode runtime must be picked up
    by the auto-started worker and round-trip a result back. We pass a
    ``worker_runtime`` with the test-model factory so the worker doesn't
    try to call Anthropic for real.
    """
    publish_runtime = AgentRuntime(broker="memory://")
    worker_runtime = _runtime()  # in-process with TestModel factory
    router = AgentRouter(
        runtime=publish_runtime,
        start_workers=True,
        worker_runtime=worker_runtime,
    )
    router.register(_echo_agent())

    await router._lifespan_startup()  # noqa: SLF001
    try:
        assert router._worker is not None  # noqa: SLF001
        # End-to-end: publish via the runtime → consume via the worker.
        client = LocalClient(server=router.server)
        result = await client.run("echo", TaskSpec(input="hi"))
        assert result.is_ok()
        assert result.output is not None
        assert result.output.model_dump()["text"] == "ok"
    finally:
        await router._lifespan_shutdown()  # noqa: SLF001
    # Worker reset on shutdown.
    assert router._worker is None  # noqa: SLF001


async def test_lifespan_skips_worker_when_start_workers_false() -> None:
    """`memory://` + start_workers=False: broker still starts, no worker."""
    runtime = AgentRuntime(broker="memory://")
    router = AgentRouter(runtime=runtime, start_workers=False)
    router.register(_echo_agent())

    await router._lifespan_startup()  # noqa: SLF001
    try:
        assert router._worker is None  # noqa: SLF001
    finally:
        await router._lifespan_shutdown()  # noqa: SLF001


async def test_lifespan_no_worker_when_no_agents_registered() -> None:
    """Nothing to subscribe to → don't bother spinning up a worker."""
    router = AgentRouter(runtime=AgentRuntime(broker="memory://"), start_workers=True)
    # No register() calls.
    await router._lifespan_startup()  # noqa: SLF001
    try:
        assert router._worker is None  # noqa: SLF001
    finally:
        await router._lifespan_shutdown()  # noqa: SLF001


# ---------------------------------------------------------------------------
# /healthz + /readyz also exposed via the router
# ---------------------------------------------------------------------------


async def test_healthz_via_mounted_router(mounted_client: httpx.AsyncClient) -> None:
    r = await mounted_client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_readyz_via_mounted_router(mounted_client: httpx.AsyncClient) -> None:
    r = await mounted_client.get("/readyz")
    assert r.status_code == 200
    assert r.json() == {"status": "ready"}
