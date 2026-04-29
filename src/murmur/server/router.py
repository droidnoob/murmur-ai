"""``AgentRouter`` — mount Murmur into a user-supplied FastAPI app.

The router wraps an :class:`AgentServer` for state (registry,
run-store, drain logic) and exposes Murmur's HTTP routes as a FastAPI
:class:`APIRouter` so users can do::

    from fastapi import FastAPI
    from murmur import AgentRuntime
    from murmur.server import AgentRouter

    agent_router = AgentRouter(runtime=AgentRuntime())
    agent_router.register(my_agent)

    app = FastAPI(lifespan=agent_router.lifespan_context)
    app.include_router(agent_router, prefix="/murmur")
    AgentRouter.install_exception_handlers(app)

The lifespan context handles drain on shutdown. The exception handlers map
:class:`MurmurError` / :class:`TimeoutError` to ``ErrorResponse`` JSON with
the right status codes — the same mapping :class:`AgentServer` installs on
its own embedded FastAPI app.

``self.broker`` exposes the underlying FastStream broker for "bring your
own subscriber" deployments. Pass a pre-configured broker to the runtime
via ``AgentRuntime(broker_instance=...)``.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import structlog
import structlog.contextvars
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from murmur.core.errors import MurmurError
from murmur.server.app import AgentServer
from murmur.server.errors import error_to_response, status_for

if TYPE_CHECKING:
    from murmur.agent import Agent
    from murmur.groups.spec import AgentGroup
    from murmur.runs import RunStore
    from murmur.runtime import AgentRuntime


log: structlog.stdlib.BoundLogger = structlog.get_logger()

_REQUEST_ID_HEADER = "X-Request-Id"


class AgentRouter(APIRouter):
    """A FastAPI ``APIRouter`` that exposes registered agents and groups.

    Constructable standalone or backed by a pre-built :class:`AgentServer`.
    Users typically pass a :class:`~murmur.AgentRuntime` and let the router
    construct the server internally; advanced users can build the server
    themselves and pass it in via ``server=`` for shared-state scenarios.
    """

    def __init__(
        self,
        *,
        runtime: AgentRuntime | None = None,
        run_store: RunStore | None = None,
        drain_timeout: float = 30.0,
        server: AgentServer | None = None,
        start_workers: bool = True,
        worker_runtime: AgentRuntime | None = None,
        worker_concurrency: int = 10,
        prefix: str = "",
        tags: list[str] | None = None,
    ) -> None:
        if server is not None and (runtime is not None or run_store is not None):
            raise ValueError(
                "pass either `server=` or "
                "(`runtime=`/`run_store=`/`drain_timeout=`) — not both"
            )

        self._server: AgentServer = server or AgentServer(
            runtime=runtime,
            run_store=run_store,
            drain_timeout=drain_timeout,
        )
        self._start_workers = start_workers
        # The worker's runtime MUST be thread-mode — broker-mode would
        # re-publish each consumed task and infinite-loop. ``Worker``
        # defaults to a fresh ``AgentRuntime()`` (thread-mode) when
        # ``runtime=None`` is forwarded; advanced users override via
        # ``worker_runtime=`` to inject custom tools / middleware /
        # test-model factories without disturbing the publishing runtime.
        self._worker_runtime: AgentRuntime | None = worker_runtime
        self._worker_concurrency = worker_concurrency
        self._worker: Any = None  # lazily built in lifespan when broker-mode

        # FastAPI's ``tags`` accepts ``list[str | Enum]``; we restrict the
        # public surface to ``list[str]`` for simplicity. ``None`` is the
        # default — only forward when the user actually provided one.
        router_kwargs: dict[str, Any] = {
            "prefix": prefix,
            "lifespan": self._lifespan_context,
        }
        if tags is not None:
            router_kwargs["tags"] = list(tags)
        super().__init__(**router_kwargs)

        for route in self._server._build_routes().routes:  # noqa: SLF001
            self.routes.append(route)

    # ------------------------------------------------------------------ state

    @property
    def server(self) -> AgentServer:
        """The backing :class:`AgentServer` — registry, run-store, drain."""
        return self._server

    def register(self, agent: Agent) -> None:
        """Register an agent. Mirrors :meth:`AgentServer.register`."""
        self._server.register(agent)

    def register_group(self, group: AgentGroup) -> None:
        """Register a group. Mirrors :meth:`AgentServer.register_group`."""
        self._server.register_group(group)

    # ------------------------------------------------------------------ broker

    @property
    def broker(self) -> Any:
        """The underlying FastStream broker (``KafkaBroker``, ``NatsBroker``,
        ``RabbitBroker``, ``RedisBroker``) when the runtime is broker-mode,
        else ``None``.

        Use this to register your own ``@broker.subscriber("user.events")``
        handlers next to Murmur's — they share the same connection and the
        same lifecycle (started / stopped via the host app's lifespan once
        24d lands).

        Treated as a documented re-export of FastStream's broker; consult
        the FastStream docs for its full API. Returns ``None`` when the
        runtime is thread-mode or when the broker is the in-memory testing
        broker (which has no FastStream surface).

        For "bring your own broker" (the user already has a configured
        FastStream broker in their app), pass it through to the runtime via
        the existing ``AgentRuntime(broker_instance=...)`` kwarg — wrap the
        FastStream broker in :class:`murmur.backends._faststream_broker.\
FastStreamBroker` (the ``_fs_broker=`` constructor seam).
        """
        # Lazy imports — keep module load lean and avoid circulars.
        from murmur.backends._faststream_broker import FastStreamBroker
        from murmur.backends.job import JobBackend

        runtime = self._server.runtime
        backend = runtime.backend
        if not isinstance(backend, JobBackend):
            return None
        inner_broker = getattr(backend, "broker", None)
        if isinstance(inner_broker, FastStreamBroker):
            return inner_broker.fs_broker
        return None

    # ------------------------------------------------------------------ lifespan

    @asynccontextmanager
    async def _lifespan_context(self, _app: Any) -> AsyncIterator[None]:
        """Lifespan: start broker + worker on entry, drain + stop on exit.

        Wired via ``super().__init__(lifespan=self._lifespan_context)`` —
        FastAPI's :class:`APIRouter` stores it as the public attribute
        ``self.lifespan_context``, so users access it as
        ``router.lifespan_context`` (no extra property on our side).

        - **Thread-mode runtime**: nothing to start; ``start_workers`` is a
          no-op.
        - **Broker-mode runtime**: starts the underlying broker so HTTP
          handlers can publish. If ``start_workers=True`` (default), also
          spins up an in-process :class:`murmur.worker.Worker` subscribed
          to all currently-registered agents. The worker's runtime is
          thread-mode (per the gotcha that broker-mode workers re-publish
          and loop) so its registry handler executes locally and publishes
          results back through the same broker.

        Workers MUST be registered (via :meth:`register`) before the host
        app's lifespan starts; agents added after startup won't be picked
        up by the running worker.
        """
        await self._lifespan_startup()
        try:
            yield
        finally:
            await self._lifespan_shutdown()

    async def _lifespan_startup(self) -> None:
        from murmur.backends.job import JobBackend
        from murmur.worker import Worker

        backend = self._server.runtime.backend
        if not isinstance(backend, JobBackend):
            # Thread-mode: nothing to start.
            return

        # Broker-mode: start the broker so /agents/{name}/run can publish.
        # ``JobBackend.start`` is idempotent — calling it eagerly here means
        # the first HTTP request doesn't pay the connection cost.
        await backend.start()

        if not self._start_workers:
            return

        agents = dict(self._server._agents)  # noqa: SLF001
        if not agents:
            return  # nothing to subscribe to

        self._worker = Worker(
            broker=backend.broker,
            agents=agents,
            runtime=self._worker_runtime,
            concurrency=self._worker_concurrency,
        )
        await self._worker.start()
        await log.ainfo(
            "agent_router_workers_started",
            agents=sorted(agents),
            concurrency=self._worker_concurrency,
        )

    async def _lifespan_shutdown(self) -> None:
        """Stop the worker, drain the server, then stop the broker.

        Order matters: stopping the worker first (which drains its in-flight
        tasks) avoids a race where the broker disconnects under it. Drain
        then runs through any /submit-spawned background runs the server
        started. Stopping the broker last ensures any final result publishes
        from the worker actually land.
        """
        from murmur.backends.job import JobBackend

        if self._worker is not None:
            await self._worker.stop()
            self._worker = None

        await self._server._drain()  # noqa: SLF001

        backend = self._server.runtime.backend
        if isinstance(backend, JobBackend):
            await backend.stop()

    # ------------------------------------------------------------------ handlers

    @classmethod
    def install_exception_handlers(cls, app: FastAPI) -> None:
        """Attach Murmur's error handlers + request-id middleware to ``app``.

        Idempotent on the per-handler level (FastAPI replaces handlers for the
        same exception type). Call once after :meth:`include_router`.
        """

        @app.middleware("http")
        async def _request_id_middleware(
            request: Request,
            call_next: Callable[[Request], Awaitable[Any]],
        ) -> Any:
            request_id = request.headers.get(_REQUEST_ID_HEADER) or str(uuid.uuid4())
            request.state.request_id = request_id
            structlog.contextvars.bind_contextvars(request_id=request_id)
            try:
                response = await call_next(request)
                response.headers[_REQUEST_ID_HEADER] = request_id
                return response
            finally:
                structlog.contextvars.unbind_contextvars("request_id")

        @app.exception_handler(MurmurError)
        async def _murmur_handler(request: Request, exc: MurmurError) -> JSONResponse:
            request_id = getattr(request.state, "request_id", "")
            return JSONResponse(
                status_code=status_for(exc),
                content=error_to_response(exc, request_id=request_id).model_dump(),
            )

        @app.exception_handler(TimeoutError)
        async def _timeout_handler(request: Request, exc: TimeoutError) -> JSONResponse:
            request_id = getattr(request.state, "request_id", "")
            return JSONResponse(
                status_code=504,
                content=error_to_response(exc, request_id=request_id).model_dump(),
            )

        # silence "defined but unused" — the decorators do the wiring
        _ = (_request_id_middleware, _murmur_handler, _timeout_handler)


__all__ = ["AgentRouter"]
