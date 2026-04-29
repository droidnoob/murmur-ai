"""``AgentServer`` — registers agents, exposes them over HTTP.

The server holds:

- a :class:`murmur.AgentRuntime` (configured for either local thread-mode or
  broker-mode, transparently to the user),
- a registry of agents and (optionally) :class:`murmur.AgentGroup` instances,
- an :class:`murmur.runs.InMemoryRunStore` for the submit/poll/stream pattern,
- a FastAPI app exposing the HTTP routes.

Synchronous routes (:meth:`runtime.run` / :meth:`runtime.gather`) for short
tasks; asynchronous ``POST /submit`` for long ones, with status polling and
SSE streaming. ``request_id`` propagates from the request headers (or a
freshly-minted UUID) through ``structlog.contextvars`` so every log line
during the request is correlated.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import uuid
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import structlog
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from murmur.core.errors import RegistryError
from murmur.runs import (
    InMemoryRunStore,
    RunEvent,
    RunEventType,
    RunState,
    RunStatus,
)
from murmur.server.errors import ErrorResponse
from murmur.types import AgentResult, TaskSpec

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.agent import Agent
    from murmur.groups.spec import AgentGroup
    from murmur.runs import RunStore
    from murmur.runtime import AgentRuntime


log: structlog.stdlib.BoundLogger = structlog.get_logger()

_REQUEST_ID_HEADER = "X-Request-Id"


class _RunRequest(BaseModel):
    task: TaskSpec
    request_id: str | None = None


class _GatherRequest(BaseModel):
    tasks: list[TaskSpec]
    max_concurrency: int = 100
    request_id: str | None = None


class _SubmitRequest(BaseModel):
    target: str
    """Agent name or group name to dispatch."""
    is_group: bool = False
    task: TaskSpec
    request_id: str | None = None


class AgentServer:
    """Registers agents / groups and serves them via FastAPI.

    >>> server = AgentServer()
    >>> server.register(my_agent)
    >>> server.register_group(my_crew)
    >>> await server.serve(port=8421)

    For tests, build the app via :meth:`app` and drive it with
    ``httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app))``.
    """

    def __init__(
        self,
        *,
        runtime: AgentRuntime | None = None,
        run_store: RunStore | None = None,
        drain_timeout: float = 30.0,
    ) -> None:
        from murmur.runtime import AgentRuntime as _AgentRuntime

        self._runtime: AgentRuntime = runtime or _AgentRuntime()
        self._run_store: RunStore = run_store or InMemoryRunStore()
        self._agents: dict[str, Agent] = {}
        self._groups: dict[str, AgentGroup] = {}
        self._drain_timeout = drain_timeout
        self._active_runs: set[str] = set()
        self._shutting_down: bool = False
        self._app: FastAPI = self._build_app()

    @property
    def app(self) -> FastAPI:
        return self._app

    @property
    def runtime(self) -> AgentRuntime:
        return self._runtime

    # ------------------------------------------------------------------ register

    def register(self, agent: Agent) -> None:
        """Register an agent under its ``agent.name``. Replaces by name."""
        self._agents[agent.name] = agent

    def register_group(self, group: AgentGroup) -> None:
        """Register a group under its ``group.name``."""
        # Auto-register the group's agents so /agents/{name} also works.
        for a in group.agents:
            self._agents.setdefault(a.name, a)
        self._groups[group.name] = group

    # ------------------------------------------------------------------ serve

    async def serve(self, port: int = 8421, host: str = "0.0.0.0") -> None:
        """Start the server. Handles SIGTERM / SIGINT for graceful shutdown."""
        import uvicorn

        config = uvicorn.Config(
            self._app, host=host, port=port, log_level="info", lifespan="on"
        )
        server = uvicorn.Server(config)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with _suppress_value_error():
                loop.add_signal_handler(sig, lambda: self._initiate_shutdown(server))
        await server.serve()

    # ------------------------------------------------------------------ private

    def _initiate_shutdown(self, server: object) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        # Mark the server for exit; uvicorn's Server.should_exit drives the loop.
        with contextlib.suppress(Exception):  # pragma: no cover — best effort
            setattr(server, "should_exit", True)  # noqa: B010

    def _build_app(self) -> FastAPI:
        # 24c — install the request-id middleware + MurmurError/TimeoutError
        # exception handlers via the shared :class:`AgentRouter` helper. The
        # standalone :class:`AgentServer` only adds the server-specific
        # 503-shutdown-guard middleware on top.
        from murmur.server.router import AgentRouter

        @asynccontextmanager
        async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
            yield
            await self._drain()

        app = FastAPI(lifespan=_lifespan)
        app.include_router(self._build_routes())
        AgentRouter.install_exception_handlers(app)

        # ---------- shutdown guard (server-only — not on the router) ----------
        # Order: middleware added LAST runs OUTERMOST in FastAPI. We want
        # the 503 short-circuit to run before the request-id middleware,
        # so it must be added after install_exception_handlers above.
        @app.middleware("http")
        async def _shutdown_guard(
            request: Request,
            call_next: Callable[[Request], Any],
        ) -> Any:
            # Liveness + readiness probes bypass the shutdown 503 so
            # orchestrators (k8s, ECS, ...) can still poll them while
            # the server is draining. ``/readyz`` reports its own 503
            # when ``_shutting_down`` is set; ``/healthz`` stays 200.
            if request.url.path in {"/healthz", "/readyz"}:
                return await cast("Any", call_next)(request)
            if self._shutting_down:
                request_id = request.headers.get(_REQUEST_ID_HEADER) or str(
                    uuid.uuid4()
                )
                return JSONResponse(
                    status_code=503,
                    content=ErrorResponse(
                        error="ServerShuttingDown",
                        message="Server is shutting down; retry another instance",
                        request_id=request_id,
                    ).model_dump(),
                    headers={"Retry-After": "5"},
                )
            return await cast("Any", call_next)(request)

        _ = _shutdown_guard  # decorator does the wiring
        return app

    def _build_routes(self) -> APIRouter:
        """All HTTP routes as an :class:`APIRouter`.

        Kept separate from :meth:`_build_app` so the router can be mounted
        into a user-supplied FastAPI app via ``app.include_router(...)``.
        Middleware and exception handlers stay on :meth:`_build_app`; the
        public :class:`AgentRouter` wrapper installs equivalent handlers
        on whatever app the user mounts the router into.
        """
        router = APIRouter()

        # ---------- discovery ----------

        @router.get("/healthz")
        async def healthz() -> dict[str, str]:
            """Liveness probe — always 200 once the process can serve."""
            return {"status": "ok"}

        @router.get("/readyz")
        async def readyz() -> JSONResponse:
            """Readiness probe — 503 during drain or before broker connect.

            Returns 503 when (a) the server has begun graceful shutdown
            or (b) the runtime's broker is configured but its
            :meth:`start` hasn't completed. Otherwise 200.
            """
            if self._shutting_down:
                return JSONResponse(
                    status_code=503,
                    content={"status": "shutting_down"},
                    headers={"Retry-After": "5"},
                )
            backend = self._runtime.backend
            if backend.__class__.__name__ == "JobBackend" and not getattr(
                backend, "started", True
            ):
                return JSONResponse(
                    status_code=503,
                    content={"status": "broker_not_started"},
                    headers={"Retry-After": "1"},
                )
            return JSONResponse(status_code=200, content={"status": "ready"})

        @router.get("/health")
        async def health() -> dict[str, str]:
            """Backwards-compat alias for ``/healthz``. New deployments
            should prefer the explicit ``/healthz`` + ``/readyz`` split."""
            return {"status": "ok"}

        @router.get("/agents")
        async def list_agents() -> list[str]:
            return sorted(self._agents)

        @router.get("/agents/{name}/schema")
        async def get_agent_schema(name: str) -> dict[str, object]:
            agent = self._require_agent(name)
            return {
                "name": agent.name,
                "input_type": (
                    agent.input_type.model_json_schema()
                    if agent.input_type is not None
                    else None
                ),
                "output_type": agent.output_type.model_json_schema(),
            }

        @router.get("/groups")
        async def list_groups() -> list[str]:
            return sorted(self._groups)

        @router.get("/groups/{name}/topology")
        async def get_group_topology(name: str) -> dict[str, object]:
            group = self._require_group(name)
            edges: list[dict[str, object]] = []
            for src in group.topology:
                for edge in group.outgoing_edges(src):
                    for tgt in edge.to:
                        edges.append(
                            {
                                "from": src.name,
                                "to": tgt.name,
                                "fan_out": edge.mapper is None,
                                "conditional": edge.condition is not None,
                            }
                        )
            return {
                "name": group.name,
                "agents": [a.name for a in group.agents],
                "edges": edges,
            }

        # ---------- synchronous dispatch ----------

        @router.post("/agents/{name}/run")
        async def run_agent(
            name: str, body: _RunRequest, request: Request
        ) -> dict[str, object]:
            agent = self._require_agent(name)
            task = _with_request_id(body.task, body.request_id, request)
            result = await self._runtime.run(agent, task)
            return _serialize_result(result)

        @router.post("/agents/{name}/gather")
        async def gather_agent(
            name: str, body: _GatherRequest, request: Request
        ) -> list[dict[str, object]]:
            agent = self._require_agent(name)
            tasks = [_with_request_id(t, body.request_id, request) for t in body.tasks]
            results = await self._runtime.gather(
                agent, tasks, max_concurrency=body.max_concurrency
            )
            return [_serialize_result(r) for r in results]

        @router.post("/groups/{name}/run")
        async def run_group(
            name: str, body: _RunRequest, request: Request
        ) -> dict[str, object]:
            group = self._require_group(name)
            task = _with_request_id(body.task, body.request_id, request)
            result = await self._runtime.run_group(group, task)
            return _serialize_result(result)

        # ---------- async submit / poll / stream ----------

        @router.post("/submit")
        async def submit(body: _SubmitRequest, request: Request) -> dict[str, str]:
            target = body.target
            if body.is_group:
                self._require_group(target)
            else:
                self._require_agent(target)
            run_id = InMemoryRunStore.new_run_id()
            await self._run_store.create(run_id, target=target)
            task = _with_request_id(body.task, body.request_id, request)
            asyncio.create_task(self._execute_run(run_id, target, body.is_group, task))
            return {"run_id": run_id}

        @router.get("/runs/{run_id}/status")
        async def get_run_status(run_id: str) -> RunStatus:
            return await self._run_store.get_status(run_id)

        @router.get("/runs/{run_id}/result")
        async def get_run_result(run_id: str) -> dict[str, object]:
            status = await self._run_store.get_status(run_id)
            if status.state not in {RunState.COMPLETED, RunState.FAILED}:
                raise HTTPException(
                    status_code=409,
                    detail=f"run is {status.state.value}; result not yet available",
                )
            result = await self._run_store.get_result(run_id)
            if result is None:
                raise RegistryError(f"run_id {run_id!r} has no result")
            return _serialize_result(result)

        @router.get("/runs/{run_id}/stream")
        async def stream_run(run_id: str) -> EventSourceResponse:
            await self._run_store.get_status(run_id)  # 404 early if unknown

            async def _gen() -> AsyncIterator[dict[str, str]]:
                async for ev in self._run_store.stream(run_id):
                    yield {"event": ev.type.value, "data": ev.model_dump_json()}

            return EventSourceResponse(_gen())

        @router.post("/runs/{run_id}/cancel")
        async def cancel_run(run_id: str) -> dict[str, str]:
            status = await self._run_store.get_status(run_id)
            if status.state in {
                RunState.COMPLETED,
                RunState.FAILED,
                RunState.CANCELLED,
            }:
                return {"state": status.state.value}
            await self._run_store.set_state(run_id, RunState.CANCELLED)
            await self._run_store.push_event(
                run_id, RunEvent(type=RunEventType.RUN_CANCELLED, run_id=run_id)
            )
            return {"state": RunState.CANCELLED.value}

        return router

    # ------------------------------------------------------------------ helpers

    def _require_agent(self, name: str) -> Agent:
        if name not in self._agents:
            raise RegistryError(f"agent {name!r} is not registered")
        return self._agents[name]

    def _require_group(self, name: str) -> AgentGroup:
        if name not in self._groups:
            raise RegistryError(f"group {name!r} is not registered")
        return self._groups[name]

    async def _execute_run(
        self,
        run_id: str,
        target: str,
        is_group: bool,
        task: TaskSpec,
    ) -> None:
        self._active_runs.add(run_id)
        await self._run_store.set_state(run_id, RunState.RUNNING)
        await self._run_store.push_event(
            run_id,
            RunEvent(type=RunEventType.AGENT_STARTED, run_id=run_id, agent=target),
        )
        try:
            if is_group:
                group = self._require_group(target)
                result = await self._runtime.run_group(group, task)
            else:
                agent = self._require_agent(target)
                result = await self._runtime.run(agent, task)
            await self._run_store.set_result(run_id, result)
            await self._run_store.set_state(
                run_id,
                RunState.COMPLETED if result.is_ok() else RunState.FAILED,
            )
            await self._run_store.push_event(
                run_id,
                RunEvent(
                    type=RunEventType.GROUP_COMPLETED
                    if is_group
                    else RunEventType.AGENT_COMPLETED,
                    run_id=run_id,
                    agent=target,
                ),
            )
        except Exception as exc:
            await self._run_store.set_state(run_id, RunState.FAILED)
            await self._run_store.push_event(
                run_id,
                RunEvent(
                    type=RunEventType.AGENT_FAILED,
                    run_id=run_id,
                    agent=target,
                    error=str(exc),
                ),
            )
        finally:
            self._active_runs.discard(run_id)

    async def _drain(self) -> None:
        """Best-effort drain on FastAPI lifespan shutdown."""
        if not self._active_runs:
            return
        await log.ainfo("server_drain_start", active=len(self._active_runs))
        deadline = asyncio.get_running_loop().time() + self._drain_timeout
        while self._active_runs:
            if asyncio.get_running_loop().time() > deadline:
                await log.awarning(
                    "server_drain_timeout", remaining=len(self._active_runs)
                )
                break
            await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# Free helpers
# ---------------------------------------------------------------------------


def _serialize_result(result: AgentResult[BaseModel]) -> dict[str, object]:
    """JSON-friendly view of an :class:`AgentResult`. Errors stringified."""
    return {
        "agent_name": result.agent_name,
        "task_id": result.task_id,
        "success": result.is_ok(),
        "output": result.output.model_dump() if result.output is not None else None,
        "error": str(result.error) if result.error is not None else None,
        "metadata": result.metadata.model_dump(),
    }


def _with_request_id(
    task: TaskSpec, body_request_id: str | None, request: Request
) -> TaskSpec:
    """Stamp the request_id from header / body onto the TaskSpec."""
    rid = body_request_id or getattr(request.state, "request_id", task.request_id)
    if rid == task.request_id:
        return task
    return task.model_copy(update={"request_id": rid})


class _suppress_value_error:
    """Tiny context manager — Windows asyncio raises ValueError for SIGTERM."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: type | None, *_: object) -> bool:
        return exc_type is not None and issubclass(
            exc_type, (ValueError, NotImplementedError)
        )


def _registered_names(
    agents: Iterable[Agent],
) -> Sequence[str]:  # pragma: no cover — debug helper
    return tuple(a.name for a in agents)


__all__ = ["AgentServer"]
