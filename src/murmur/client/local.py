"""``LocalClient`` — in-process counterpart to the HTTP
:class:`murmur_client.MurmurClient`.

Same calling surface as the HTTP client, but dispatches against an
in-process :class:`murmur.AgentRuntime` instead of going over the wire.
Use this when both producer and consumer live in the same ASGI process
(e.g. a FastAPI app with :class:`murmur.server.AgentRouter` mounted) and
you want to skip the httpx round-trip.

Two explicit classes — ``MurmurClient(server_url)`` HTTP and
``LocalClient(runtime=...)`` in-process — over a unified
``MurmurClient(transport=...)``. Each constructor takes the obvious args
for its transport; both classes satisfy the same calling Protocol so
generic code can accept either.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel

from murmur._sync import reject_if_in_event_loop
from murmur.core.errors import RegistryError
from murmur.runs import (
    InMemoryRunStore,
    RunEvent,
    RunEventType,
    RunState,
    RunStatus,
)
from murmur.server.app import AgentServer

if TYPE_CHECKING:
    from murmur.runs import RunStore
    from murmur.runtime import AgentRuntime
    from murmur.types import AgentResult, GroupResult, TaskSpec


class LocalClient:
    """Async in-process client. Same surface as
    :class:`murmur_client.MurmurClient`.

    Construct with either:

    - ``server=``: wrap an existing :class:`AgentServer` (typical when
      the server is already shared with an :class:`AgentRouter`).
    - ``runtime=`` / ``run_store=``: build an internal server from these.

    The two constructor shapes are mutually exclusive — pass one or the
    other, not both.
    """

    def __init__(
        self,
        *,
        server: AgentServer | None = None,
        runtime: AgentRuntime | None = None,
        run_store: RunStore | None = None,
    ) -> None:
        if server is not None and (runtime is not None or run_store is not None):
            raise ValueError(
                "pass either `server=` or (`runtime=`/`run_store=`) — not both"
            )

        self._server: AgentServer = server or AgentServer(
            runtime=runtime, run_store=run_store
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """No-op for the local client — kept for API symmetry with the
        HTTP client. The backing :class:`AgentServer` and its runtime
        own their own lifecycles; we don't tear them down here.
        """
        return None

    @property
    def server(self) -> AgentServer:
        """The backing server — registry, runtime, run-store."""
        return self._server

    # ------------------------------------------------------------------ discovery

    async def health(self) -> dict[str, str]:
        return {"status": "ok"}

    async def list_agents(self) -> list[str]:
        return sorted(self._server._agents)  # noqa: SLF001

    async def get_agent_schema(self, name: str) -> dict[str, Any]:
        agent = self._server._require_agent(name)  # noqa: SLF001
        return {
            "name": agent.name,
            "input_type": (
                agent.input_type.model_json_schema()
                if agent.input_type is not None
                else None
            ),
            "output_type": agent.output_type.model_json_schema(),
        }

    async def list_groups(self) -> list[str]:
        return sorted(self._server._groups)  # noqa: SLF001

    async def get_group_topology(self, name: str) -> dict[str, Any]:
        group = self._server._require_group(name)  # noqa: SLF001
        edges: list[dict[str, Any]] = []
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

    # ------------------------------------------------------------------ sync dispatch

    async def run(
        self,
        agent_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel]:
        agent = self._server._require_agent(agent_name)  # noqa: SLF001
        task = _with_request_id(task, request_id)
        return await self._server._runtime.run(agent, task)  # noqa: SLF001

    async def gather(
        self,
        agent_name: str,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,
        request_id: str | None = None,
    ) -> list[AgentResult[BaseModel]]:
        agent = self._server._require_agent(agent_name)  # noqa: SLF001
        stamped = [_with_request_id(t, request_id) for t in tasks]
        return await self._server._runtime.gather(  # noqa: SLF001
            agent, stamped, max_concurrency=max_concurrency
        )

    async def run_group(
        self,
        group_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel] | GroupResult:
        group = self._server._require_group(group_name)  # noqa: SLF001
        task = _with_request_id(task, request_id)
        return await self._server._runtime.run_group(group, task)  # noqa: SLF001

    # ---------------------------------------------------------------- sync entry points

    def run_sync(
        self,
        agent_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel]:
        """Blocking variant of :meth:`run` — wraps the async path in
        :func:`asyncio.run`. **Cannot be called from inside a running
        event loop**.
        """
        reject_if_in_event_loop("LocalClient.run_sync")
        return asyncio.run(self.run(agent_name, task, request_id=request_id))

    # ------------------------------------------------------------------ async dispatch

    async def submit(
        self,
        target: str,
        task: TaskSpec,
        *,
        is_group: bool = False,
        request_id: str | None = None,
    ) -> LocalRun:
        # Validate target exists before queueing — same fail-fast as the
        # HTTP /submit endpoint.
        if is_group:
            self._server._require_group(target)  # noqa: SLF001
        else:
            self._server._require_agent(target)  # noqa: SLF001

        run_id = InMemoryRunStore.new_run_id()
        await self._server._run_store.create(run_id, target=target)  # noqa: SLF001
        stamped = _with_request_id(task, request_id)
        asyncio.create_task(
            self._server._execute_run(run_id, target, is_group, stamped)  # noqa: SLF001
        )
        return LocalRun(client=self, run_id=run_id, target=target)

    # ------------------------------------------------------------ run-handle internals

    async def _status(self, run_id: str) -> RunStatus:
        return await self._server._run_store.get_status(run_id)  # noqa: SLF001

    async def _result(self, run_id: str) -> AgentResult[BaseModel] | GroupResult:
        status = await self._server._run_store.get_status(run_id)  # noqa: SLF001
        if status.state not in {RunState.COMPLETED, RunState.FAILED}:
            raise RegistryError(
                f"run is {status.state.value}; result not yet available"
            )
        result = await self._server._run_store.get_result(run_id)  # noqa: SLF001
        if result is None:
            raise RegistryError(f"run_id {run_id!r} has no result")
        return result

    async def _cancel(self, run_id: str) -> None:
        status = await self._server._run_store.get_status(run_id)  # noqa: SLF001
        if status.state in {
            RunState.COMPLETED,
            RunState.FAILED,
            RunState.CANCELLED,
        }:
            return
        await self._server._run_store.set_state(run_id, RunState.CANCELLED)  # noqa: SLF001
        await self._server._run_store.push_event(  # noqa: SLF001
            run_id, RunEvent(type=RunEventType.RUN_CANCELLED, run_id=run_id)
        )

    async def _stream(self, run_id: str) -> AsyncIterator[RunEvent]:
        await self._server._run_store.get_status(run_id)  # noqa: SLF001
        async for ev in self._server._run_store.stream(run_id):  # noqa: SLF001
            yield ev


class LocalRun:
    """Handle for an asynchronously-dispatched in-process run.

    Mirrors the shape of :class:`murmur_client.client.Run` (the HTTP
    variant) so the same code paths work against either client. The two
    classes don't share a base — each just exposes the same four methods
    so callers can write code generic over the run-handle Protocol.
    """

    def __init__(self, *, client: LocalClient, run_id: str, target: str) -> None:
        self._client = client
        self._run_id = run_id
        self._target = target

    @property
    def id(self) -> str:
        return self._run_id

    @property
    def target(self) -> str:
        return self._target

    async def status(self) -> RunStatus:
        return await self._client._status(self._run_id)

    async def result(self) -> AgentResult[BaseModel] | GroupResult:
        return await self._client._result(self._run_id)

    async def cancel(self) -> None:
        await self._client._cancel(self._run_id)

    def stream(self) -> AsyncIterator[RunEvent]:
        return self._client._stream(self._run_id)


def _with_request_id(task: TaskSpec, request_id: str | None) -> TaskSpec:
    """Stamp ``request_id`` onto ``task`` if it differs."""
    if request_id is None or request_id == task.request_id:
        return task
    return task.model_copy(update={"request_id": request_id})


__all__ = ["LocalClient", "LocalRun"]
