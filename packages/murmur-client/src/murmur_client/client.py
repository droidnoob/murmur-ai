"""``MurmurClient`` and the :class:`Run` handle.

User-facing surface:

- :meth:`MurmurClient.run` — dispatch a single agent, await result.
- :meth:`MurmurClient.gather` — fan out an agent over many tasks.
- :meth:`MurmurClient.run_group` — synchronous group dispatch.
- :meth:`MurmurClient.submit` — async dispatch; returns a :class:`Run`.
- :class:`Run` — wraps ``status`` / ``result`` / ``stream`` / ``cancel``.

Errors round-trip with their typed class — the server emits an
:class:`ErrorResponse`, the client maps it back to the matching
:class:`MurmurError` subclass so user code catches the same exception
whether it ran locally or remote.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import Any, Protocol, Self

import httpx
from pydantic import BaseModel, ConfigDict, Field

from murmur._sync import reject_if_in_event_loop
from murmur.core.errors import MurmurError, RegistryError
from murmur.runs import RunEvent, RunStatus
from murmur.server.errors import ErrorResponse, response_to_error
from murmur.types import AgentResult, ResultMetadata, TaskSpec

_REQUEST_ID_HEADER = "X-Request-Id"


class _RemoteResult(BaseModel):
    """Wire shape returned by sync run / gather / run_group endpoints."""

    model_config = ConfigDict(frozen=True)

    agent_name: str
    task_id: str
    success: bool
    output: dict[str, Any] | None = None
    error: str | None = None
    metadata: ResultMetadata = Field(default_factory=ResultMetadata)


class MurmurClient:
    """Async HTTP client for an :class:`AgentServer`.

    Use as an async context manager so the underlying ``httpx.AsyncClient``
    is closed cleanly. ``transport`` is an escape hatch for tests
    (``httpx.ASGITransport(app=server.app)``).
    """

    def __init__(
        self,
        server_url: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        sync_transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._server_url = server_url.rstrip("/")
        self._timeout = timeout
        self._sync_transport = sync_transport
        self._http: httpx.AsyncClient = httpx.AsyncClient(
            base_url=self._server_url,
            timeout=timeout,
            transport=transport,
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
        await self._http.aclose()

    # ------------------------------------------------------------------ discovery

    async def health(self) -> dict[str, str]:
        r = await self._http.get("/health")
        self._raise_for_status(r)
        return r.json()

    async def list_agents(self) -> list[str]:
        r = await self._http.get("/agents")
        self._raise_for_status(r)
        return list(r.json())

    async def get_agent_schema(self, name: str) -> dict[str, Any]:
        r = await self._http.get(f"/agents/{name}/schema")
        self._raise_for_status(r)
        return dict(r.json())

    async def list_groups(self) -> list[str]:
        r = await self._http.get("/groups")
        self._raise_for_status(r)
        return list(r.json())

    async def get_group_topology(self, name: str) -> dict[str, Any]:
        r = await self._http.get(f"/groups/{name}/topology")
        self._raise_for_status(r)
        return dict(r.json())

    # ------------------------------------------------------------------ sync dispatch

    async def run(
        self,
        agent_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel]:
        rid = request_id or str(uuid.uuid4())
        r = await self._http.post(
            f"/agents/{agent_name}/run",
            json={"task": task.model_dump(), "request_id": rid},
            headers={_REQUEST_ID_HEADER: rid},
        )
        self._raise_for_status(r)
        return _wire_to_agent_result(r.json())

    async def gather(
        self,
        agent_name: str,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,
        request_id: str | None = None,
    ) -> list[AgentResult[BaseModel]]:
        rid = request_id or str(uuid.uuid4())
        r = await self._http.post(
            f"/agents/{agent_name}/gather",
            json={
                "tasks": [t.model_dump() for t in tasks],
                "max_concurrency": max_concurrency,
                "request_id": rid,
            },
            headers={_REQUEST_ID_HEADER: rid},
        )
        self._raise_for_status(r)
        return [_wire_to_agent_result(item) for item in r.json()]

    async def run_group(
        self,
        group_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel]:
        rid = request_id or str(uuid.uuid4())
        r = await self._http.post(
            f"/groups/{group_name}/run",
            json={"task": task.model_dump(), "request_id": rid},
            headers={_REQUEST_ID_HEADER: rid},
        )
        self._raise_for_status(r)
        return _wire_to_agent_result(r.json())

    # ---------------------------------------------------------------- sync entry points

    def run_sync(
        self,
        agent_name: str,
        task: TaskSpec,
        *,
        request_id: str | None = None,
    ) -> AgentResult[BaseModel]:
        """Blocking variant of :meth:`run` for notebook / REPL / script use.

        Opens a one-shot :class:`httpx.Client` for the call — does not
        share the persistent ``httpx.AsyncClient`` used by the async
        methods. **Cannot be called from inside a running event loop**
        (raises :class:`RuntimeError`).
        """
        reject_if_in_event_loop("MurmurClient.run_sync")
        rid = request_id or str(uuid.uuid4())
        with httpx.Client(
            base_url=self._server_url,
            timeout=self._timeout,
            transport=self._sync_transport,
        ) as http:
            r = http.post(
                f"/agents/{agent_name}/run",
                json={"task": task.model_dump(), "request_id": rid},
                headers={_REQUEST_ID_HEADER: rid},
            )
            self._raise_for_status(r)
            return _wire_to_agent_result(r.json())

    # ------------------------------------------------------------------ async dispatch

    async def submit(
        self,
        target: str,
        task: TaskSpec,
        *,
        is_group: bool = False,
        request_id: str | None = None,
    ) -> Run:
        rid = request_id or str(uuid.uuid4())
        r = await self._http.post(
            "/submit",
            json={
                "target": target,
                "is_group": is_group,
                "task": task.model_dump(),
                "request_id": rid,
            },
            headers={_REQUEST_ID_HEADER: rid},
        )
        self._raise_for_status(r)
        return Run(client=self, run_id=r.json()["run_id"], target=target)

    # ------------------------------------------------------------ run-handle internals

    async def _status(self, run_id: str) -> RunStatus:
        r = await self._http.get(f"/runs/{run_id}/status")
        self._raise_for_status(r)
        return RunStatus.model_validate(r.json())

    async def _result(self, run_id: str) -> AgentResult[BaseModel]:
        r = await self._http.get(f"/runs/{run_id}/result")
        self._raise_for_status(r)
        return _wire_to_agent_result(r.json())

    async def _cancel(self, run_id: str) -> None:
        r = await self._http.post(f"/runs/{run_id}/cancel")
        self._raise_for_status(r)

    async def _stream(self, run_id: str) -> AsyncIterator[RunEvent]:
        async with self._http.stream("GET", f"/runs/{run_id}/stream") as response:
            self._raise_for_status(response)
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:") :].strip()
                if not payload:
                    continue
                try:
                    yield RunEvent.model_validate_json(payload)
                except Exception:  # pragma: no cover — malformed lines
                    continue

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code < 400:
            return
        body: dict[str, Any]
        try:
            body = response.json()
        except json.JSONDecodeError:
            raise MurmurError(
                f"server returned {response.status_code} with non-JSON body: "
                f"{response.text!r}"
            ) from None
        if "error" in body:
            try:
                err = ErrorResponse.model_validate(body)
                raise response_to_error(err)
            except (KeyError, ValueError):  # pragma: no cover — malformed payload
                pass
        # Fallback for HTTPException-style detail (no typed error class).
        detail = body.get("detail", body)
        raise RegistryError(f"server returned {response.status_code}: {detail!r}")


class _RunBackend(Protocol):
    """Internal hooks ``Run`` calls. Both :class:`MurmurClient` (HTTP) and
    :class:`murmur_client.LocalClient` (in-process) satisfy this Protocol —
    structural, no inheritance — so a single :class:`Run` handle works for
    either transport."""

    async def _status(self, run_id: str) -> RunStatus: ...
    async def _result(self, run_id: str) -> AgentResult[BaseModel]: ...
    async def _cancel(self, run_id: str) -> None: ...
    def _stream(self, run_id: str) -> AsyncIterator[RunEvent]: ...


class Run:
    """Handle for an asynchronously-dispatched run.

    Backed by either :class:`MurmurClient` (HTTP) or
    :class:`murmur_client.LocalClient` (in-process); the four ``_status`` /
    ``_result`` / ``_cancel`` / ``_stream`` hooks satisfy a shared Protocol.
    """

    def __init__(self, *, client: _RunBackend, run_id: str, target: str) -> None:
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

    async def result(self) -> AgentResult[BaseModel]:
        return await self._client._result(self._run_id)

    async def cancel(self) -> None:
        await self._client._cancel(self._run_id)

    def stream(self) -> AsyncIterator[RunEvent]:
        return self._client._stream(self._run_id)


def _wire_to_agent_result(body: dict[str, Any]) -> AgentResult[BaseModel]:
    """Reverse of ``murmur.server.app._serialize_result``.

    The client doesn't know the agent's ``output_type`` so it returns the
    output as a generic dict-bearing :class:`BaseModel`. Callers that want
    a strongly-typed instance should call ``OutputType.model_validate(
    result.output.model_dump())`` themselves; we don't have the schema info
    here without a discovery round-trip.
    """
    metadata = ResultMetadata.model_validate(body.get("metadata", {}))
    output_dict = body.get("output")
    output: BaseModel | None
    if output_dict is None:
        output = None
    else:
        output = _UntypedOutput.model_validate({"_payload": output_dict})
    error: BaseException | None
    err_msg = body.get("error")
    error = MurmurError(err_msg) if err_msg else None
    return AgentResult[BaseModel](
        output=output,
        error=error,
        metadata=metadata,
        agent_name=body["agent_name"],
        task_id=body["task_id"],
    )


class _UntypedOutput(BaseModel):
    """Dict-shaped fallback for client-side outputs.

    A discovery-driven typed deserializer (the client fetches the schema
    and instantiates the correct class) is a future option. For now the
    payload is exposed as a plain dict via :attr:`payload`.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    payload: dict[str, Any] = Field(default_factory=dict, alias="_payload")

    def model_dump(self, **_: Any) -> dict[str, Any]:  # type: ignore[override]
        return dict(self.payload)


__all__ = ["MurmurClient", "Run"]
