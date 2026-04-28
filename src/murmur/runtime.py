"""``murmur.AgentRuntime`` — the front door.

Constructs the pipeline, picks a backend (:class:`ThreadBackend` for local,
:class:`JobBackend` for distributed), and exposes :meth:`run` and
:meth:`gather`.

Broker URLs are parsed *here*. Users never construct FastStream brokers
directly. Supported schemes:

- ``memory://``           — in-process pub/sub (no external services)
- ``kafka://host:port``
- ``nats://host:port``
- ``amqp://host:port``    — RabbitMQ
- ``redis://host:port``
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from pydantic import BaseModel

from murmur.backends._faststream_broker import FastStreamBroker
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.job import JobBackend
from murmur.backends.thread import ThreadBackend
from murmur.core.errors import RegistryError, SpecValidationError
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import AgentContext, AgentResult, TaskSpec

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.agent import Agent
    from murmur.core.protocols.backend import Backend
    from murmur.core.protocols.broker import Broker
    from murmur.core.protocols.registry import Registry
    from murmur.groups.spec import AgentGroup


_FASTSTREAM_SCHEMES: frozenset[str] = frozenset({"kafka", "nats", "amqp", "redis"})
_KNOWN_SCHEMES: frozenset[str] = frozenset({"memory"}) | _FASTSTREAM_SCHEMES


class AgentRuntime:
    """The orchestration runtime.

    >>> runtime = AgentRuntime()                       # ThreadBackend
    >>> runtime = AgentRuntime(broker="memory://")     # JobBackend, in-proc
    >>> runtime = AgentRuntime(broker="kafka://...")   # JobBackend, real broker

    The ``backend`` / ``broker_instance`` / ``registry`` / ``tool_registry``
    / ``tool_executor`` keyword arguments are escape hatches for tests and
    advanced users; production code should rely on the broker-URL parsing
    above.
    """

    def __init__(
        self,
        *,
        broker: str | None = None,
        broker_instance: Broker | None = None,
        runtime_id: str | None = None,
        registry: Registry | None = None,
        backend: Backend | None = None,
        tool_registry: ToolRegistry | None = None,
        tool_executor: ToolExecutor | None = None,
    ) -> None:
        self._registry = registry
        self._tool_registry: ToolRegistry = tool_registry or ToolRegistry()
        self._tool_executor: ToolExecutor = tool_executor or ToolExecutor(
            self._tool_registry
        )
        self._runtime_id: str = runtime_id or str(uuid.uuid4())
        self._backend: Backend = backend or self._build_backend(
            broker_url=broker, broker_instance=broker_instance
        )

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def runtime_id(self) -> str:
        return self._runtime_id

    @property
    def tool_registry(self) -> ToolRegistry:
        return self._tool_registry

    async def run(
        self,
        agent: Agent | str,
        task: TaskSpec,
    ) -> AgentResult[BaseModel]:
        """Run a single agent against a single task. Returns a typed result."""
        resolved = self._resolve(agent)
        prepared_context = await resolved.context_passer.prepare(AgentContext(), task)
        handle = await self._backend.spawn(resolved, task, prepared_context)
        return await self._backend.result(handle)

    async def gather(
        self,
        agent: Agent | str,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,
    ) -> list[AgentResult[BaseModel]]:
        """Fan a single agent across many tasks. Bounded by ``max_concurrency``.

        Delegates to ``backend.gather`` when the backend implements one
        (Addendum 3 — ``ThreadBackend`` uses an ``asyncio.Queue`` + worker
        pool; ``JobBackend`` publishes via the ``ResultCollector``). Falls
        back to a semaphore-bounded fan-out otherwise. Per-task failures
        always land in their slot's :attr:`AgentResult.error` — never
        raises on partial failure.
        """
        if max_concurrency < 1:
            raise SpecValidationError("max_concurrency must be >= 1")
        resolved = self._resolve(agent)

        backend_gather = getattr(self._backend, "gather", None)
        if callable(backend_gather):
            return await backend_gather(
                resolved, tasks, max_concurrency=max_concurrency
            )

        sem = asyncio.Semaphore(max_concurrency)

        async def _one(t: TaskSpec) -> AgentResult[BaseModel]:
            async with sem:
                return await self.run(resolved, t)

        return await asyncio.gather(*[_one(t) for t in tasks])

    async def run_group(
        self,
        group: AgentGroup,
        task: TaskSpec,
    ) -> AgentResult[BaseModel]:
        """Walk an ``AgentGroup`` topology against ``task``.

        Returns the terminal agent's result. Failed slots in fan-out tiers
        are filtered before downstream mappers run; if every slot in a tier
        fails, raises :class:`murmur.core.errors.AllAgentsFailedError`.
        """
        # Imported lazily to keep ``murmur.groups`` optional-feeling and
        # avoid circular import at module load time.
        from murmur.groups.runner import run_group as _run_group

        return await _run_group(self, group, task)

    # ------------------------------------------------------------------ helpers

    def _resolve(self, agent: Agent | str) -> Agent:
        if isinstance(agent, str):
            if self._registry is None:
                raise RegistryError(
                    f"cannot resolve agent name {agent!r}: no registry configured"
                )
            return self._registry.get(agent)
        return agent

    def _build_backend(
        self,
        *,
        broker_url: str | None,
        broker_instance: Broker | None,
    ) -> Backend:
        if broker_instance is not None:
            return JobBackend(broker=broker_instance, runtime_id=self._runtime_id)
        if broker_url is None:
            return ThreadBackend(
                tool_registry=self._tool_registry,
                tool_executor=self._tool_executor,
            )
        scheme = urlparse(broker_url).scheme
        if scheme not in _KNOWN_SCHEMES:
            raise SpecValidationError(
                f"unsupported broker URL scheme {scheme!r}; "
                f"expected one of {sorted(_KNOWN_SCHEMES)}"
            )
        broker = self._build_broker(scheme=scheme, url=broker_url)
        return JobBackend(
            broker=broker, runtime_id=self._runtime_id, broker_url=broker_url
        )

    @staticmethod
    def _build_broker(*, scheme: str, url: str) -> Broker:
        if scheme == "memory":
            return InMemoryBroker()
        return FastStreamBroker(scheme=scheme, url=url)


__all__ = ["AgentRuntime"]
