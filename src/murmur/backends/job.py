"""JobBackend — broker-distributed execution.

Activated when :class:`murmur.AgentRuntime` is constructed with a broker URL
(``kafka://``, ``nats://``, ``amqp://``, ``redis://``, or the in-process
``memory://`` mode). FastStream is a hidden implementation detail — users
never import ``faststream`` directly. The only sanctioned PydanticAI /
FastStream imports outside this module live in :mod:`murmur.interop`.

Topology (Addendum 3)::

    runtime --publish--> murmur.{agent}.tasks ---> Worker
                                                      |
                                                      v
                                             ThreadBackend dispatch
                                                      |
              ResultCollector  <----publish---- murmur.results.{runtime_id}

Per Addendum 3 §"JobBackend is just a transport for ThreadBackend
invocations across machines": this backend never runs the LLM itself; it
publishes tasks, the Worker consumes them, dispatches via ThreadBackend's
path locally, and publishes back. Symmetry collapses duplication.

Satisfies :class:`murmur.core.protocols.Backend` structurally — required
surface: ``spawn``, ``status``, ``kill``, ``result``. Adds a backend-native
``gather`` (Addendum 3) using the :class:`ResultCollector`.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from murmur.backends._collector import ResultCollector
from murmur.core.errors import SpawnError
from murmur.core.protocols.backend import BackendStatus
from murmur.messages import ResultMessage, TaskMessage, task_topic
from murmur.types import (
    AgentContext,
    AgentHandle,
    AgentResult,
    ResultMetadata,
    TaskSpec,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.agent import Agent
    from murmur.core.protocols.broker import Broker


log: structlog.stdlib.BoundLogger = structlog.get_logger()


class JobBackend:
    """Broker-backed backend. Publishes tasks, awaits results via collector."""

    name: str = "job"

    def __init__(
        self,
        *,
        broker: Broker,
        runtime_id: str | None = None,
        broker_url: str | None = None,
        default_timeout: float | None = None,
    ) -> None:
        self._broker = broker
        self._runtime_id: str = runtime_id or str(uuid.uuid4())
        self._broker_url: str | None = broker_url
        self._default_timeout = default_timeout
        self._collector = ResultCollector(self._runtime_id, broker)
        self._cache: dict[str, AgentResult[BaseModel]] = {}
        self._handles: dict[str, tuple[Agent, TaskSpec]] = {}
        self._started: bool = False

    @property
    def runtime_id(self) -> str:
        return self._runtime_id

    @property
    def broker_url(self) -> str | None:
        return self._broker_url

    async def start(self) -> None:
        """Connect the broker and register the reply-topic subscription.

        Idempotent. ``AgentRuntime`` calls this lazily on first ``run`` /
        ``gather``; users never need to call it explicitly.
        """
        if self._started:
            return
        await self._broker.start()
        await self._collector.start()
        self._started = True

    async def stop(self) -> None:
        """Disconnect the broker. Idempotent."""
        await self._broker.stop()
        self._started = False

    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,  # noqa: ARG002 — context-passer runs server-side
    ) -> AgentHandle:
        await self.start()
        handle = AgentHandle(agent_name=agent.name, task_id=task.id, backend=self.name)
        batch_id = handle.handle_id
        self._collector.register(batch_id, expected=1)
        self._handles[batch_id] = (agent, task)

        msg = TaskMessage(
            batch_id=batch_id,
            task_id=f"{batch_id}-0",
            reply_to=self._collector.reply_topic,
            request_id=task.request_id,
            task=task,
        )
        await log.ainfo(
            "agent_spawned",
            agent_name=agent.name,
            task_id=task.id,
            request_id=task.request_id,
            backend=self.name,
            trust_level=agent.trust_level.value,
        )
        await self._broker.publish(
            task_topic(agent.name), msg.model_dump_json().encode()
        )
        return handle

    async def status(self, handle: AgentHandle) -> BackendStatus:
        """Best-effort status — broker abstractions don't expose remote state.

        Returns ``COMPLETED`` once the result has landed in the local cache,
        ``RUNNING`` while we're waiting on a known handle, and raises if the
        handle was never spawned by this backend instance.
        """
        if handle.handle_id in self._cache:
            cached = self._cache[handle.handle_id]
            return BackendStatus.COMPLETED if cached.is_ok() else BackendStatus.FAILED
        if handle.handle_id not in self._handles:
            raise SpawnError(f"unknown handle {handle.handle_id!r}")
        return BackendStatus.RUNNING

    async def kill(self, handle: AgentHandle) -> None:
        """Best-effort kill.

        Broker semantics don't allow us to recall an in-flight task once
        published; we mark the local handle as cancelled so subsequent
        ``result`` calls return a ``KILLED`` envelope without blocking on
        the worker. Workers may still complete and publish a result that we
        then discard as an orphan.
        """
        if handle.handle_id in self._cache:
            return
        if handle.handle_id not in self._handles:
            return
        agent, task = self._handles.pop(handle.handle_id)
        self._cache[handle.handle_id] = AgentResult[BaseModel](
            output=None,
            error=SpawnError(f"agent {agent.name!r} was killed"),
            metadata=ResultMetadata(backend=self.name),
            agent_name=agent.name,
            task_id=task.id,
        )

    async def result(self, handle: AgentHandle) -> AgentResult[BaseModel]:
        if handle.handle_id in self._cache:
            return self._cache[handle.handle_id]
        if handle.handle_id not in self._handles:
            raise SpawnError(f"unknown handle {handle.handle_id!r}")
        agent, task = self._handles[handle.handle_id]
        msg = await self._collector.await_handle(
            batch_id=handle.handle_id,
            timeout=self._default_timeout,
        )
        result = _msg_to_result(msg, agent=agent, user_task_id=task.id)
        self._cache[handle.handle_id] = result
        self._handles.pop(handle.handle_id, None)
        return result

    async def gather(
        self,
        agent: Agent,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,  # noqa: ARG002 — broker fan-out is bounded by worker count
    ) -> list[AgentResult[BaseModel]]:
        """Publish ``tasks`` as one batch; await all via the ``ResultCollector``.

        ``max_concurrency`` is accepted for API parity with the Backend
        Protocol but ignored — broker-side fan-out is bounded by the worker
        fleet's concurrency / prefetch, not the publisher's.
        """
        if not tasks:
            return []
        await self.start()
        batch_id = str(uuid.uuid4())
        self._collector.register(batch_id, expected=len(tasks))

        topic = task_topic(agent.name)
        for index, task in enumerate(tasks):
            msg = TaskMessage(
                batch_id=batch_id,
                task_id=f"{batch_id}-{index}",
                reply_to=self._collector.reply_topic,
                request_id=task.request_id,
                task=task,
            )
            await self._broker.publish(topic, msg.model_dump_json().encode())

        slots = await self._collector.gather_batch(
            batch_id=batch_id,
            timeout=self._default_timeout,
        )
        return [
            _msg_to_result(slots[i], agent=agent, user_task_id=tasks[i].id)
            for i in range(len(tasks))
        ]


def _msg_to_result(
    msg: ResultMessage | None,
    *,
    agent: Agent,
    user_task_id: str,
) -> AgentResult[BaseModel]:
    """Convert a wire ``ResultMessage`` into a typed :class:`AgentResult`.

    ``msg=None`` represents a timeout / missing slot — return a synthetic
    failure envelope. Otherwise re-validate ``output_payload`` against the
    agent's ``output_type`` so downstream code receives the right typed
    instance.
    """
    if msg is None:
        return AgentResult[BaseModel](
            output=None,
            error=SpawnError(
                f"task {user_task_id!r} did not complete (broker timeout)"
            ),
            metadata=ResultMetadata(backend="job"),
            agent_name=agent.name,
            task_id=user_task_id,
        )
    metadata = ResultMetadata(
        duration_ms=msg.duration_ms,
        tokens_used=msg.tokens_used,
        backend=msg.backend or "job",
    )
    if msg.success and msg.output_payload is not None:
        typed_output = agent.output_type.model_validate(dict(msg.output_payload))
        return AgentResult[BaseModel](
            output=typed_output,
            error=None,
            metadata=metadata,
            agent_name=agent.name,
            task_id=user_task_id,
        )
    return AgentResult[BaseModel](
        output=None,
        error=SpawnError(msg.error_message or "agent failed"),
        metadata=metadata,
        agent_name=agent.name,
        task_id=user_task_id,
    )


__all__ = ["JobBackend"]
