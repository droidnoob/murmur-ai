"""JobBackend — broker-distributed execution.

Activated when :class:`murmur.AgentRuntime` is constructed with a broker URL
(``kafka://``, ``nats://``, ``amqp://``, ``redis://``, or the in-process
``memory://`` mode). FastStream is a hidden implementation detail — users
never import ``faststream`` directly. The only sanctioned PydanticAI /
FastStream imports outside this module live in :mod:`murmur.interop`.

Topology::

    runtime --publish--> murmur.{agent}.tasks ---> Worker
                                                      |
                                                      v
                                             ThreadBackend dispatch
                                                      |
              ResultCollector  <----publish---- murmur.results.{runtime_id}

This backend is a transport for ThreadBackend invocations across
machines — it never runs the LLM itself. It publishes tasks, the Worker
consumes them, dispatches via ThreadBackend's path locally, and
publishes the result back.

Satisfies :class:`murmur.core.protocols.Backend` structurally — required
surface: ``spawn``, ``status``, ``kill``, ``result``. Adds a backend-native
``gather`` using the :class:`ResultCollector`.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from murmur.backends._collector import ResultCollector
from murmur.core.errors import SpawnError
from murmur.core.protocols.backend import BackendStatus
from murmur.messages import ResultMessage, TaskMessage, events_topic, task_topic
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
    from murmur.core.protocols.events import EventEmitter


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
        publish_events: bool = False,
        event_emitter: EventEmitter | None = None,
    ) -> None:
        self._broker = broker
        self._runtime_id: str = runtime_id or str(uuid.uuid4())
        self._broker_url: str | None = broker_url
        self._default_timeout = default_timeout
        self._collector = ResultCollector(self._runtime_id, broker)
        self._cache: dict[str, AgentResult[BaseModel]] = {}
        self._handles: dict[str, tuple[Agent, TaskSpec]] = {}
        self._started: bool = False
        self._publish_events: bool = publish_events
        # ``event_emitter`` is what we forward bridged events into. The
        # runtime passes its own emitter down so per-agent / per-tool
        # events fired on the worker side land in the publisher's local
        # observability stack alongside batch / group events. Optional
        # because tests construct ``JobBackend`` directly without a
        # runtime above it; ``publish_events=True`` without an emitter
        # is a programming error and we reject it eagerly.
        if publish_events and event_emitter is None:
            raise SpawnError(
                "JobBackend(publish_events=True) requires event_emitter= "
                "so received events have somewhere to land"
            )
        self._event_emitter: EventEmitter | None = event_emitter

    @property
    def runtime_id(self) -> str:
        return self._runtime_id

    @property
    def broker_url(self) -> str | None:
        return self._broker_url

    @property
    def started(self) -> bool:
        """``True`` once :meth:`start` has connected the broker.

        Surfaced for readiness probes so callers can distinguish "broker
        configured but not yet ready" from "no broker configured at all".
        """
        return self._started

    @property
    def broker(self) -> Broker:
        """The underlying :class:`Broker` instance (in-memory or FastStream).

        Exposed so callers can drill through to the FastStream broker and
        register their own ``@broker.subscriber`` handlers next to
        Murmur's.
        """
        return self._broker

    @property
    def publish_events(self) -> bool:
        """Whether worker-side runtime events are bridged back to this
        runtime's local emitter via ``murmur.events.{runtime_id}``."""
        return self._publish_events

    async def start(self) -> None:
        """Connect the broker and register the reply-topic subscription.

        Idempotent. ``AgentRuntime`` calls this lazily on first ``run`` /
        ``gather``; users never need to call it explicitly.

        When ``publish_events=True`` was passed at construction, also
        subscribes to the per-runtime events topic so worker-side
        :class:`RuntimeEvent` envelopes land in the local emitter.
        """
        if self._started:
            return
        await self._broker.start()
        await self._collector.start()
        if self._publish_events:
            await self._broker.subscribe(
                events_topic(self._runtime_id), self._on_event_message
            )
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
            events_topic=self._events_topic_for_publish(),
        )
        await log.ainfo(
            "agent_spawned",
            agent_name=agent.name,
            task_id=task.id,
            request_id=task.request_id,
            backend=self.name,
            trust_level=agent.trust_level.value,
        )
        await self._emit_dispatched(agent=agent, task=task)
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
        bridge_topic = self._events_topic_for_publish()
        for index, task in enumerate(tasks):
            msg = TaskMessage(
                batch_id=batch_id,
                task_id=f"{batch_id}-{index}",
                reply_to=self._collector.reply_topic,
                request_id=task.request_id,
                task=task,
                events_topic=bridge_topic,
            )
            await self._emit_dispatched(agent=agent, task=task)
            await self._broker.publish(topic, msg.model_dump_json().encode())

        slots = await self._collector.gather_batch(
            batch_id=batch_id,
            timeout=self._default_timeout,
        )
        return [
            _msg_to_result(slots[i], agent=agent, user_task_id=tasks[i].id)
            for i in range(len(tasks))
        ]

    # ------------------------------------------------------------------ events

    def _events_topic_for_publish(self) -> str | None:
        """The topic the worker should relay events onto for tasks
        published by *this* backend, or ``None`` when the bridge is off.
        """
        return events_topic(self._runtime_id) if self._publish_events else None

    async def _emit_dispatched(self, *, agent: Agent, task: TaskSpec) -> None:
        """Fire the publisher-side AGENT_DISPATCHED event.

        Always emitted on broker dispatch — independent of
        ``publish_events``. ThreadBackend has no equivalent because its
        AGENT_SPAWNED event already happens publisher-side; for the
        broker path the spawn doesn't fire until the worker picks the
        message up, which can be seconds away. AGENT_DISPATCHED gives
        callers immediate "task accepted" visibility.
        """
        if self._event_emitter is None:
            return
        from murmur.events.types import EventType, RuntimeEvent

        broker_label = self._broker_url
        if broker_label is None:
            scheme = getattr(self._broker, "scheme", None)
            url = getattr(self._broker, "url", None)
            broker_label = url or scheme
        await self._event_emitter.emit(
            RuntimeEvent(
                event_type=EventType.AGENT_DISPATCHED,
                agent_name=agent.name,
                task_id=task.id,
                trace_id=task.request_id,
                payload={
                    "backend": self.name,
                    "broker": broker_label,
                    "trust_level": agent.trust_level.value,
                },
            )
        )

    async def _on_event_message(self, payload: bytes) -> None:
        """Forward a wire :class:`RuntimeEvent` into the local emitter.

        Subscribed only when ``publish_events=True`` (see :meth:`start`).
        Decode failures and emitter exceptions are swallowed —
        observability never takes the runtime down. Without an emitter
        we early-return; ``__init__`` rejects the misconfiguration so
        this guard is defence-in-depth.
        """
        if self._event_emitter is None:  # pragma: no cover — guarded by __init__
            return
        from murmur.events.types import RuntimeEvent

        try:
            event = RuntimeEvent.model_validate_json(payload)
        except Exception as exc:
            await log.aerror("event_bridge_decode_failed", error=str(exc))
            return
        try:
            await self._event_emitter.emit(event)
        except Exception as exc:  # pragma: no cover — emitter contract is no-raise
            await log.aerror("event_bridge_emit_failed", error=str(exc))


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
