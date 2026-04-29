"""``Worker`` — the broker-side consumer.

A ``Worker`` subscribes to broker topics for a set of agents and dispatches
incoming tasks through an internal ``ThreadBackend``-backed runtime, then
publishes the result back to the message's ``reply_to``. Lifecycle hooks
let users plug in observability without subclassing.

The worker's runtime MUST be a *local* one — give it a broker URL and
you get an infinite loop where tasks are republished instead of
dispatched. The constructor builds a default ``AgentRuntime()`` (no
broker, ThreadBackend) when none is supplied.

Satisfies :class:`murmur.core.protocols.worker.Worker` structurally.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

import structlog
import structlog.contextvars

from murmur.core.errors import SpecValidationError
from murmur.core.protocols.worker import OnComplete, OnError, OnStart
from murmur.messages import ResultMessage, TaskMessage, task_topic

if TYPE_CHECKING:
    from murmur.agent import Agent
    from murmur.core.protocols.broker import Broker, MessageHandler
    from murmur.runtime import AgentRuntime


log: structlog.stdlib.BoundLogger = structlog.get_logger()


class Worker:
    """Consumer for one or more registered agents."""

    def __init__(
        self,
        *,
        broker: Broker,
        agents: Mapping[str, Agent],
        runtime: AgentRuntime | None = None,
        concurrency: int = 10,
        prefetch: int = 5,
    ) -> None:
        if not agents:
            raise SpecValidationError("Worker requires at least one agent")
        if concurrency < 1:
            raise SpecValidationError("concurrency must be >= 1")
        if prefetch < 1:
            raise SpecValidationError("prefetch must be >= 1")

        from murmur.runtime import AgentRuntime as _AgentRuntime

        self._broker = broker
        self._agents: dict[str, Agent] = dict(agents)
        # NB: the worker's runtime MUST be ThreadBackend-backed. Passing a
        # broker-backed runtime here re-publishes tasks → infinite loop.
        self._runtime: AgentRuntime = runtime or _AgentRuntime()
        self._concurrency = concurrency
        self._prefetch = prefetch
        self._semaphore = asyncio.Semaphore(concurrency)
        self._active: dict[str, asyncio.Task[None]] = {}

        self._on_start: OnStart | None = None
        self._on_complete: OnComplete | None = None
        self._on_error: OnError | None = None
        self._started: bool = False

    # ------------------------------------------------------------------ hooks

    def on_task_start(self, fn: OnStart) -> OnStart:
        self._on_start = fn
        return fn

    def on_task_complete(self, fn: OnComplete) -> OnComplete:
        self._on_complete = fn
        return fn

    def on_task_error(self, fn: OnError) -> OnError:
        self._on_error = fn
        return fn

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        """Begin consuming tasks. Subscribes ``broker`` to each agent's topic."""
        if self._started:
            return
        await self._broker.start()
        for agent_name in self._agents:
            await self._broker.subscribe(
                task_topic(agent_name), self._make_handler(agent_name)
            )
        self._started = True
        await log.ainfo(
            "worker_started",
            agents=list(self._agents.keys()),
            concurrency=self._concurrency,
        )

    async def stop(self) -> None:
        """Drain in-flight tasks then disconnect the broker."""
        if not self._started:
            return
        active = list(self._active.values())
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        await self._broker.stop()
        self._started = False
        await log.ainfo("worker_stopped")

    # ------------------------------------------------------------------ private

    def _make_handler(self, agent_name: str) -> MessageHandler:
        async def handler(payload: bytes) -> None:
            try:
                msg = TaskMessage.model_validate_json(payload)
            except Exception as exc:
                await log.aerror("worker_decode_failed", error=str(exc))
                return
            current = asyncio.current_task()
            if current is not None:
                self._active[msg.task_id] = current
            try:
                await self._run_one(agent_name, msg)
            finally:
                self._active.pop(msg.task_id, None)

        return handler

    async def _run_one(self, agent_name: str, msg: TaskMessage) -> None:
        agent = self._agents[agent_name]
        structlog.contextvars.bind_contextvars(
            request_id=msg.request_id,
            batch_id=msg.batch_id,
            task_id=msg.task_id,
            agent_name=agent_name,
        )
        async with self._semaphore:
            start = time.perf_counter()
            response: ResultMessage
            try:
                if self._on_start is not None:
                    await self._on_start(msg.task_id, agent_name)
                result = await self._runtime.run(agent, msg.task)
                duration_ms = int((time.perf_counter() - start) * 1000)
                if self._on_complete is not None:
                    await self._on_complete(msg.task_id, agent_name, duration_ms)
                if result.is_ok() and result.output is not None:
                    response = ResultMessage(
                        batch_id=msg.batch_id,
                        task_id=msg.task_id,
                        request_id=msg.request_id,
                        success=True,
                        output_payload=result.output.model_dump(),
                        error_message=None,
                        duration_ms=result.metadata.duration_ms,
                        tokens_used=result.metadata.tokens_used,
                        backend=result.metadata.backend,
                        agent_name=agent_name,
                    )
                else:
                    response = ResultMessage(
                        batch_id=msg.batch_id,
                        task_id=msg.task_id,
                        request_id=msg.request_id,
                        success=False,
                        output_payload=None,
                        error_message=str(result.error)
                        if result.error
                        else "agent failed",
                        duration_ms=result.metadata.duration_ms,
                        tokens_used=result.metadata.tokens_used,
                        backend=result.metadata.backend,
                        agent_name=agent_name,
                    )
            except Exception as exc:
                if self._on_error is not None:
                    await self._on_error(msg.task_id, agent_name, exc)
                response = ResultMessage(
                    batch_id=msg.batch_id,
                    task_id=msg.task_id,
                    request_id=msg.request_id,
                    success=False,
                    output_payload=None,
                    error_message=str(exc),
                    agent_name=agent_name,
                )
            finally:
                structlog.contextvars.unbind_contextvars(
                    "request_id", "batch_id", "task_id", "agent_name"
                )
        await self._broker.publish(msg.reply_to, response.model_dump_json().encode())


__all__ = ["Worker"]
