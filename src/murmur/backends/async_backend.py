"""AsyncBackend — asyncio-based, in-process execution.

The default backend. Zero configuration, no broker required. Suitable for
local development and single-host deployments.

Satisfies :class:`murmur.core.protocols.Backend` structurally — required
surface: ``spawn``, ``status``, ``kill``, ``result``. Adds a backend-native
``gather`` that drives a worker pool over an :class:`asyncio.Queue` so
per-task failures land in their slot rather than collapsing the batch.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

import structlog.contextvars
from pydantic import BaseModel

from murmur._dispatch import build_pydantic_ai_agent
from murmur.core.errors import SpawnError
from murmur.core.protocols.backend import BackendStatus
from murmur.events.log import LogEventEmitter
from murmur.events.types import EventType, RuntimeEvent
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import (
    AgentContext,
    AgentHandle,
    AgentResult,
    ResultMetadata,
    TaskSpec,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pydantic_ai

    from murmur.agent import Agent
    from murmur.core.protocols.events import EventEmitter


class AsyncBackend:
    """Asyncio task-based backend. Default for ``AgentRuntime``."""

    name: str = "thread"

    def __init__(
        self,
        *,
        tool_registry: ToolRegistry | None = None,
        tool_executor: ToolExecutor | None = None,
        event_emitter: EventEmitter | None = None,
    ) -> None:
        # Registry identity rule: the registry the executor consults at
        # execution-time fall-through MUST be the same object as the
        # registry the agent-build path reads from. Otherwise tool
        # registrations land on one view and execution misses them.
        # When both are passed, validate the executor's registry matches;
        # when only the executor is passed, derive the registry from it.
        # When only the registry is passed (or neither), construct the
        # executor against it.
        if tool_executor is not None and tool_registry is not None:
            if tool_executor.registry is not tool_registry:
                raise ValueError(
                    "AsyncBackend(tool_registry=..., tool_executor=...) requires "
                    "the two to share the same registry — otherwise registrations "
                    "on the registry are invisible at execution. Pass only one, "
                    "or ensure tool_executor.registry is tool_registry."
                )
            self._tool_registry: ToolRegistry = tool_registry
        elif tool_executor is not None:
            self._tool_registry = tool_executor.registry
        else:
            self._tool_registry = tool_registry or ToolRegistry()
        # Expose the registry as a public read-only attribute so callers
        # that need to register tools where dispatch will look them up
        # (e.g. AgentTeam's per-run delegate tool) can find the right
        # one regardless of whether this backend was constructed by the
        # runtime or injected with its own registry.
        self._emitter: EventEmitter = event_emitter or LogEventEmitter()
        # If the user passes a tool_executor, it carries its own emitter; we
        # don't second-guess. Otherwise build one that shares ours so both
        # backend and tool events flow through the same sink.
        self._tool_executor: ToolExecutor = tool_executor or ToolExecutor(
            self._tool_registry, event_emitter=self._emitter
        )
        self._tasks: dict[str, asyncio.Task[AgentResult[BaseModel]]] = {}
        self._killed: set[str] = set()

    @property
    def tool_registry(self) -> ToolRegistry:
        """The :class:`ToolRegistry` this backend resolves tools against
        at dispatch. Shared with :class:`AgentRuntime` when the runtime
        constructed this backend; otherwise (when injected) the registry
        the backend was given at construction. Callers needing to
        register tools that dispatch will see — e.g. ``AgentTeam``'s
        per-run ``delegate`` tool — go through this property.
        """
        return self._tool_registry

    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> AgentHandle:
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.AGENT_SPAWNED,
                agent_name=agent.name,
                task_id=task.id,
                trace_id=task.request_id,
                parent_trace_id=context.parent_trace_id,
                payload={
                    "backend": self.name,
                    "trust_level": agent.trust_level.value,
                },
            )
        )
        handle = AgentHandle(agent_name=agent.name, task_id=task.id, backend=self.name)
        self._tasks[handle.handle_id] = asyncio.create_task(
            self._execute(agent, task, context, handle),
            name=f"murmur-thread:{handle.handle_id}",
        )
        return handle

    async def status(self, handle: AgentHandle) -> BackendStatus:
        if handle.handle_id in self._killed:
            return BackendStatus.KILLED
        task = self._tasks.get(handle.handle_id)
        if task is None:
            raise SpawnError(f"unknown handle {handle.handle_id!r}")
        if not task.done():
            return BackendStatus.RUNNING
        if task.cancelled():
            return BackendStatus.KILLED
        if task.exception() is not None:
            return BackendStatus.FAILED
        return (
            BackendStatus.COMPLETED if task.result().is_ok() else BackendStatus.FAILED
        )

    async def kill(self, handle: AgentHandle) -> None:
        task = self._tasks.get(handle.handle_id)
        self._killed.add(handle.handle_id)
        if task is None or task.done():
            return
        task.cancel()
        # Idempotent — swallow the cancellation; result() owners see KILLED.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def result(self, handle: AgentHandle) -> AgentResult[BaseModel]:
        task = self._tasks.get(handle.handle_id)
        if task is None:
            raise SpawnError(f"unknown handle {handle.handle_id!r}")
        try:
            return await task
        except asyncio.CancelledError:
            return AgentResult[BaseModel](
                output=None,
                error=SpawnError(f"agent {handle.agent_name!r} was killed"),
                metadata=ResultMetadata(backend=self.name),
                agent_name=handle.agent_name,
                task_id=handle.task_id,
            )

    async def gather(
        self,
        agent: Agent,
        tasks: Sequence[TaskSpec],
        context: AgentContext | None = None,
        *,
        max_concurrency: int = 100,
    ) -> list[AgentResult[BaseModel]]:
        """Fan ``agent`` across ``tasks`` using an ``asyncio.Queue`` worker pool.

        Each task gets its own context prep via ``agent.context_passer``
        (matching :meth:`AgentRuntime.run` semantics). Per-task failures land
        in their slot's :attr:`AgentResult.error` — this method never raises
        on partial-failure batches. Results come back in input order.

        ``context`` carries the cascading-spawn parent linkage shared across
        every slot — :meth:`AgentRuntime.gather` derives it from the calling
        spawn frame so per-task events emit a ``parent_trace_id``. ``None``
        (default) is a top-level batch with no parent.
        """
        if not tasks:
            return []
        base_context = context if context is not None else AgentContext()
        queue: asyncio.Queue[tuple[int, TaskSpec]] = asyncio.Queue()
        for index, task in enumerate(tasks):
            queue.put_nowait((index, task))

        results: dict[int, AgentResult[BaseModel]] = {}

        async def worker() -> None:
            while True:
                try:
                    index, task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    ctx = await agent.context_passer.prepare(base_context, task)
                    # Re-overlay the cascading-spawn bookkeeping after the
                    # context-passer (which is free to drop conversation
                    # history but must not drop parent linkage).
                    ctx = ctx.model_copy(
                        update={
                            "depth": base_context.depth,
                            "parent_agent": base_context.parent_agent,
                            "parent_trace_id": base_context.parent_trace_id,
                            "ancestors": base_context.ancestors,
                        }
                    )
                    handle = await self.spawn(agent, task, ctx)
                    results[index] = await self.result(handle)
                except Exception as exc:  # safety net — _execute swallows already
                    results[index] = AgentResult[BaseModel](
                        output=None,
                        error=SpawnError(f"agent {agent.name!r} failed: {exc}"),
                        metadata=ResultMetadata(backend=self.name),
                        agent_name=agent.name,
                        task_id=task.id,
                    )

        pool_size = min(max(max_concurrency, 1), len(tasks))
        workers = [asyncio.create_task(worker()) for _ in range(pool_size)]
        await asyncio.gather(*workers)
        return [results[i] for i in range(len(tasks))]

    # ------------------------------------------------------------------ helpers

    async def _execute(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
        handle: AgentHandle,  # noqa: ARG002 — passed by the dispatcher; reserved for richer logging
    ) -> AgentResult[BaseModel]:
        start = time.perf_counter()
        # Push a slot-local spawn frame so any ``runtime.run`` issued by
        # this agent's tool loop sees this run as the parent. ``runtime.run``
        # already pushes the same frame for its own dispatch path, but
        # ``runtime.gather``'s pool workers don't — putting the push here
        # covers both call sites uniformly. Idempotent re-push for the
        # ``run()`` path (same agent, same context, same trace_id).
        from murmur.runtime import _current_spawn, _SpawnFrame

        spawn_token = _current_spawn.set(
            _SpawnFrame(
                agent_name=agent.name,
                agent_context=context,
                trace_id=task.request_id,
            )
        )
        structlog.contextvars.bind_contextvars(
            request_id=task.request_id,
            agent_name=agent.name,
            task_id=task.id,
            backend=self.name,
            trust_level=agent.trust_level.value,
        )
        try:
            try:
                pa_input = _apply_pre_hooks(agent, task)
                pa_agent = await self._build_pa_agent(agent, agent.tools, task.id)
                pa_result = await pa_agent.run(pa_input)
                output = _apply_post_hooks(agent, pa_result.output)
                duration_ms = int((time.perf_counter() - start) * 1000)
                tokens_used = _extract_tokens(pa_result)
                await self._emitter.emit(
                    RuntimeEvent(
                        event_type=EventType.AGENT_COMPLETED,
                        agent_name=agent.name,
                        task_id=task.id,
                        trace_id=task.request_id,
                        parent_trace_id=context.parent_trace_id,
                        payload={
                            "duration_ms": duration_ms,
                            "tokens_used": tokens_used,
                            "backend": self.name,
                        },
                    )
                )
                return AgentResult[BaseModel](
                    output=output,
                    metadata=ResultMetadata(
                        duration_ms=duration_ms,
                        tokens_used=tokens_used,
                        backend=self.name,
                    ),
                    agent_name=agent.name,
                    task_id=task.id,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                duration_ms = int((time.perf_counter() - start) * 1000)
                wrapped: SpawnError = (
                    exc
                    if isinstance(exc, SpawnError)
                    else SpawnError(f"agent {agent.name!r} failed: {exc}")
                )
                await self._emitter.emit(
                    RuntimeEvent(
                        event_type=EventType.AGENT_FAILED,
                        agent_name=agent.name,
                        task_id=task.id,
                        trace_id=task.request_id,
                        parent_trace_id=context.parent_trace_id,
                        payload={
                            "duration_ms": duration_ms,
                            "error": str(wrapped),
                            "backend": self.name,
                        },
                    )
                )
                return AgentResult[BaseModel](
                    output=None,
                    error=wrapped,
                    metadata=ResultMetadata(duration_ms=duration_ms, backend=self.name),
                    agent_name=agent.name,
                    task_id=task.id,
                )
        finally:
            structlog.contextvars.unbind_contextvars(
                "request_id", "agent_name", "task_id", "backend", "trust_level"
            )
            _current_spawn.reset(spawn_token)

    async def _build_pa_agent(
        self,
        agent: Agent,
        allowed: frozenset[str],
        task_id: str,
    ) -> pydantic_ai.Agent[None, Any]:
        return await build_pydantic_ai_agent(
            agent=agent,
            allowed=allowed,
            registry=self._tool_registry,
            executor=self._tool_executor,
            task_id=task_id,
        )


def _apply_pre_hooks(agent: Agent, task: TaskSpec) -> str:
    """Run ``agent.pre_process`` over ``task.input``.

    If ``agent.input_type`` is set, the raw string is first parsed into that
    type before the hooks run; the final result is re-serialised to JSON for
    PydanticAI's ``run`` (which always wants a string user prompt).
    """
    payload: object = task.input
    if agent.input_type is not None and isinstance(payload, str):
        payload = agent.input_type.model_validate_json(payload)
    for hook in agent.pre_process:
        payload = hook(payload)
    if isinstance(payload, str):
        return payload
    if isinstance(payload, BaseModel):
        return payload.model_dump_json()
    return str(payload)


def _apply_post_hooks(agent: Agent, output: BaseModel) -> BaseModel:
    """Run ``agent.post_process`` over the LLM output.

    Hooks are same-type ``(output_type) -> output_type`` — we trust the user's
    declaration; deviating from the contract is caught by ``ty`` at the call
    site, not by Murmur at runtime.
    """
    current: BaseModel = output
    for hook in agent.post_process:
        current = hook(current)
    return current


def _extract_tokens(pa_result: object) -> int:
    """Pull the total-token count off a ``pydantic_ai`` run result.

    PydanticAI exposes usage as a ``RunUsage`` with ``input_tokens`` /
    ``output_tokens``. We sum them; older / future shapes that expose
    ``total_tokens`` directly also work via ``getattr``.
    """
    usage_fn = getattr(pa_result, "usage", None)
    if usage_fn is None:
        return 0
    usage = usage_fn() if callable(usage_fn) else usage_fn
    total = getattr(usage, "total_tokens", None)
    if isinstance(total, int):
        return total
    inp = int(getattr(usage, "input_tokens", 0) or 0)
    out = int(getattr(usage, "output_tokens", 0) or 0)
    return inp + out


__all__ = ["AsyncBackend"]
