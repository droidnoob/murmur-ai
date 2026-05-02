"""``murmur.AgentRuntime`` — the front door.

Constructs the pipeline, picks a backend (:class:`AsyncBackend` for local,
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
import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict

from murmur._sync import reject_if_in_event_loop
from murmur.backends._faststream_broker import FastStreamBroker
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.backends.job import JobBackend
from murmur.core.errors import RegistryError, SpecValidationError
from murmur.core.pipeline import Pipeline, PipelineContext
from murmur.middleware.cost_tracking import CostTrackingMiddleware, TokenBudget
from murmur.middleware.depth_limit import DepthLimitMiddleware
from murmur.middleware.retry import RetryMiddleware
from murmur.middleware.timeout import TimeoutMiddleware
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import AgentContext, AgentResult, TaskSpec

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.agent import Agent
    from murmur.core.protocols.backend import Backend
    from murmur.core.protocols.broker import Broker
    from murmur.core.protocols.events import EventEmitter
    from murmur.core.protocols.registry import Registry
    from murmur.core.protocols.toolsets import ToolsetProvider
    from murmur.groups.spec import AgentGroup


_FASTSTREAM_SCHEMES: frozenset[str] = frozenset({"kafka", "nats", "amqp", "redis"})
_KNOWN_SCHEMES: frozenset[str] = frozenset({"memory"}) | _FASTSTREAM_SCHEMES


class RuntimeOptions(BaseModel):
    """Frozen tuning knobs for :class:`AgentRuntime`.

    Wraps the per-run middleware pipeline (timeout, retry, depth-limit) so
    callers can dial individual knobs without subclassing or constructing
    middleware directly. Defaults are safe — timeout is generous, retry is
    off, depth limit only kicks in for cascading sub-agent spawns.

    >>> runtime = AgentRuntime(options=RuntimeOptions(timeout_seconds=60))
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    timeout_seconds: float = 300.0
    """Cancel the run after this long. ``TimeoutMiddleware`` translates the
    underlying :class:`asyncio.TimeoutError` into a :class:`SpawnError`."""

    max_spawn_depth: int = 4
    """Cap on cascading agent spawns — rejects runs whose
    ``AgentContext.depth`` is already at or above this value. Top-level
    runs have depth 0, so the limit only matters once the sub-agent
    spawning path is in use."""

    retry_max_attempts: int = 1
    """``1`` (default) means no retry. Set to ``2+`` to enable
    :class:`RetryMiddleware` on transient :class:`SpawnError`."""

    retry_backoff_factor: float = 1.5
    """Multiplicative backoff between retries
    (``backoff_factor ** attempt`` seconds)."""

    token_budget: TokenBudget | None = None
    """Optional token-cost ceiling for the runtime. ``None`` (default)
    disables cost tracking. Construct via
    :class:`murmur.middleware.TokenBudget(limit=...)`. Once the budget is
    exhausted, subsequent ``runtime.run`` / ``runtime.gather`` calls fail
    with :class:`BudgetExceededError` before dispatch and emit a
    :data:`EventType.BUDGET_EXCEEDED` event."""

    mcp_eager_start: bool = False
    """Hold MCP toolset providers open across runs via supervisor tasks.

    Default ``False`` — every dispatch re-enters the MCP server's context
    (PydanticAI does this internally on each ``list_tools`` /
    ``direct_call_tool``), respawning the stdio subprocess each time.
    Cheap for low-frequency calls; wasteful at high throughput.

    When ``True``, :class:`AgentRuntime` spawns one supervisor task per
    provider on first dispatch. The supervisor enters the provider's
    context once (spawning the subprocess), holds the entry open until
    :meth:`AgentRuntime.shutdown` signals shutdown, then releases it.
    Other dispatch calls re-enter the same context — PydanticAI's
    :class:`MCPServer` ref-counts entries, so the inner enter / exit
    pairs are no-ops while the supervisor holds the outer entry.

    Because anyio cancel scopes are task-bound, ``__aenter__`` and
    ``__aexit__`` must run on the same asyncio task; the supervisor
    pattern guarantees this. Always pair with a :meth:`shutdown` call
    (or rely on :class:`AgentRouter` / :class:`AgentServer` lifespan,
    which call it automatically) — otherwise the held subprocess leaks
    until process exit."""


class AgentRuntime:
    """The orchestration runtime.

    >>> runtime = AgentRuntime()                       # AsyncBackend
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
        options: RuntimeOptions | None = None,
        event_emitter: EventEmitter | None = None,
        publish_events: bool = False,
    ) -> None:
        from murmur.events.log import LogEventEmitter

        self._registry = registry
        self._tool_registry: ToolRegistry = tool_registry or ToolRegistry()
        # Default emitter forwards every event to structlog with the same
        # event names previously used by direct ``log.ainfo`` calls — opting
        # out (e.g. ``MultiEventEmitter([])``) means no observability output.
        self._emitter: EventEmitter = event_emitter or LogEventEmitter()
        self._tool_executor: ToolExecutor = tool_executor or ToolExecutor(
            self._tool_registry, event_emitter=self._emitter
        )
        self._runtime_id: str = runtime_id or str(uuid.uuid4())
        self._publish_events: bool = publish_events
        self._backend: Backend = backend or self._build_backend(
            broker_url=broker, broker_instance=broker_instance
        )
        self._options: RuntimeOptions = options or RuntimeOptions()
        # Providers seen via ``_resolve`` — kept for shutdown cleanup. Object
        # identity is the right key (Protocol instances aren't hashable in
        # general, but our concrete is a regular class).
        self._mcp_providers: list[ToolsetProvider] = []
        # Eager-start (mp5) bookkeeping. One supervisor task per provider
        # holds the MCP server's context open across dispatches; the inner
        # PA-MCP entry/exit pairs become no-ops via the upstream
        # ``_running_count`` ref-counting. Entries keyed by ``id(provider)``
        # because :class:`ToolsetProvider` Protocol instances aren't always
        # hashable.
        self._mcp_warm_events: dict[int, asyncio.Event] = {}
        self._mcp_shutdown_events: dict[int, asyncio.Event] = {}
        self._mcp_supervisor_tasks: dict[int, asyncio.Task[None]] = {}
        self._mcp_supervisor_errors: dict[int, BaseException] = {}
        self._mcp_warm_lock: asyncio.Lock | None = None

    @property
    def event_emitter(self) -> EventEmitter:
        """The runtime's event sink. Pass ``event_emitter=`` at init to
        substitute a custom one (e.g. ``MultiEventEmitter`` for SSE +
        log fan-out)."""
        return self._emitter

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def runtime_id(self) -> str:
        return self._runtime_id

    @property
    def tool_registry(self) -> ToolRegistry:
        return self._tool_registry

    @property
    def options(self) -> RuntimeOptions:
        return self._options

    async def run(
        self,
        agent: Agent | str,
        task: TaskSpec,
    ) -> AgentResult[BaseModel]:
        """Run a single agent against a single task. Returns a typed result.

        Wires the configured middleware (timeout, depth-limit, optional
        retry) around backend dispatch via :class:`Pipeline`. ``gather`` is
        unaffected — its per-slot path bypasses middleware to keep batch
        semantics simple. Tune per-run behavior via :class:`RuntimeOptions`
        passed to :meth:`__init__`.
        """
        resolved = self._resolve(agent)
        await self._warm_mcp_providers(resolved)
        agent_context = AgentContext()

        async def dispatch_stage(
            ctx: PipelineContext,
            _next: object,  # terminal — never invoked
        ) -> AgentResult[BaseModel]:
            prepared = await resolved.context_passer.prepare(ctx.agent_context, task)
            handle = await self._backend.spawn(resolved, task, prepared)
            return await self._backend.result(handle)

        stages: list[object] = [
            TimeoutMiddleware(self._options.timeout_seconds),
            DepthLimitMiddleware(self._options.max_spawn_depth),
        ]
        if self._options.token_budget is not None:
            # Pre-check + post-charge against the runtime-wide budget. Built
            # per-spawn so the closure carries this run's emitter for the
            # BUDGET_EXCEEDED emission.
            stages.append(
                CostTrackingMiddleware(
                    self._options.token_budget,
                    event_emitter=self._emitter,
                )
            )
        if self._options.retry_max_attempts > 1:
            stages.append(
                RetryMiddleware(
                    max_attempts=self._options.retry_max_attempts,
                    backoff_factor=self._options.retry_backoff_factor,
                )
            )
        stages.append(dispatch_stage)

        pipeline = Pipeline[AgentResult[BaseModel]](cast("list[Any]", stages))
        ctx = PipelineContext(
            task=task, agent_name=resolved.name, agent_context=agent_context
        )
        return await pipeline.run(ctx)

    async def gather(
        self,
        agent: Agent | str,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,
        fail_fast: bool = False,
    ) -> list[AgentResult[BaseModel]]:
        """Fan a single agent across many tasks. Bounded by ``max_concurrency``.

        Delegates to ``backend.gather`` when the backend implements one
        (``AsyncBackend`` uses an ``asyncio.Queue`` + worker pool;
        ``JobBackend`` publishes via the ``ResultCollector``). Falls
        back to a semaphore-bounded fan-out otherwise. **Default
        (``fail_fast=False``)**: per-task failures always land in their
        slot's :attr:`AgentResult.error` — never raises on partial failure.
        **``fail_fast=True``**: re-raises the first task's error from the
        gathered slots after the batch settles (we still wait for in-flight
        tasks to finish so partial results aren't dropped).
        """
        if max_concurrency < 1:
            raise SpecValidationError("max_concurrency must be >= 1")
        resolved = self._resolve(agent)
        await self._warm_mcp_providers(resolved)

        from murmur.events.types import EventType, RuntimeEvent

        # Use the first task's request_id as the batch's trace_id when
        # available; otherwise fall back to the runtime_id (a batch with no
        # tasks is rejected upstream by the empty-list short-circuit).
        batch_trace_id = tasks[0].request_id if tasks else self._runtime_id
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.BATCH_STARTED,
                agent_name=resolved.name,
                trace_id=batch_trace_id,
                payload={
                    "task_count": len(tasks),
                    "max_concurrency": max_concurrency,
                },
            )
        )

        backend_gather = getattr(self._backend, "gather", None)
        if callable(backend_gather):
            results = await backend_gather(
                resolved, tasks, max_concurrency=max_concurrency
            )
        else:
            results = await self._fallback_gather(resolved, tasks, max_concurrency)

        success_count = sum(1 for r in results if r.is_ok())
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.BATCH_COMPLETED,
                agent_name=resolved.name,
                trace_id=batch_trace_id,
                payload={
                    "task_count": len(tasks),
                    "success_count": success_count,
                    "failure_count": len(results) - success_count,
                },
            )
        )

        if fail_fast:
            for r in results:
                if r.error is not None:
                    raise r.error
        return results

    async def _fallback_gather(
        self,
        resolved: Agent,
        tasks: Sequence[TaskSpec],
        max_concurrency: int,
    ) -> list[AgentResult[BaseModel]]:
        """Semaphore-bounded fan-out when the backend has no ``gather``.

        Wraps each task in try/except so a per-task failure becomes an
        :class:`AgentResult` with ``error`` set rather than propagating —
        matches the spec for :meth:`gather` (default ``fail_fast=False``).
        """
        from murmur.core.errors import SpawnError
        from murmur.types import ResultMetadata

        sem = asyncio.Semaphore(max_concurrency)

        async def _one(t: TaskSpec) -> AgentResult[BaseModel]:
            async with sem:
                try:
                    return await self.run(resolved, t)
                except Exception as exc:
                    return AgentResult[BaseModel](
                        output=None,
                        error=SpawnError(f"agent {resolved.name!r} failed: {exc}"),
                        metadata=ResultMetadata(
                            backend=self._backend.__class__.__name__
                        ),
                        agent_name=resolved.name,
                        task_id=t.id,
                    )

        return await asyncio.gather(*[_one(t) for t in tasks])

    def run_sync(
        self,
        agent: Agent | str,
        task: TaskSpec,
    ) -> AgentResult[BaseModel]:
        """Blocking variant of :meth:`run` for notebook / REPL / script use.

        Internally :func:`asyncio.run`. **Cannot be called from inside a
        running event loop** — raises :class:`RuntimeError` instead, with
        a pointer to the async variant. Mirrors PydanticAI's
        ``Agent.run_sync`` and the rest of the project's sync API surface.
        """
        reject_if_in_event_loop("run_sync")
        return asyncio.run(self.run(agent, task))

    def gather_sync(
        self,
        agent: Agent | str,
        tasks: Sequence[TaskSpec],
        *,
        max_concurrency: int = 100,
        fail_fast: bool = False,
    ) -> list[AgentResult[BaseModel]]:
        """Blocking variant of :meth:`gather`. Same caller restrictions as
        :meth:`run_sync`."""
        reject_if_in_event_loop("gather_sync")
        return asyncio.run(
            self.gather(
                agent,
                tasks,
                max_concurrency=max_concurrency,
                fail_fast=fail_fast,
            )
        )

    async def run_group(
        self,
        group: AgentGroup,
        task: TaskSpec,
    ) -> AgentResult[BaseModel]:
        """Walk an ``AgentGroup`` topology against ``task``.

        Returns the terminal agent's result. Failed slots in fan-out tiers
        are filtered before downstream mappers run; if every slot in a tier
        fails, raises :class:`murmur.core.errors.AllAgentsFailedError`.

        Emits :data:`EventType.GROUP_STARTED` before traversal and
        :data:`EventType.GROUP_COMPLETED` after the terminal result settles.
        Per-agent events (``AGENT_SPAWNED``, ``AGENT_COMPLETED`` etc.) come
        from each step's underlying :meth:`run` call.
        """
        # Imported lazily to keep ``murmur.groups`` optional-feeling and
        # avoid circular import at module load time.
        from murmur.events.types import EventType, RuntimeEvent
        from murmur.groups.runner import run_group as _run_group

        start = time.perf_counter()
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.GROUP_STARTED,
                agent_name=group.name,
                task_id=task.id,
                trace_id=task.request_id,
                payload={"node_count": len(group.topology)},
            )
        )
        try:
            return await _run_group(self, group, task)
        finally:
            duration_ms = int((time.perf_counter() - start) * 1000)
            await self._emitter.emit(
                RuntimeEvent(
                    event_type=EventType.GROUP_COMPLETED,
                    agent_name=group.name,
                    task_id=task.id,
                    trace_id=task.request_id,
                    payload={"duration_ms": duration_ms},
                )
            )

    # ------------------------------------------------------------------ helpers

    def _resolve(self, agent: Agent | str) -> Agent:
        if isinstance(agent, str):
            if self._registry is None:
                raise RegistryError(
                    f"cannot resolve agent name {agent!r}: no registry configured"
                )
            resolved = self._registry.get(agent)
        else:
            resolved = agent
        self._track_mcp_providers(resolved)
        return resolved

    def _track_mcp_providers(self, agent: Agent) -> None:
        """Remember any MCP providers an agent brings so :meth:`shutdown`
        can stop them later. Idempotent — duplicates are filtered."""
        for provider in agent.mcp_servers:
            if provider not in self._mcp_providers:
                self._mcp_providers.append(provider)

    async def _warm_mcp_providers(self, agent: Agent) -> None:
        """Eager-start each MCP provider on the agent (mp5).

        Spawns one supervisor task per provider that holds the MCP
        server's context open until :meth:`shutdown` fires the
        per-provider shutdown event. Inner dispatch calls
        (``list_tools`` / ``direct_call_tool``) become no-op
        ``__aenter__`` / ``__aexit__`` pairs via PydanticAI's upstream
        ref-counting — the actual subprocess stays warm.

        No-op when :attr:`RuntimeOptions.mcp_eager_start` is False (the
        default — preserves the per-call respawn behaviour). Concurrent
        first-dispatches of the same provider are deduplicated via
        ``_mcp_warm_lock`` so we never spawn two supervisors. If the
        first supervisor's ``provider.start()`` raises, the cached
        exception surfaces to every concurrent waiter so they all see
        the same failure rather than racing into a half-warmed state.
        """
        if not self._options.mcp_eager_start or not agent.mcp_servers:
            return
        if self._mcp_warm_lock is None:
            # ``asyncio.Lock`` must be constructed inside a running loop on
            # 3.11+; we lazy-init on first warm-up to dodge the constructor's
            # deprecation warning when there's no current loop.
            self._mcp_warm_lock = asyncio.Lock()
        for provider in agent.mcp_servers:
            await self._warm_one_provider(provider)

    async def _warm_one_provider(self, provider: ToolsetProvider) -> None:
        """Ensure exactly one supervisor task is running for ``provider``."""
        key = id(provider)
        # Fast path: already warm — wait on its event without lock contention.
        if key in self._mcp_warm_events:
            await self._mcp_warm_events[key].wait()
            err = self._mcp_supervisor_errors.get(key)
            if err is not None:
                raise err
            return
        assert self._mcp_warm_lock is not None  # set in _warm_mcp_providers
        async with self._mcp_warm_lock:
            # Re-check inside the lock — another task may have just spawned it.
            if key in self._mcp_warm_events:
                await self._mcp_warm_events[key].wait()
                err = self._mcp_supervisor_errors.get(key)
                if err is not None:
                    raise err
                return
            ready = asyncio.Event()
            shutdown = asyncio.Event()
            self._mcp_warm_events[key] = ready
            self._mcp_shutdown_events[key] = shutdown
            task = asyncio.create_task(
                self._supervise_provider(provider, key, ready, shutdown),
                name=f"murmur-mcp-supervisor-{provider.__class__.__name__}-{key}",
            )
            self._mcp_supervisor_tasks[key] = task
        await ready.wait()
        err = self._mcp_supervisor_errors.get(key)
        if err is not None:
            raise err

    async def _supervise_provider(
        self,
        provider: ToolsetProvider,
        key: int,
        ready: asyncio.Event,
        shutdown: asyncio.Event,
    ) -> None:
        """Hold ``provider`` open until ``shutdown`` is set.

        Runs as its own asyncio task so ``provider.start()`` and
        ``provider.stop()`` execute on the same task — anyio's cancel
        scopes won't accept cross-task entry/exit. Failures during
        ``start()`` are cached on ``_mcp_supervisor_errors[key]`` so
        concurrent waiters see the same exception.
        """
        try:
            try:
                await provider.start()
            except BaseException as exc:
                self._mcp_supervisor_errors[key] = exc
                ready.set()
                return
            ready.set()
            await shutdown.wait()
        finally:
            with contextlib.suppress(Exception):
                await provider.stop()

    async def shutdown(self) -> None:
        """Release runtime-owned resources.

        Three cleanup paths run in sequence:

        1. **Eager-start supervisors (mp5)** — when
           :attr:`RuntimeOptions.mcp_eager_start` is True, one supervisor
           task per provider holds the MCP context open. Setting each
           shutdown event lets the supervisors exit ``provider.stop()``
           on the *same* task that called ``provider.start()``, which is
           what anyio's cancel scopes require.
        2. **Manually pre-warmed providers** — providers a user
           pre-warmed by calling ``await provider.start()`` themselves
           get a ``stop()`` here as a safety net. Providers in eager-start
           mode are already stopped by their supervisor; the second
           ``stop()`` is a no-op.
        3. **Broker-mode runtimes** additionally need
           ``await backend.stop()`` — :class:`AgentServer` /
           :class:`AgentRouter` lifespan already drives that.
        """
        # Phase 1: signal every supervisor to exit and await their cleanup.
        # Capture the keyset BEFORE clearing so phase 2 can skip these
        # providers (their supervisor already called stop()).
        supervised_keys = set(self._mcp_supervisor_tasks)
        for shutdown_event in self._mcp_shutdown_events.values():
            shutdown_event.set()
        for task in self._mcp_supervisor_tasks.values():
            with contextlib.suppress(Exception):
                await task
        self._mcp_warm_events.clear()
        self._mcp_shutdown_events.clear()
        self._mcp_supervisor_tasks.clear()
        self._mcp_supervisor_errors.clear()

        # Phase 2: best-effort stop on any provider not covered by a
        # supervisor (e.g. user pre-warmed manually before mp5 was opt-in).
        # Filter out the supervised set — calling stop() twice is benign
        # (start_count==0 short-circuits) but counts towards stop_count
        # which tests assert on.
        for provider in self._mcp_providers:
            if id(provider) in supervised_keys:
                continue
            with contextlib.suppress(Exception):
                await provider.stop()

    def _build_backend(
        self,
        *,
        broker_url: str | None,
        broker_instance: Broker | None,
    ) -> Backend:
        # JobBackend always receives the runtime's emitter so AGENT_DISPATCHED
        # fires publisher-side on every broker dispatch — independent of the
        # ``publish_events`` bridge. The bridge only governs whether
        # worker-side events are *relayed back* over the broker.
        if broker_instance is not None:
            return JobBackend(
                broker=broker_instance,
                runtime_id=self._runtime_id,
                publish_events=self._publish_events,
                event_emitter=self._emitter,
            )
        if broker_url is None:
            if self._publish_events:
                raise SpecValidationError(
                    "publish_events=True requires a broker — pass broker= or "
                    "broker_instance= to AgentRuntime, or drop publish_events"
                )
            return AsyncBackend(
                tool_registry=self._tool_registry,
                tool_executor=self._tool_executor,
                event_emitter=self._emitter,
            )
        scheme = urlparse(broker_url).scheme
        if scheme not in _KNOWN_SCHEMES:
            raise SpecValidationError(
                f"unsupported broker URL scheme {scheme!r}; "
                f"expected one of {sorted(_KNOWN_SCHEMES)}"
            )
        broker = self._build_broker(scheme=scheme, url=broker_url)
        return JobBackend(
            broker=broker,
            runtime_id=self._runtime_id,
            broker_url=broker_url,
            publish_events=self._publish_events,
            event_emitter=self._emitter,
        )

    @staticmethod
    def _build_broker(*, scheme: str, url: str) -> Broker:
        if scheme == "memory":
            return InMemoryBroker()
        return FastStreamBroker(scheme=scheme, url=url)


__all__ = ["AgentRuntime"]
