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
import contextvars
import time
import uuid
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict

from murmur._sync import reject_if_in_event_loop
from murmur.backends._brokers import make_broker
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.backends.job import JobBackend
from murmur.core.errors import (
    DepthLimitError,
    RegistryError,
    SpawnCapError,
    SpawnCycleError,
    SpawnError,
    SpecValidationError,
)
from murmur.core.pipeline import Pipeline, PipelineContext
from murmur.middleware.cost_tracking import CostTrackingMiddleware, TokenBudget
from murmur.middleware.depth_limit import DepthLimitMiddleware
from murmur.middleware.retry import RetryMiddleware
from murmur.middleware.timeout import TimeoutMiddleware
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import AgentContext, AgentResult, GroupResult, TaskSpec

if TYPE_CHECKING:
    from collections.abc import Sequence

    from murmur.agent import Agent
    from murmur.core.protocols.backend import Backend
    from murmur.core.protocols.broker import Broker
    from murmur.core.protocols.events import EventEmitter
    from murmur.core.protocols.registry import Registry
    from murmur.core.protocols.toolsets import ToolsetProvider
    from murmur.groups.spec import AgentGroup
    from murmur.groups.team import AgentTeam


_FASTSTREAM_SCHEMES: frozenset[str] = frozenset({"kafka", "nats", "amqp", "redis"})
_KNOWN_SCHEMES: frozenset[str] = frozenset({"memory"}) | _FASTSTREAM_SCHEMES


class _SpawnFrame(BaseModel):
    """Currently-executing run's stack frame — what nested ``runtime.run``
    calls look up to derive their child :class:`AgentContext`.

    Carries the agent's own name (for the ancestor set), its context
    (for depth/ancestors composition), and its ``trace_id`` (so child
    events can attribute back through ``parent_trace_id``).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    agent_name: str
    agent_context: AgentContext
    trace_id: str


_current_spawn: contextvars.ContextVar[_SpawnFrame | None] = contextvars.ContextVar(
    "murmur_current_spawn", default=None
)
"""Per-task contextvar carrying the currently-executing run's
:class:`_SpawnFrame`. Set by :meth:`AgentRuntime.run` for the duration of a
dispatch; read by sub-spawn entry points (e.g. ``spawn_agents``) to derive
the child :class:`AgentContext` (depth + ancestors + parent_trace_id).

Lives at module level because ``contextvars`` are scoped per-task by Python's
asyncio runtime — a fresh value set in a ``Task`` doesn't leak back to its
parent. Cross-process / cross-worker propagation is intentionally out of
scope: cycle detection only meaningful within a single run, which always
executes inside one process even with :class:`JobBackend`."""


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

    max_total_spawns: int | None = None
    """Optional per-runtime kill switch for total dispatches over the
    runtime's lifetime. ``None`` (default) = unbounded — what long-lived
    workers and servers want.

    When set, every ``run()`` and every backend-native ``gather()`` slot
    decrements the budget; once exhausted, further dispatches fail with
    :class:`SpawnCapError` and the counter never resets. Independent of
    token budget — a runaway cascade hits this before the cost meter
    catches up. Use it as an explicit opt-in safety rail (e.g. tests that
    exercise the cap, or short-lived process boundaries) — leave it
    ``None`` for any runtime that handles ongoing traffic."""

    cycle_policy: Literal["strict", "permissive"] = "strict"
    """Cycle-detection policy for cascading sub-spawns.

    ``"strict"`` (default) rejects any ``runtime.run`` / ``runtime.gather``
    whose target ``Agent.name`` already appears on the parent chain
    (ancestors + immediate parent), raising :class:`SpawnCycleError`
    before any backend work. This is the safe default — bounded reuse
    patterns like ``reviewer → fact_checker → reviewer`` are
    structurally indistinguishable from runaway recursion at the
    runtime level, and most callers want the guard.

    ``"permissive"`` skips the cycle check entirely. **Termination
    becomes the caller's responsibility** — typically by tracking
    iteration counts in tool arguments / agent inputs, or by relying
    on :attr:`max_spawn_depth` and :attr:`max_total_spawns`, both of
    which remain enforced regardless of this setting. Use this when a
    legitimate workflow needs the same registered agent name to recur
    on the chain (e.g. a critic loop with an explicit external
    counter); avoid it on any runtime that runs untrusted prompts or
    where bugs in tool plumbing could let an LLM ask for the same
    agent forever."""

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

    broker_signing_key: bytes | None = None
    """Optional symmetric HMAC-SHA256 key for authenticating broker
    envelopes — opt-in. Default ``None`` preserves the documented "broker
    is trusted" baseline: no signature is computed or verified.

    When set on a publisher runtime, :class:`murmur.backends.JobBackend`
    signs every outbound :class:`murmur.messages.TaskMessage` over its
    safety-relevant fields (``agent_name``, ``request_id``,
    ``parent_spawn``) before publishing. The matching worker — built
    with :class:`murmur.worker.Worker(..., signing_key=...)` — verifies
    on receive and rejects mismatched / missing signatures with a
    structured failure :class:`murmur.messages.ResultMessage` so the
    publisher's :meth:`AgentRuntime.run` resolves cleanly with
    ``result.error`` set rather than the worker crashing.

    Recommended length is **at least 32 random bytes**
    (``secrets.token_bytes(32)``). Pass them as raw ``bytes`` — there is
    no key-derivation layer. For key rotation, the worker accepts a
    sequence of keys (``signing_key=(new, old)``) and verifies against
    any; the publisher always signs with one — roll new workers first,
    swap the publisher, then drop ``old``."""

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
        # Registry identity rule: the registry the executor consults at
        # execution-time fall-through MUST be the same object as the
        # registry the agent-build path reads from. Otherwise tool
        # registrations land on one view and execution misses them. Same
        # rule as ``AsyncBackend.__init__`` — see there for rationale.
        if tool_executor is not None and tool_registry is not None:
            if tool_executor.registry is not tool_registry:
                raise ValueError(
                    "AgentRuntime(tool_registry=..., tool_executor=...) requires "
                    "the two to share the same registry — otherwise registrations "
                    "on the registry are invisible at execution. Pass only one, "
                    "or ensure tool_executor.registry is tool_registry."
                )
            self._tool_registry: ToolRegistry = tool_registry
        elif tool_executor is not None:
            self._tool_registry = tool_executor.registry
        else:
            self._tool_registry = tool_registry or ToolRegistry()
        # Default emitter forwards every event to structlog with the same
        # event names previously used by direct ``log.ainfo`` calls — opting
        # out (e.g. ``MultiEventEmitter([])``) means no observability output.
        self._emitter: EventEmitter = event_emitter or LogEventEmitter()
        self._tool_executor: ToolExecutor = tool_executor or ToolExecutor(
            self._tool_registry, event_emitter=self._emitter
        )
        self._runtime_id: str = runtime_id or str(uuid.uuid4())
        self._publish_events: bool = publish_events
        # Options must land before ``_build_backend`` reads
        # ``broker_signing_key`` off them.
        self._options: RuntimeOptions = options or RuntimeOptions()
        self._backend: Backend = backend or self._build_backend(
            broker_url=broker, broker_instance=broker_instance
        )
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
        # Cascading-spawn tally — incremented on every successful pre-flight
        # check in ``run()`` (top-level + cascaded). Once it reaches
        # ``options.max_total_spawns`` further dispatches raise
        # :class:`SpawnCapError`. Lock-protected since multiple cascaded
        # children can race on the same runtime instance.
        self._spawn_count: int = 0
        self._spawn_count_lock: asyncio.Lock | None = None
        # Latch the construction-time identity invariant — with this set
        # later, ``__setattr__`` rejects attempts to swap the tool
        # registry / executor and re-introduce the divergence the
        # constructor guard was designed to prevent.
        self.__dict__["_init_complete"] = True

    # Names whose values must not change after ``__init__`` completes —
    # rebinding any of them would let a caller bypass the constructor's
    # registry/executor identity check.
    _LOCKED_AFTER_INIT: frozenset[str] = frozenset({"_tool_registry", "_tool_executor"})

    def __setattr__(self, name: str, value: object) -> None:
        # Resolve via ``type(self)`` so subclasses that extend
        # ``_LOCKED_AFTER_INIT`` with additional locked fields actually
        # get their override honoured.
        if (
            self.__dict__.get("_init_complete")
            and name in type(self)._LOCKED_AFTER_INIT
        ):
            raise AttributeError(
                f"{type(self).__name__}.{name} is immutable after "
                f"construction; swapping it would bypass the "
                f"registry/executor identity invariant established "
                f"in __init__"
            )
        super().__setattr__(name, value)

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

        Cascading-spawn semantics: when this call originates from inside
        another agent's run (the ``spawn_agents`` tool, for instance), the
        runtime reads the parent's frame from the ``_current_spawn``
        contextvar, derives the child :class:`AgentContext` (depth + 1,
        ancestors + parent_name, parent_trace_id = parent's trace_id), and
        rejects cycles before any backend work. The runtime's per-instance
        ``_spawn_count`` is incremented on every accepted dispatch and
        rejects further runs with :class:`SpawnCapError` once
        :attr:`RuntimeOptions.max_total_spawns` is reached.

        Cycle detection is **name-based**, not run-id-based. ``Agent.name``
        is the registry key and the canonical identity for an agent in
        Murmur's model — two ``runtime.run`` calls on the same agent with
        different inputs are still the same agent re-entering itself, and
        that's the runaway case we want to catch. If a workflow needs the
        same logic at a deeper level, give the deeper instance a distinct
        name (``worker-rev2`` etc.) — that disambiguates intent and keeps
        the cycle guard meaningful. For workflows that genuinely require
        bounded reuse of a single registered name on the chain, opt into
        :attr:`RuntimeOptions.cycle_policy` ``"permissive"`` and own
        termination yourself (depth + cap remain enforced).
        """
        resolved = self._resolve(agent)
        await self._warm_mcp_providers(resolved)

        # Cycle: name already on the parent chain → reject before claiming
        # a spawn slot or warming any backend state. Chain computation
        # stays unconditional so depth / ancestors propagation below
        # is identical across both policies — only the raise is gated.
        parent_frame = _current_spawn.get()
        if parent_frame is not None and self._options.cycle_policy == "strict":
            chain = parent_frame.agent_context.ancestors | {parent_frame.agent_name}
            if resolved.name in chain:
                raise SpawnCycleError(
                    f"agent {resolved.name!r} is already on the spawn chain "
                    f"({sorted(chain)}); cascading would form a cycle"
                )

        if parent_frame is None:
            agent_context = AgentContext()
        else:
            agent_context = AgentContext(
                depth=parent_frame.agent_context.depth + 1,
                parent_agent=parent_frame.agent_name,
                parent_trace_id=parent_frame.trace_id,
                ancestors=parent_frame.agent_context.ancestors
                | {parent_frame.agent_name},
            )

        async def claim_stage(
            ctx: PipelineContext,
            next_stage: Any,
        ) -> AgentResult[BaseModel]:
            # Spawn cap is charged AFTER pre-dispatch validation
            # (DepthLimit, CostTracking) so a rejected run never burns a
            # slot. Sits inside the Retry boundary deliberately —
            # RetryMiddleware re-invokes the next stage on backend
            # failure, but this stage runs once per ``run()`` because
            # Retry wraps it (not the other way around).
            await self._claim_spawn_slot(ctx.agent_name)
            return await next_stage(ctx)

        async def dispatch_stage(
            ctx: PipelineContext,
            _next: object,  # terminal — never invoked
        ) -> AgentResult[BaseModel]:
            prepared = await resolved.context_passer.prepare(ctx.agent_context, task)
            # Re-overlay the cascading-spawn bookkeeping (depth / ancestors /
            # parent linkage). These fields are runtime-owned, not
            # context-passer territory — a NullContextPasser that returns
            # ``AgentContext()`` must not be allowed to wipe parent linkage
            # or downstream ``parent_trace_id`` / cycle detection breaks.
            prepared = prepared.model_copy(
                update={
                    "depth": ctx.agent_context.depth,
                    "parent_agent": ctx.agent_context.parent_agent,
                    "parent_trace_id": ctx.agent_context.parent_trace_id,
                    "ancestors": ctx.agent_context.ancestors,
                }
            )
            handle = await self._backend.spawn(resolved, task, prepared)
            return await self._backend.result(handle)

        # Pipeline ordering — outside-in:
        #
        #   Timeout
        #     DepthLimit       ← rejects before claim_stage
        #     CostTracking?    ← rejects before claim_stage (pre-check arm)
        #     ClaimSlot        ← cap charged here, not at run() entry
        #     Retry?           ← retries dispatch_stage only
        #       dispatch_stage ← backend handoff
        #
        # Putting ClaimSlot after DepthLimit and CostTracking guarantees
        # locally-rejected work doesn't burn ``max_total_spawns``. Putting
        # it above Retry guarantees one slot per user-visible ``run()``,
        # not one slot per retry attempt.
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
        stages.append(claim_stage)
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
        # Publish this run as the parent frame for any cascaded
        # ``runtime.run`` calls fired from inside the agent's tool loop.
        # Reset on exit so concurrent siblings under one grandparent don't
        # see each other as ancestors.
        token = _current_spawn.set(
            _SpawnFrame(
                agent_name=resolved.name,
                agent_context=agent_context,
                trace_id=task.request_id,
            )
        )
        try:
            return await pipeline.run(ctx)
        finally:
            _current_spawn.reset(token)

    async def _claim_spawn_slot(self, agent_name: str) -> None:
        """Atomic check-and-increment against ``options.max_total_spawns``.

        When the cap is ``None`` (default) the counter still increments —
        useful for observability — but no rejection ever fires. When the
        cap is set, rejects with :class:`SpawnCapError` once exhausted; the
        counter never resets, so the runtime stays sealed until restart.
        Lazy lock binds to the current event loop.
        """
        await self._claim_spawn_batch(agent_name, 1)

    async def _claim_spawn_batch(self, agent_name: str, count: int) -> None:
        """Atomic batch-level check-and-increment.

        Either claims all ``count`` slots or none — never partially. Used
        by :meth:`gather` so an oversized batch can't burn through a
        finite ``max_total_spawns`` and brick subsequent dispatches when
        the request itself never executes.
        """
        if count < 0:
            raise SpecValidationError("count must be >= 0")
        if count == 0:
            return
        if self._spawn_count_lock is None:
            self._spawn_count_lock = asyncio.Lock()
        async with self._spawn_count_lock:
            cap = self._options.max_total_spawns
            if cap is not None and self._spawn_count + count > cap:
                remaining = max(cap - self._spawn_count, 0)
                raise SpawnCapError(
                    f"runtime spawn cap would be exceeded "
                    f"({cap} total spawns; {remaining} remaining; "
                    f"requested {count}); refusing to dispatch "
                    f"{agent_name!r}"
                )
            self._spawn_count += count

    @property
    def spawn_count(self) -> int:
        """Total dispatches accepted by this runtime (top-level + cascaded).

        Read-only — useful for tests and observability. Compare against
        :attr:`RuntimeOptions.max_total_spawns` to gauge headroom.
        """
        return self._spawn_count

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

        :attr:`RuntimeOptions.timeout_seconds` applies to the whole batch
        (matching :meth:`run`'s pipeline-level wrapping). When the wall
        clock fires before the backend gather settles, the
        :class:`asyncio.TimeoutError` is translated into
        :class:`SpawnError` — slots already claimed against
        ``max_total_spawns`` stay claimed.
        """
        if max_concurrency < 1:
            raise SpecValidationError("max_concurrency must be >= 1")
        resolved = self._resolve(agent)
        await self._warm_mcp_providers(resolved)

        # Cycle: same name in-chain rule that ``run`` enforces. Reject before
        # claiming any slots so the cap stays honest on cycle rejection.
        # Chain computation stays unconditional so per-slot depth /
        # ancestors propagation below is identical across both policies —
        # only the raise is gated by ``cycle_policy``.
        parent_frame = _current_spawn.get()
        if parent_frame is not None and self._options.cycle_policy == "strict":
            chain = parent_frame.agent_context.ancestors | {parent_frame.agent_name}
            if resolved.name in chain:
                raise SpawnCycleError(
                    f"agent {resolved.name!r} is already on the spawn chain "
                    f"({sorted(chain)}); gather would form a cycle"
                )

        # Per-slot context: every gathered slot shares one parent frame.
        if parent_frame is None:
            slot_context = AgentContext()
        else:
            slot_context = AgentContext(
                depth=parent_frame.agent_context.depth + 1,
                parent_agent=parent_frame.agent_name,
                parent_trace_id=parent_frame.trace_id,
                ancestors=parent_frame.agent_context.ancestors
                | {parent_frame.agent_name},
            )

        # Depth: ``gather`` doesn't go through the pipeline (no
        # DepthLimitMiddleware on the batch path), so enforce the same
        # rule inline. Mirrors ``DepthLimitMiddleware.__call__`` —
        # rejects when the slot's depth would equal or exceed the cap.
        # Without this, a parent at ``depth = max_spawn_depth - 1``
        # could fan out children at the cap (or deeper, if those
        # children gather again) and bypass the recursion guard
        # entirely.
        if slot_context.depth >= self._options.max_spawn_depth:
            raise DepthLimitError(
                f"cascading-spawn depth {slot_context.depth} exceeds limit "
                f"{self._options.max_spawn_depth} (gather agent={resolved.name})"
            )

        from murmur.events.types import EventType, RuntimeEvent

        # Use the first task's request_id as the batch's trace_id when
        # available; otherwise fall back to the runtime_id (a batch with no
        # tasks is rejected upstream by the empty-list short-circuit).
        batch_trace_id = tasks[0].request_id if tasks else self._runtime_id

        # Token budget pre-check: ``run`` enforces this through
        # ``CostTrackingMiddleware``, but ``gather`` doesn't go through
        # the pipeline. Mirror the gate inline — fail closed when the
        # budget is already exhausted, before claiming any slots, so a
        # batch dispatched on an empty budget never burns cap or fires
        # work. Aggregate post-charge happens after results come back.
        budget = self._options.token_budget
        if budget is not None and budget.remaining <= 0:
            await self._emitter.emit(
                RuntimeEvent(
                    event_type=EventType.BUDGET_EXCEEDED,
                    agent_name=resolved.name,
                    trace_id=batch_trace_id,
                    parent_trace_id=slot_context.parent_trace_id,
                    payload={
                        "limit": budget.limit,
                        "used": budget.used,
                        "scope": "runtime",
                        "batch": True,
                        "task_count": len(tasks),
                    },
                )
            )
            from murmur.core.errors import BudgetExceededError

            raise BudgetExceededError(
                f"token budget exhausted before gather agent={resolved.name!r} "
                f"(limit={budget.limit}, used={budget.used}, "
                f"task_count={len(tasks)})"
            )

        # Cap: backend-native ``gather`` bypasses ``run`` and thus the
        # per-call cap charge. Apply the cap atomically at the batch level
        # so an oversized request fails closed without mutating the counter
        # — otherwise an explicit ``max_total_spawns`` could be permanently
        # exhausted by a single rejected ``gather`` call (Codex review).
        # ``_fallback_gather`` still charges per-call via ``self.run``, so
        # the per-slot claim only runs on the backend-native path.
        backend_gather = getattr(self._backend, "gather", None)
        if callable(backend_gather):
            await self._claim_spawn_batch(resolved.name, len(tasks))
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.BATCH_STARTED,
                agent_name=resolved.name,
                trace_id=batch_trace_id,
                parent_trace_id=slot_context.parent_trace_id,
                payload={
                    "task_count": len(tasks),
                    "max_concurrency": max_concurrency,
                },
            )
        )

        # Per-batch timeout: ``run`` wraps the whole pipeline in
        # ``TimeoutMiddleware``; ``gather`` doesn't go through the pipeline,
        # so backend-native ``gather`` paths (``AsyncBackend.gather``,
        # ``JobBackend.gather``) drive their own concurrency primitives and
        # would otherwise ignore ``options.timeout_seconds`` entirely. Mirror
        # the middleware here so a long-tail batch can't hang past the
        # configured wall clock. One timeout covers the whole call (matching
        # ``run`` pipeline-level wrapping) — not per-slot.
        #
        # Slot-accounting note: when this fires after ``_claim_spawn_batch``,
        # the claimed slots stay claimed. Same semantics as ``run()`` — the
        # pipeline puts ``Timeout`` outside ``ClaimSlot``, so a timed-out
        # run also keeps its slot. The runtime stays sealed once the cap is
        # exhausted; rerun behaviour is unchanged.
        try:
            async with asyncio.timeout(self._options.timeout_seconds):
                if callable(backend_gather):
                    results = await backend_gather(
                        resolved,
                        tasks,
                        slot_context,
                        max_concurrency=max_concurrency,
                    )
                else:
                    results = await self._fallback_gather(
                        resolved, tasks, max_concurrency
                    )
        except TimeoutError as exc:
            raise SpawnError(
                f"gather timed out after {self._options.timeout_seconds}s "
                f"(agent={resolved.name}, task_count={len(tasks)})"
            ) from exc

        # Token budget post-charge: aggregate the per-slot ``tokens_used``
        # from the batch and decrement the runtime-wide budget. Mirrors
        # ``CostTrackingMiddleware`` semantics — pre-check is gated; this
        # post-charge is the bookkeeping that lets the *next* call see an
        # accurate remaining count. ``_fallback_gather`` already charges
        # per-call via ``self.run`` (CostTrackingMiddleware); only the
        # backend-native path needs this aggregate charge.
        if budget is not None and callable(backend_gather):
            batch_tokens = sum(
                int(getattr(r.metadata, "tokens_used", 0) or 0) for r in results
            )
            if batch_tokens > 0:
                await budget.consume(batch_tokens)

        success_count = sum(1 for r in results if r.is_ok())
        await self._emitter.emit(
            RuntimeEvent(
                event_type=EventType.BATCH_COMPLETED,
                agent_name=resolved.name,
                trace_id=batch_trace_id,
                parent_trace_id=slot_context.parent_trace_id,
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
        group: AgentGroup | AgentTeam,
        task: TaskSpec,
    ) -> AgentResult[BaseModel] | GroupResult:
        """Walk an ``AgentGroup`` topology against ``task``.

        Returns one of two shapes depending on how many terminal nodes
        actually fire:

        - Exactly one terminal — typical single-leaf or branch-routed
          topology — returns a plain :class:`AgentResult`. Identical
          to the pre-multi-terminal contract.
        - More than one terminal — moderator-and-specialists shape
          where each leaf is its own terminal — returns a
          :class:`GroupResult` keyed by ``Agent.name`` with
          aggregate metadata (summed tokens, max duration,
          ``backend="group"``).

        Failed slots in fan-out tiers are filtered before downstream
        mappers run; if every slot in a tier fails, raises
        :class:`murmur.core.errors.AllAgentsFailedError`.

        Emits :data:`EventType.GROUP_STARTED` before traversal and
        :data:`EventType.GROUP_COMPLETED` after the terminal result settles.
        Per-agent events (``AGENT_SPAWNED``, ``AGENT_COMPLETED`` etc.) come
        from each step's underlying :meth:`run` call.
        """
        # Imported lazily to keep ``murmur.groups`` optional-feeling and
        # avoid circular import at module load time.
        from murmur.events.types import EventType, RuntimeEvent
        from murmur.groups.runner import run_group as _run_group
        from murmur.groups.spec import AgentGroup as _AgentGroup
        from murmur.groups.team import AgentTeam as _AgentTeam
        from murmur.groups.team_runner import run_team as _run_team

        if not isinstance(group, _AgentGroup | _AgentTeam):
            raise TypeError(
                f"run_group expects AgentGroup or AgentTeam; got "
                f"{type(group).__name__!r}"
            )

        if isinstance(group, _AgentTeam):
            start = time.perf_counter()
            await self._emitter.emit(
                RuntimeEvent(
                    event_type=EventType.GROUP_STARTED,
                    agent_name=group.name,
                    task_id=task.id,
                    trace_id=task.request_id,
                    payload={
                        "shape": "team",
                        "delegate_count": len(group.delegates),
                    },
                )
            )
            try:
                return await _run_team(self, group, task)
            finally:
                duration_ms = int((time.perf_counter() - start) * 1000)
                await self._emitter.emit(
                    RuntimeEvent(
                        event_type=EventType.GROUP_COMPLETED,
                        agent_name=group.name,
                        task_id=task.id,
                        trace_id=task.request_id,
                        payload={"duration_ms": duration_ms, "shape": "team"},
                    )
                )

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
        signing_key = self._options.broker_signing_key
        if broker_instance is not None:
            return JobBackend(
                broker=broker_instance,
                runtime_id=self._runtime_id,
                publish_events=self._publish_events,
                event_emitter=self._emitter,
                signing_key=signing_key,
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
            signing_key=signing_key,
        )

    @staticmethod
    def _build_broker(*, scheme: str, url: str) -> Broker:
        if scheme == "memory":
            return InMemoryBroker()
        return make_broker(scheme=scheme, url=url)


__all__ = ["AgentRuntime"]
