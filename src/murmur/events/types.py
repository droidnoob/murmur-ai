"""Runtime instrumentation events.

:class:`RuntimeEvent` is the typed envelope every observability emission
flows through — agent spawns, tool calls, batch lifecycle, budget /
depth-limit hits. Distinct from :class:`murmur.runs.RunEvent`, which
is a run-level SSE replay record for ``GET /runs/{run_id}/stream``;
this is finer-grained and fires on every action a runtime takes.

Trace identity follows the Phase 2 design: ``trace_id`` is exactly the
``request_id`` from :class:`murmur.types.TaskSpec` — we don't introduce
a new ID concept. ``parent_trace_id`` is reserved for cascading-spawn
support (Phase 4) and stays ``None`` until then.

Payload is a free-form ``Mapping[str, object]`` keyed by event type.
The shape of the payload per event is documented inline below.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    """Discriminator for :class:`RuntimeEvent.event_type`."""

    AGENT_DISPATCHED = "agent_dispatched"
    """Emitted publisher-side by :class:`JobBackend` immediately after a
    task is published to the broker — *before* the worker picks it up and
    fires :data:`AGENT_SPAWNED`. Gives the publisher local visibility into
    "task accepted by broker" even when the distributed event bridge is
    off. AsyncBackend never emits this; AGENT_SPAWNED is the equivalent
    head-of-run signal.

    Payload: ``{"backend": str, "broker": str | None, "trust_level": str}``."""

    AGENT_SPAWNED = "agent_spawned"
    """Emitted by the runtime when an agent run starts.

    Payload: ``{"backend": str, "trust_level": str}``."""

    AGENT_COMPLETED = "agent_completed"
    """Emitted when an agent run produces an output.

    Payload: ``{"duration_ms": int, "tokens_used": int, "backend": str}``."""

    AGENT_FAILED = "agent_failed"
    """Emitted when an agent run errors.

    Payload: ``{"duration_ms": int, "error": str, "backend": str}``."""

    TOOL_CALL_STARTED = "tool_call_started"
    """Emitted by :class:`ToolExecutor` before each tool invocation.

    Payload: ``{"tool_name": str, "trust_level": str}``."""

    TOOL_CALL_COMPLETED = "tool_call_completed"
    """Emitted on a successful tool return.

    Payload: ``{"tool_name": str}``."""

    TOOL_CALL_FAILED = "tool_call_failed"
    """Emitted on a tool exception.

    Payload: ``{"tool_name": str, "error": str}``."""

    BATCH_STARTED = "batch_started"
    """Emitted at the head of :meth:`AgentRuntime.gather`.

    Payload: ``{"task_count": int, "max_concurrency": int}``."""

    BATCH_COMPLETED = "batch_completed"
    """Emitted after every slot of :meth:`AgentRuntime.gather` settles.

    Payload: ``{"task_count": int, "success_count": int, "failure_count": int}``."""

    GROUP_STARTED = "group_started"
    """Emitted at the head of :meth:`AgentRuntime.run_group`.

    Payload: ``{"group_name": str, "node_count": int}``."""

    GROUP_COMPLETED = "group_completed"
    """Emitted on terminal-result of :meth:`AgentRuntime.run_group`.

    Payload: ``{"group_name": str, "duration_ms": int}``."""

    BUDGET_EXCEEDED = "budget_exceeded"
    """Emitted by cost-tracking middleware just before raising ``BudgetExceededError``.

    Payload: ``{"limit": int, "used": int, "scope": "task" | "runtime"}``."""

    DEPTH_LIMIT_EXCEEDED = "depth_limit_exceeded"
    """Emitted by depth-limit middleware just before raising ``DepthLimitError``.

    Payload: ``{"limit": int, "depth": int}``."""


class RuntimeEvent(BaseModel):
    """Frozen envelope for one runtime instrumentation point.

    Crosses thread / process / broker boundaries safely (Pydantic
    serialisation) so emitters can be wired in any backend without
    leaking unserialisable state.
    """

    model_config = ConfigDict(frozen=True)

    event_type: EventType
    """Discriminator. Determines what payload shape callers should expect —
    see :class:`EventType` value docstrings for the per-type payload contract."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    """UTC timestamp of the event. Set at the emission point inside the
    runtime; not the timestamp the emitter delivered to its sink."""

    agent_name: str
    """Name of the :class:`Agent` the event is about — or the dispatching
    agent's name for batch / group / tool-call events."""

    task_id: str | None = None
    """``None`` for non-task events (BATCH_STARTED, GROUP_STARTED, etc.)."""

    trace_id: str
    """Same value as :attr:`murmur.types.TaskSpec.request_id`. Phase 2 does
    not introduce a new ID — every log line, every event, every broker
    message carries this one id."""

    parent_trace_id: str | None = None
    """Reserved for cascading-spawn support (Phase 4). Stays ``None`` for
    top-level runs; child spawns will carry the parent's trace_id here."""

    payload: Mapping[str, object] = Field(default_factory=dict)
    """Per-event-type details — see :class:`EventType` docstrings."""


__all__ = ["EventType", "RuntimeEvent"]
