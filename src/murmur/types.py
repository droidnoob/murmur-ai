"""Murmur public value types.

These are part of the public API and are re-exported from :mod:`murmur`.

All types here are frozen Pydantic models or stdlib enums — they may cross
thread / process / broker boundaries safely. Mutate via ``model_copy(update=...)``.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Generic, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

T = TypeVar("T", bound=BaseModel)
F = TypeVar("F")


class _FanOutMarker:
    """Marker placed in :data:`FanOut`'s ``Annotated`` metadata.

    Discovered at runtime by :func:`murmur.groups.get_fan_out_field` to find
    the field of an ``output_type`` that the group runner should fan out
    over when no explicit ``mapper`` is supplied on the edge.
    """

    def __repr__(self) -> str:  # pragma: no cover — debug only
        return "FanOutMarker"


FanOut = Annotated[F, _FanOutMarker()]
"""Type annotation marking a Pydantic field as the fan-out target.

Use as ``FanOut[list[T]]`` on the field that holds the items the group
runner should split over. The runner will spawn one downstream agent per
item when no explicit ``mapper`` is set on the edge.

>>> class DecompositionResult(BaseModel):
...     sub_questions: FanOut[list[SubQuestion]]
...     reasoning: str = ""

Constraints (enforced by :func:`murmur.groups.get_fan_out_field`):

- The annotated type must be ``list[T]``. Not tuple, not set.
- Only one field per model may carry the marker.
"""


class TrustLevel(StrEnum):
    """Tool-access policy applied to an agent at runtime."""

    HIGH = "high"
    """Full tool access."""

    MEDIUM = "medium"
    """Curated tool set — the default for most agents."""

    LOW = "low"
    """Read-only tools only."""

    SANDBOX = "sandbox"
    """No tools — pure reasoning."""


class TaskSpec(BaseModel):
    """A single unit of work handed to ``runtime.run`` / ``runtime.gather``."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Per-task UUID. Auto-generated; collisions on the broker results topic
    are correlated by this value."""

    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Correlates one logical request across logs / broker messages / HTTP.

    Generated per task by default; supply explicitly to thread an upstream id
    (e.g. an ``X-Request-Id`` header) through every layer of the runtime.
    """

    input: str
    """The agent's input — a plain string, or a JSON-serialised structure when
    ``Agent.input_type`` is set (the runtime decodes against the agent's
    declared input type at dispatch)."""

    metadata: Mapping[str, str] = Field(default_factory=dict)
    """Free-form string-to-string metadata. Surfaces on every emitted
    ``RuntimeEvent`` and on the broker wire envelope; use for tenant /
    customer / trace tags. Frozen at construction."""


class ResultMetadata(BaseModel):
    """Per-result diagnostics produced by the runtime."""

    model_config = ConfigDict(frozen=True)

    duration_ms: int = 0
    """Wall-clock time from spawn to result, in milliseconds."""

    tokens_used: int = 0
    """Total tokens consumed (request + response + provider-side built-in
    tool tokens). Driver behind :class:`CostTrackingMiddleware`'s
    post-charge."""

    cost_usd: float = 0.0
    """Best-effort USD cost computed from ``tokens_used`` and the model's
    published rates. ``0.0`` when rates aren't known for the model in use."""

    backend: str = ""
    """Class name of the :class:`Backend` that ran the agent (e.g.
    ``"AsyncBackend"``, ``"JobBackend"``). Empty until populated by the
    backend's result path."""

    trace_id: str | None = None
    """Same value as ``TaskSpec.request_id`` for the run that produced this
    result — populated when available. ``None`` for synthetic results."""


class AgentResult(BaseModel, Generic[T]):
    """The typed envelope every ``runtime.run`` call returns.

    Either ``output`` is set (success) or ``error`` is set (failure) — never
    both. Use :meth:`is_ok` to discriminate.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    output: T | None = None
    """The agent's structured output, validated against
    ``Agent.output_type``. ``None`` when ``error`` is set."""

    error: BaseException | None = None
    """The exception that caused the run to fail. ``None`` on success.
    Always a :class:`MurmurError` subclass when raised by the runtime;
    user-tool exceptions wrap in :class:`ToolExecutionError`."""

    metadata: ResultMetadata = Field(default_factory=ResultMetadata)
    """Per-result diagnostics — duration, tokens, cost, backend, trace_id."""

    agent_name: str
    """Name of the :class:`Agent` that produced this result. Mirrors
    ``Agent.name``."""

    task_id: str
    """The originating ``TaskSpec.id`` — correlates a result back to its
    request."""

    def is_ok(self) -> bool:
        """``True`` if the agent succeeded and ``output`` is populated."""
        return self.error is None and self.output is not None


class GroupResult(BaseModel):
    """Multi-leaf result from :meth:`AgentRuntime.run_group`.

    Returned when an :class:`AgentGroup` topology has more than one
    terminal node fire — typically a moderator-and-specialists shape
    where each specialist is its own leaf rather than feeding a
    single synthesiser. Single-leaf topologies still return a plain
    :class:`AgentResult` for backward compatibility.

    Iteration: ``GroupResult.outputs`` is keyed by ``Agent.name`` so
    callers can pick out a specific terminal by name. Use
    :attr:`terminal` for the single-leaf convenience case.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    outputs: Mapping[str, AgentResult[BaseModel]]
    """Per-leaf results keyed by ``Agent.name``. A leaf that was
    skipped at runtime (branch routing condition, heterogeneous
    fan-out filter empty) is absent from this mapping — present
    keys correspond to terminals that actually fired.

    Insulated from caller mutation by an input-copy in the
    after-validator: the dict the model stores is independent of
    whatever the constructor was handed. Pydantic's
    ``model_config(frozen=True)`` blocks whole-attribute reassignment
    (``result.outputs = ...``); reaching through the stored dict
    reference (``result.outputs["new"] = ...``) is technically
    possible but undefined behaviour — treat ``GroupResult`` as
    read-only after construction."""

    metadata: ResultMetadata = Field(default_factory=ResultMetadata)
    """Aggregate diagnostics across every fired leaf. ``tokens_used``
    sums; ``duration_ms`` takes the max (parallel tiers don't add
    durations); ``cost_usd`` sums; ``backend`` is the literal
    string ``"group"``; ``trace_id`` mirrors the task's request_id."""

    @model_validator(mode="after")
    def _isolate_outputs(self) -> Self:
        """Copy the input mapping into a fresh dict so external
        mutation of the caller's dict can't bleed into this model.

        Earlier iterations wrapped ``outputs`` in
        :class:`types.MappingProxyType` for hard read-only
        enforcement; that broke ``model_copy(deep=True)`` and
        ``copy.deepcopy`` because ``mappingproxy`` isn't picklable
        (``TypeError: cannot pickle 'mappingproxy' object``). The
        plain-dict copy keeps standard Pydantic serialization /
        deep-copy semantics intact while still insulating the model
        from external aliasing.
        """
        if not isinstance(self.outputs, dict):
            object.__setattr__(self, "outputs", dict(self.outputs))
        return self

    @property
    def terminal(self) -> AgentResult[BaseModel]:
        """Convenience accessor for single-leaf cases.

        Raises :class:`ValueError` when the group fired more than one
        terminal — callers must use the keyed ``outputs`` mapping in
        that case.
        """
        if len(self.outputs) != 1:
            raise ValueError(
                f"GroupResult has {len(self.outputs)} terminals, not 1; "
                f"use .outputs[name] to pick a specific leaf"
            )
        return next(iter(self.outputs.values()))


class AgentHandle(BaseModel):
    """Opaque handle returned by a backend's ``spawn`` — used to kill / await."""

    model_config = ConfigDict(frozen=True)

    handle_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Backend-issued UUID. Treated as opaque by callers."""

    agent_name: str
    """Mirrors ``Agent.name`` for the spawned run."""

    task_id: str
    """Mirrors ``TaskSpec.id`` for the dispatched task."""

    backend: str
    """Class name of the :class:`Backend` that issued this handle."""


class AgentContext(BaseModel):
    """Context object passed between stages in the pipeline.

    Carries the conversation history, parent agent reference (for cascading
    spawns), and any user-attached metadata. Stages may produce a new
    ``AgentContext`` via ``model_copy(update=...)`` but never mutate the one
    they receive.
    """

    model_config = ConfigDict(frozen=True)

    messages: tuple[Mapping[str, str], ...] = Field(default_factory=tuple)
    """Conversation history forwarded into the spawn. Each entry is a
    ``{"role": "user"|"assistant"|"system", "content": str}`` mapping.
    Empty tuple = fresh context. The :class:`ContextPasser` chosen on the
    agent decides what fills this on each spawn."""

    parent_agent: str | None = None
    """Name of the immediate parent agent, when this is a sub-spawn.
    ``None`` for top-level runs."""

    parent_trace_id: str | None = None
    """``trace_id`` of the parent run that issued this sub-spawn. Threaded
    through onto every child :class:`RuntimeEvent` so observability backends
    can stitch a cascading run into a single tree. ``None`` for top-level
    runs."""

    ancestors: frozenset[str] = Field(default_factory=frozenset)
    """Set of agent names currently above this run in the spawn chain.
    Empty for top-level runs; for a sub-spawn it contains every ancestor up
    to the top-level agent. The runtime rejects a spawn whose target name
    already appears in ``ancestors`` with :class:`SpawnCycleError` —
    preventing A → B → A reentry without a separate graph store."""

    depth: int = 0
    """Cascading-spawn depth. ``0`` for top-level runs; incremented per
    sub-spawn. :class:`DepthLimitMiddleware` rejects when this reaches
    ``RuntimeOptions.max_spawn_depth``."""

    metadata: Mapping[str, str] = Field(default_factory=dict)
    """Free-form context metadata, threaded through to the spawned agent.
    Distinct from ``TaskSpec.metadata`` — that's per-task; this is
    per-context."""


__all__ = [
    "AgentContext",
    "AgentHandle",
    "AgentResult",
    "FanOut",
    "ResultMetadata",
    "TaskSpec",
    "TrustLevel",
]
