"""Murmur public value types.

These are part of the public API and are re-exported from :mod:`murmur`.

All types here are frozen Pydantic models or stdlib enums — they may cross
thread / process / broker boundaries safely. Mutate via ``model_copy(update=...)``.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

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
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Correlates one logical request across logs / broker messages / HTTP.

    Generated per task by default; supply explicitly to thread an upstream id
    (e.g. an ``X-Request-Id`` header) through every layer of the runtime.
    """

    input: str
    metadata: Mapping[str, str] = Field(default_factory=dict)


class ResultMetadata(BaseModel):
    """Per-result diagnostics produced by the runtime."""

    model_config = ConfigDict(frozen=True)

    duration_ms: int = 0
    tokens_used: int = 0
    cost_usd: float = 0.0
    backend: str = ""
    trace_id: str | None = None


class AgentResult(BaseModel, Generic[T]):
    """The typed envelope every ``runtime.run`` call returns.

    Either ``output`` is set (success) or ``error`` is set (failure) — never
    both. Use :meth:`is_ok` to discriminate.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    output: T | None = None
    error: BaseException | None = None
    metadata: ResultMetadata = Field(default_factory=ResultMetadata)
    agent_name: str
    task_id: str

    def is_ok(self) -> bool:
        """``True`` if the agent succeeded and ``output`` is populated."""
        return self.error is None and self.output is not None


class AgentHandle(BaseModel):
    """Opaque handle returned by a backend's ``spawn`` — used to kill / await."""

    model_config = ConfigDict(frozen=True)

    handle_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_name: str
    task_id: str
    backend: str


class AgentContext(BaseModel):
    """Context object passed between stages in the pipeline.

    Carries the conversation history, parent agent reference (for cascading
    spawns), and any user-attached metadata. Stages may produce a new
    ``AgentContext`` via ``model_copy(update=...)`` but never mutate the one
    they receive.
    """

    model_config = ConfigDict(frozen=True)

    messages: tuple[Mapping[str, str], ...] = Field(default_factory=tuple)
    parent_agent: str | None = None
    depth: int = 0
    metadata: Mapping[str, str] = Field(default_factory=dict)


__all__ = [
    "AgentContext",
    "AgentHandle",
    "AgentResult",
    "FanOut",
    "ResultMetadata",
    "TaskSpec",
    "TrustLevel",
]
