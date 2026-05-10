"""Wire-format types and helpers used by the lean HTTP client.

Mirrors the Pydantic models that :class:`murmur.AgentServer` serialises
on the wire. The client deliberately does not depend on ``murmur-ai`` —
each side defines its own copies so the heavy server-side install
(PydanticAI, FastStream, structlog …) doesn't get pulled into
client-only deployments. Round-trip identity comes from matching the
JSON shape, not from sharing the Python class.

If a field is added on the server side and the client wants to surface
it, the change has to land here too. The shape is small enough that
that's a feature: it forces a deliberate sync rather than an invisible
import-graph dependency.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T", bound=BaseModel)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class MurmurError(Exception):
    """Base class for every error the client raises."""


class RegistryError(MurmurError):
    """Server returned 404 / unknown agent / unknown group."""


class BudgetExceededError(MurmurError):
    """Server returned 429 / token-budget exhausted."""


class DepthLimitError(MurmurError):
    """Server returned 429 / spawn-depth limit reached."""


class TrustViolationError(MurmurError):
    """Server returned 403 / trust-level guard rejected the call."""


class SpecValidationError(MurmurError):
    """Server returned 400 / bad spec."""


class TopologyError(SpecValidationError):
    """Server returned 400 / bad group topology."""


class SpawnError(MurmurError):
    """Server returned 500 / spawn failure."""


class ToolExecutionError(MurmurError):
    """Server returned 500 / tool raised."""


class ContextError(MurmurError):
    """Server returned 500 / context preparation failed."""


class AllAgentsFailedError(MurmurError):
    """Server returned 500 / every fan-out slot failed."""


# Map server-side class names → client-side class. Names match the
# server's :class:`ErrorResponse.error` field.
_NAME_TO_CLASS: dict[str, type[MurmurError]] = {
    "MurmurError": MurmurError,
    "RegistryError": RegistryError,
    "BudgetExceededError": BudgetExceededError,
    "DepthLimitError": DepthLimitError,
    "TrustViolationError": TrustViolationError,
    "SpecValidationError": SpecValidationError,
    "TopologyError": TopologyError,
    "SpawnError": SpawnError,
    "ToolExecutionError": ToolExecutionError,
    "ContextError": ContextError,
    "AllAgentsFailedError": AllAgentsFailedError,
}


class ErrorResponse(BaseModel):
    """Wire shape the server returns on any non-2xx response."""

    model_config = ConfigDict(frozen=True)

    error: str
    """Class name of the raised error — e.g. ``"BudgetExceededError"``."""

    message: str
    """Human-readable detail."""

    agent: str | None = None
    task_id: str | None = None
    request_id: str


def response_to_error(response: ErrorResponse) -> MurmurError:
    """Recreate the typed exception client-side from a wire payload.

    Unknown error names fall back to :class:`MurmurError` so a server
    that adds a new error class still surfaces *something* sensible to
    the client without an immediate version bump.
    """
    cls = _NAME_TO_CLASS.get(response.error, MurmurError)
    return cls(response.message)


# ---------------------------------------------------------------------------
# Task / result envelopes
# ---------------------------------------------------------------------------


class TaskSpec(BaseModel):
    """A single unit of work handed to the runtime."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
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
    """The typed envelope every ``runtime.run`` call returns."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    output: T | None = None
    error: BaseException | None = None
    metadata: ResultMetadata = Field(default_factory=ResultMetadata)
    agent_name: str
    task_id: str

    def is_ok(self) -> bool:
        """``True`` when the run succeeded — i.e. ``output`` is set."""
        return self.error is None and self.output is not None


# ---------------------------------------------------------------------------
# Runs (status + events)
# ---------------------------------------------------------------------------


class RunState(StrEnum):
    """Coarse-grained lifecycle state for a submitted run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunProgress(BaseModel):
    """Per-step counters reported during a run."""

    model_config = ConfigDict(frozen=True)

    total: int = 0
    completed: int = 0
    failed: int = 0
    running: int = 0


class RunStatus(BaseModel):
    """A run's current state + progress snapshot."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    state: RunState
    progress: RunProgress | None = None


class RunEventType(StrEnum):
    """Discriminator for :class:`RunEvent` instances on the SSE stream."""

    AGENT_STARTED = "agent_started"
    AGENT_COMPLETED = "agent_completed"
    AGENT_FAILED = "agent_failed"
    GROUP_COMPLETED = "group_completed"
    RUN_CANCELLED = "run_cancelled"


class RunEvent(BaseModel):
    """Stream event published over SSE for ``GET /runs/{run_id}/stream``."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: RunEventType
    run_id: str
    agent: str | None = None
    task_id: str | None = None
    error: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


# ---------------------------------------------------------------------------
# sync-call guard
# ---------------------------------------------------------------------------


def reject_if_in_event_loop(method_name: str) -> None:
    """Raise if the caller is inside a running asyncio loop.

    Used by ``MurmurClient.run_sync`` to give a clearer error than the
    stdlib's ``RuntimeError: asyncio.run() cannot be called from a
    running event loop``.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        f"{method_name} called from a running event loop; use the async variant instead"
    )


__all__ = [
    "AgentResult",
    "AllAgentsFailedError",
    "BudgetExceededError",
    "ContextError",
    "DepthLimitError",
    "ErrorResponse",
    "MurmurError",
    "RegistryError",
    "ResultMetadata",
    "RunEvent",
    "RunEventType",
    "RunProgress",
    "RunState",
    "RunStatus",
    "SpawnError",
    "SpecValidationError",
    "TaskSpec",
    "ToolExecutionError",
    "TopologyError",
    "TrustViolationError",
    "reject_if_in_event_loop",
    "response_to_error",
]


# ---------------------------------------------------------------------------
# Re-exports kept off the wildcard list intentionally — these are the
# auxiliary handles used internally by client.py.
# ---------------------------------------------------------------------------


class _RemoteResult(BaseModel):  # pragma: no cover — wire-only helper
    """Compat shim for callers that imported the legacy server-side wire
    type. Kept here so any out-of-tree code that referenced it doesn't
    break; new code should reach for :class:`AgentResult` directly."""

    model_config = ConfigDict(frozen=True)

    agent_name: str
    task_id: str
    success: bool
    output: dict[str, Any] | None = None
    error: str | None = None
    metadata: ResultMetadata = Field(default_factory=ResultMetadata)
