"""Core package — Protocols, the pipeline composer, and domain errors.

All Protocols live in :mod:`murmur.core.protocols`. This package re-exports the
most commonly used ones at the top level for convenience.

Nothing in :mod:`murmur.core` imports from sibling concrete packages
(``backends``, ``context``, ``tools``, ``routing``, …). The arrow always points
inward to ``core/`` and ``types``.
"""

from murmur.core.errors import (
    BudgetExceededError,
    ContextError,
    DepthLimitError,
    MurmurError,
    RegistryError,
    SpawnCapError,
    SpawnCycleError,
    SpawnError,
    SpecValidationError,
    ToolExecutionError,
    TrustViolationError,
)
from murmur.core.pipeline import Pipeline, PipelineContext
from murmur.core.protocols import (
    Backend,
    BackendStatus,
    ContextPasser,
    EventEmitter,
    Middleware,
    NextStage,
    OnComplete,
    OnError,
    OnStart,
    Registry,
    RouteDecision,
    Router,
    Stage,
    ToolExecutor,
    ToolProvider,
    Worker,
)

__all__ = [
    "Backend",
    "BackendStatus",
    "BudgetExceededError",
    "ContextError",
    "ContextPasser",
    "DepthLimitError",
    "EventEmitter",
    "Middleware",
    "MurmurError",
    "NextStage",
    "OnComplete",
    "OnError",
    "OnStart",
    "Pipeline",
    "PipelineContext",
    "Registry",
    "RegistryError",
    "RouteDecision",
    "Router",
    "SpawnCapError",
    "SpawnCycleError",
    "SpawnError",
    "SpecValidationError",
    "Stage",
    "ToolExecutionError",
    "ToolExecutor",
    "ToolProvider",
    "TrustViolationError",
    "Worker",
]
