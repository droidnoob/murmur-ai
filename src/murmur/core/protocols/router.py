"""Router Protocol — single-agent vs orchestrator decision.

The default is rule-based; an LLM-backed classifier slots in by
satisfying this same Protocol.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from murmur.types import TaskSpec


class RouteDecision(StrEnum):
    """Outcome of router classification."""

    SINGLE = "single"
    """Run a single agent against the task."""

    MULTI = "multi"
    """Hand off to the orchestrator for fan-out + aggregation."""


class Router(Protocol):
    """Pluggable routing strategy."""

    async def classify(self, task: TaskSpec) -> RouteDecision:
        """Decide whether ``task`` runs as single-agent or multi-agent."""
        ...


__all__ = ["RouteDecision", "Router"]
