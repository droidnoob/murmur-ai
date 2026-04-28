"""Default Phase 1 router — every task takes the single-agent path.

Satisfies :class:`murmur.core.protocols.router.Router` structurally. The
LLM-backed classifier in later phases drops in via the same Protocol.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from murmur.core.protocols.router import RouteDecision

if TYPE_CHECKING:
    from murmur.types import TaskSpec


class AlwaysSingleRouter:
    """Trivial router — returns ``RouteDecision.SINGLE`` for every task."""

    async def classify(self, task: TaskSpec) -> RouteDecision:  # noqa: ARG002 — protocol arg
        return RouteDecision.SINGLE


__all__ = ["AlwaysSingleRouter"]
