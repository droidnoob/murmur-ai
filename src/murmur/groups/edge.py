"""``Edge`` — a single arrow in an :class:`AgentGroup` topology.

An edge declares: "the output of *upstream* flows to these *downstream*
agents, optionally transformed by a *mapper*". The runner inspects the
mapper's return type to decide between a single dispatch
(``runtime.run``) and a fan-out (``runtime.gather``):

- Mapper returns ``TaskSpec`` → single dispatch.
- Mapper returns ``list[TaskSpec]`` → fan-out.
- No mapper, upstream ``output_type`` has a :data:`FanOut`-annotated field
  → auto fan-out over that field, one downstream per item.
- No mapper, no ``FanOut`` → JSON-serialise the upstream output as the
  downstream input.

``max_concurrency`` caps the fan-out width when ``runtime.gather`` is
called. Defaults to 100.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from murmur.agent import Agent


EdgeMapper = Callable[..., Any]
"""Return ``TaskSpec`` for single dispatch, ``list[TaskSpec]`` for fan-out."""


@dataclass(frozen=True)
class Edge:
    """A frozen value object describing one outgoing connection."""

    to: tuple[Agent, ...] = ()
    """Downstream agents. Empty tuple == terminal node."""

    mapper: EdgeMapper | None = None
    """Optional ``(upstream_output) -> TaskSpec | list[TaskSpec]`` transform."""

    max_concurrency: int = 100
    """Width cap when this edge fans out."""

    @staticmethod
    def terminal() -> Edge:
        """Convenience for terminal nodes — ``Edge(to=())``."""
        return Edge(to=())


__all__ = ["Edge", "EdgeMapper"]
