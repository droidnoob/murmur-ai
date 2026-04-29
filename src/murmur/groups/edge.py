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

Conditions gate whether an edge fires at all. The predicate receives
the upstream's typed output (same shape as ``mapper``) and returns
``True`` to fire / ``False`` to skip. Predicates may be sync **or**
async — the runner awaits the result if it's a coroutine.

Recommended pattern for AI-driven routing: model the classifier as a
real :class:`~murmur.Agent` node with a typed ``output_type`` and use
plain sync conditions on its output. Hidden LLM calls inside a
condition lambda are invisible to the runtime's accounting / tracing.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from murmur.agent import Agent


EdgeMapper = Callable[..., Any]
"""Return ``TaskSpec`` for single dispatch, ``list[TaskSpec]`` for fan-out."""

EdgeCondition = Callable[[BaseModel], bool | Awaitable[bool]]
"""``(upstream_output) -> bool``. Async returns are awaited at the call site."""


@dataclass(frozen=True)
class Edge:
    """A frozen value object describing one outgoing connection."""

    to: tuple[Agent, ...] = ()
    """Downstream agents. Empty tuple == terminal node."""

    mapper: EdgeMapper | None = None
    """Optional ``(upstream_output) -> TaskSpec | list[TaskSpec]`` transform."""

    max_concurrency: int = 100
    """Width cap when this edge fans out."""

    condition: EdgeCondition | None = None
    """Optional predicate over the upstream's typed output.

    ``None`` means "always fire". When set, the runner evaluates the
    callable with the upstream output (same value the ``mapper`` would
    receive) and only traverses to ``to`` agents when it returns truthy.
    Async callables are awaited.
    """

    @staticmethod
    def terminal() -> Edge:
        """Convenience for terminal nodes — ``Edge(to=())``."""
        return Edge(to=())


__all__ = ["Edge", "EdgeCondition", "EdgeMapper"]
