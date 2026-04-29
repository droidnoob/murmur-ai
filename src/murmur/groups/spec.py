"""``AgentGroup`` — a frozen DAG of agents connected by :class:`Edge` arrows.

Constructed once, then run via :meth:`murmur.AgentRuntime.run_group` against
a :class:`TaskSpec`. Validation runs at construction time:

- Every :class:`Edge` ``to`` target must be a key in the topology.
- The graph must have at least one entry node (no incoming edges).
- The graph must have at least one terminal node (``Edge.to=()``).
- No cycles.

A topology value may be a single :class:`Edge` (the common case) **or**
a tuple of edges from the same source. Multiple outgoing edges enable
branch routing when each carries a :attr:`Edge.condition` predicate.

Type compatibility (output_type → input_type / FanOut item type) is *not*
enforced at construction — those checks happen in the runner when it
resolves a mapper or attempts auto fan-out. (Static checking lands later.)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from murmur.core.errors import TopologyError
from murmur.groups.edge import Edge

if TYPE_CHECKING:
    from murmur.agent import Agent


EdgeOrEdges = Edge | tuple[Edge, ...]
"""A topology value: one outgoing edge, or several when branch routing."""


def _normalize(edges: EdgeOrEdges) -> tuple[Edge, ...]:
    """Coerce a topology value into a tuple of :class:`Edge` instances."""
    if isinstance(edges, Edge):
        return (edges,)
    return tuple(edges)


@dataclass(frozen=True)
class AgentGroup:
    """A named DAG of agents.

    >>> crew = AgentGroup(
    ...     name="research",
    ...     topology={
    ...         head: Edge(to=(minion,), mapper=head_to_minions),
    ...         minion: Edge(to=(synthesizer,), mapper=minions_to_synth),
    ...         synthesizer: Edge.terminal(),
    ...     },
    ... )

    Multiple outgoing edges with mutually-exclusive conditions enable
    branch routing:

    >>> crew = AgentGroup(
    ...     name="ticket_router",
    ...     topology={
    ...         triage: (
    ...             Edge(to=(quick_replier,), condition=lambda o: o.severity == "low"),
    ...             Edge(to=(escalator,),     condition=lambda o: o.severity == "high"),
    ...         ),
    ...         quick_replier: Edge.terminal(),
    ...         escalator:     Edge.terminal(),
    ...     },
    ... )
    """

    name: str
    topology: Mapping[Agent, EdgeOrEdges] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.topology:
            raise TopologyError(f"AgentGroup {self.name!r} has empty topology")
        # Every Edge.to target must be a node in the topology.
        nodes = set(self.topology.keys())
        for src in self.topology:
            for edge in self.outgoing_edges(src):
                for tgt in edge.to:
                    if tgt not in nodes:
                        raise TopologyError(
                            f"agent {tgt.name!r} (edge from {src.name!r}) is "
                            f"not a node in the topology"
                        )
        if self._has_cycle():
            raise TopologyError(f"AgentGroup {self.name!r} topology has a cycle")
        if not self.entry_nodes():
            raise TopologyError(
                f"AgentGroup {self.name!r} has no entry node (every node "
                f"has an incoming edge)"
            )
        if not self.terminal_nodes():
            raise TopologyError(
                f"AgentGroup {self.name!r} has no terminal node (every node "
                f"has at least one outgoing edge)"
            )

    @property
    def agents(self) -> tuple[Agent, ...]:
        """Tuple of agents in topology declaration order."""
        return tuple(self.topology.keys())

    def outgoing_edges(self, src: Agent) -> tuple[Edge, ...]:
        """Outgoing edges from ``src`` — always a tuple, even for single edges."""
        return _normalize(self.topology[src])

    def entry_nodes(self) -> tuple[Agent, ...]:
        """Nodes with no incoming edges."""
        targets: set[Agent] = set()
        for src in self.topology:
            for edge in self.outgoing_edges(src):
                targets.update(edge.to)
        return tuple(a for a in self.topology if a not in targets)

    def terminal_nodes(self) -> tuple[Agent, ...]:
        """Nodes with no outgoing edges (every outgoing edge has empty ``to``)."""
        result: list[Agent] = []
        for src in self.topology:
            edges = self.outgoing_edges(src)
            if all(not edge.to for edge in edges):
                result.append(src)
        return tuple(result)

    def topological_order(self) -> tuple[Agent, ...]:
        """Kahn's algorithm — order in which to walk the DAG."""
        indeg: dict[Agent, int] = dict.fromkeys(self.topology, 0)
        for src in self.topology:
            for edge in self.outgoing_edges(src):
                for tgt in edge.to:
                    indeg[tgt] = indeg.get(tgt, 0) + 1
        queue: list[Agent] = [a for a, d in indeg.items() if d == 0]
        order: list[Agent] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for edge in self.outgoing_edges(node):
                for tgt in edge.to:
                    indeg[tgt] -= 1
                    if indeg[tgt] == 0:
                        queue.append(tgt)
        if len(order) != len(self.topology):  # pragma: no cover — caught by _has_cycle
            raise TopologyError(f"AgentGroup {self.name!r} topology has a cycle")
        return tuple(order)

    def _has_cycle(self) -> bool:
        white: set[Agent] = set(self.topology.keys())
        gray: set[Agent] = set()

        def visit(node: Agent) -> bool:
            if node in gray:
                return True
            if node not in white:
                return False
            white.discard(node)
            gray.add(node)
            for edge in self.outgoing_edges(node):
                for tgt in edge.to:
                    if visit(tgt):
                        return True
            gray.discard(node)
            return False

        return any(visit(a) for a in list(white))


__all__ = ["AgentGroup", "EdgeOrEdges"]
