"""``AgentGroup`` — a frozen DAG of agents connected by :class:`Edge` arrows.

Constructed once, then run via :meth:`murmur.AgentRuntime.run_group` against
a :class:`TaskSpec`. Validation runs at construction time:

- Every :class:`Edge` ``to`` target must be a key in the topology.
- The graph must have at least one entry node (no incoming edges).
- The graph must have at least one terminal node (``Edge.to=()``).
- No cycles.

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
    """

    name: str
    topology: Mapping[Agent, Edge] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.topology:
            raise TopologyError(f"AgentGroup {self.name!r} has empty topology")
        # Every Edge.to target must be a node in the topology.
        nodes = set(self.topology.keys())
        for src, edge in self.topology.items():
            for tgt in edge.to:
                if tgt not in nodes:
                    raise TopologyError(
                        f"agent {tgt.name!r} (edge from {src.name!r}) is not "
                        f"a node in the topology"
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

    def entry_nodes(self) -> tuple[Agent, ...]:
        """Nodes with no incoming edges."""
        targets: set[Agent] = set()
        for edge in self.topology.values():
            targets.update(edge.to)
        return tuple(a for a in self.topology if a not in targets)

    def terminal_nodes(self) -> tuple[Agent, ...]:
        """Nodes with no outgoing edges."""
        return tuple(a for a, edge in self.topology.items() if not edge.to)

    def topological_order(self) -> tuple[Agent, ...]:
        """Kahn's algorithm — order in which to walk the DAG."""
        indeg: dict[Agent, int] = dict.fromkeys(self.topology, 0)
        for edge in self.topology.values():
            for tgt in edge.to:
                indeg[tgt] = indeg.get(tgt, 0) + 1
        queue: list[Agent] = [a for a, d in indeg.items() if d == 0]
        order: list[Agent] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for tgt in self.topology[node].to:
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
            for tgt in self.topology[node].to:
                if visit(tgt):
                    return True
            gray.discard(node)
            return False

        return any(visit(a) for a in list(white))


__all__ = ["AgentGroup"]
