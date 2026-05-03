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

from pydantic import BaseModel

from murmur.core.errors import SpecValidationError, TopologyError
from murmur.groups._introspection import get_fan_out_field
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
    """Stable identifier — used as the registry key, the broker topic suffix
    for group-level events, and the ``agent_name`` field on
    :data:`EventType.GROUP_*` events."""

    topology: Mapping[Agent, EdgeOrEdges] = field(default_factory=dict)
    """The DAG. Keys are :class:`Agent` instances; each value is one
    :class:`Edge` (most common) or a tuple of edges for branch routing
    with :attr:`Edge.condition` predicates. Validated at construction —
    cycles, dangling refs, and missing entry / terminal nodes raise
    :class:`TopologyError`."""

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
        # Heterogeneous fan-out: validate routing tables eagerly so
        # configuration errors (orphan agent, missing union member,
        # duplicate input_type, missing input_type) surface at
        # construction rather than at first dispatch.
        for src in self.topology:
            self._heterogeneous_dispatch_for(src)

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

    def topological_tiers(self) -> tuple[tuple[Agent, ...], ...]:
        """Topological order grouped into dependency tiers.

        Each tier contains nodes whose dependencies are all in earlier
        tiers. Within one tier the nodes are pairwise independent and
        therefore safe to dispatch in parallel. Tier order across the
        result preserves the DAG's dependency direction — tier ``i+1``
        is only reachable once every node in tier ``i`` has produced
        a result. Topology declaration order is preserved within each
        tier so the contract is "results stored, terminal returned"
        rather than "node X ran before node Y".
        """
        # Stable lookup for declaration order — used to sort each tier's
        # ready set independently of the order in which earlier-tier
        # parents happened to release their downstreams (which depends on
        # iteration order over outgoing edges, not topology declaration).
        order_index: dict[Agent, int] = {a: i for i, a in enumerate(self.topology)}
        indeg: dict[Agent, int] = dict.fromkeys(self.topology, 0)
        for src in self.topology:
            for edge in self.outgoing_edges(src):
                for tgt in edge.to:
                    indeg[tgt] = indeg.get(tgt, 0) + 1
        tiers: list[tuple[Agent, ...]] = []
        ready: list[Agent] = [a for a in self.topology if indeg[a] == 0]
        seen = 0
        while ready:
            tier = tuple(ready)
            tiers.append(tier)
            seen += len(tier)
            next_ready: list[Agent] = []
            for node in tier:
                for edge in self.outgoing_edges(node):
                    for tgt in edge.to:
                        indeg[tgt] -= 1
                        if indeg[tgt] == 0:
                            next_ready.append(tgt)
            next_ready.sort(key=order_index.__getitem__)
            ready = next_ready
        if seen != len(self.topology):  # pragma: no cover — caught by _has_cycle
            raise TopologyError(f"AgentGroup {self.name!r} topology has a cycle")
        return tuple(tiers)

    def _heterogeneous_dispatch_for(
        self, source: Agent
    ) -> dict[type[BaseModel], Agent] | None:
        """Build the type-routing dispatch table for a heterogeneous fan-out source.

        Returns ``None`` when the source's ``output_type`` has no
        :data:`FanOut` field, when the FanOut item type is a single type
        (single-type fan-out — the existing path handles it), or when
        the source has no outgoing targets at all (terminal). Otherwise
        returns a frozen ``{item_type: downstream_agent}`` mapping.

        Validators raise :class:`SpecValidationError` on:

        - A downstream agent with no ``Agent.input_type`` declared —
          required for typed routing under a heterogeneous source.
        - Two downstream agents claiming the same ``input_type`` —
          ambiguous routing under one source.
        - A union member with no matching downstream in the source's
          outgoing targets — items of that type would have nowhere to go.
        - A downstream agent whose ``input_type`` doesn't match any
          union member — the agent would never receive any items.

        Called eagerly at construction (``__post_init__``) so misconfigured
        topologies fail fast; called again at runtime by the runner so the
        table itself is never stored on the frozen ``AgentGroup``.
        """
        fan_out = get_fan_out_field(source.output_type)
        if fan_out is None:
            return None
        _field_name, item_types = fan_out
        if len(item_types) <= 1:
            return None  # Single-type fan-out — existing path handles it.

        targets: list[Agent] = []
        for edge in self.outgoing_edges(source):
            targets.extend(edge.to)
        if not targets:
            # Terminal source — no routing to validate.
            return None

        table: dict[type[BaseModel], Agent] = {}
        seen: dict[type[BaseModel], str] = {}
        for downstream in targets:
            if downstream.input_type is None:
                raise SpecValidationError(
                    f"agent {downstream.name!r} downstream of fan-out source "
                    f"{source.name!r} (heterogeneous union "
                    f"{[t.__name__ for t in item_types]}) must declare "
                    f"Agent.input_type for typed routing"
                )
            existing = seen.get(downstream.input_type)
            if existing is not None:
                raise SpecValidationError(
                    f"agents {existing!r} and {downstream.name!r} both claim "
                    f"input_type {downstream.input_type.__name__!r}; ambiguous "
                    f"routing under fan-out source {source.name!r}"
                )
            seen[downstream.input_type] = downstream.name
            table[downstream.input_type] = downstream

        missing = [t for t in item_types if t not in table]
        if missing:
            raise SpecValidationError(
                f"fan-out source {source.name!r} has union members "
                f"{[t.__name__ for t in missing]} with no matching downstream "
                f"agent (declared targets: "
                f"{[a.name for a in targets]})"
            )
        orphans = [a.name for a in targets if a.input_type not in item_types]
        if orphans:
            raise SpecValidationError(
                f"agents {orphans!r} downstream of fan-out source "
                f"{source.name!r} have input_types not in the union "
                f"{[t.__name__ for t in item_types]}"
            )
        return table

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
