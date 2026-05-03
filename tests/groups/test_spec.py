"""Unit tests for ``murmur.groups.spec.AgentGroup`` validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur.agent import Agent
from murmur.context.null import NullContextPasser
from murmur.core.errors import SpecValidationError, TopologyError
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.types import FanOut, TrustLevel


class _Out(BaseModel):
    text: str


def _agent(name: str) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


def test_simple_three_node_chain() -> None:
    a, b, c = _agent("a"), _agent("b"), _agent("c")
    group = AgentGroup(
        name="chain",
        topology={
            a: Edge(to=(b,)),
            b: Edge(to=(c,)),
            c: Edge.terminal(),
        },
    )
    assert group.entry_nodes() == (a,)
    assert group.terminal_nodes() == (c,)
    assert group.topological_order() == (a, b, c)


def test_dangling_target_raises() -> None:
    a, b, ghost = _agent("a"), _agent("b"), _agent("ghost")
    with pytest.raises(TopologyError, match="not a node"):
        AgentGroup(
            name="dangling",
            topology={
                a: Edge(to=(ghost,)),
                b: Edge.terminal(),
            },
        )


def test_cycle_raises() -> None:
    a, b = _agent("a"), _agent("b")
    with pytest.raises(TopologyError, match="cycle"):
        AgentGroup(
            name="cycle",
            topology={
                a: Edge(to=(b,)),
                b: Edge(to=(a,)),
            },
        )


def test_self_loop_raises_cycle() -> None:
    a = _agent("a")
    with pytest.raises(TopologyError, match="cycle"):
        AgentGroup(name="loop1", topology={a: Edge(to=(a,))})


def test_empty_topology_raises() -> None:
    with pytest.raises(TopologyError, match="empty topology"):
        AgentGroup(name="empty", topology={})


def test_agents_property_preserves_declaration_order() -> None:
    a, b, c = _agent("a"), _agent("b"), _agent("c")
    group = AgentGroup(
        name="ordered",
        topology={a: Edge(to=(b,)), b: Edge(to=(c,)), c: Edge.terminal()},
    )
    assert group.agents == (a, b, c)


# ---------------------------------------------------------------------------
# topological_tiers — dependency-grouped traversal for parallel sibling dispatch
# ---------------------------------------------------------------------------


def test_topological_tiers_linear_chain_one_node_per_tier() -> None:
    a, b, c = _agent("a"), _agent("b"), _agent("c")
    group = AgentGroup(
        name="chain",
        topology={a: Edge(to=(b,)), b: Edge(to=(c,)), c: Edge.terminal()},
    )
    assert group.topological_tiers() == ((a,), (b,), (c,))


def test_topological_tiers_diamond_groups_siblings() -> None:
    """a -> {b, c} -> d. Tiers: ([a], [b, c], [d])."""
    a, b, c, d = _agent("a"), _agent("b"), _agent("c"), _agent("d")
    group = AgentGroup(
        name="diamond",
        topology={
            a: Edge(to=(b, c)),
            b: Edge(to=(d,)),
            c: Edge(to=(d,)),
            d: Edge.terminal(),
        },
    )
    tiers = group.topological_tiers()
    assert tiers[0] == (a,)
    assert set(tiers[1]) == {b, c}
    assert tiers[2] == (d,)


def test_topological_tiers_preserves_declaration_order_within_tier() -> None:
    """Tier members keep topology declaration order — gives a stable contract
    even though concurrent dispatch makes execution order non-deterministic.
    """
    a, b, c, d, e = (_agent(n) for n in "abcde")
    # b, c, d are all siblings under a; declaration order: b, c, d.
    group = AgentGroup(
        name="fan",
        topology={
            a: Edge(to=(b, c, d)),
            b: Edge(to=(e,)),
            c: Edge(to=(e,)),
            d: Edge(to=(e,)),
            e: Edge.terminal(),
        },
    )
    tiers = group.topological_tiers()
    assert tiers == ((a,), (b, c, d), (e,))


def test_topological_tiers_node_only_advances_once_all_deps_seen() -> None:
    """A node with one shallow + one deep dependency lands in the deeper tier."""
    a, b, c, d = _agent("a"), _agent("b"), _agent("c"), _agent("d")
    # a -> b, a -> c, b -> d, c -> d. d depends on b AND c. Both b/c in tier 1.
    # d only fires in tier 2 — never in tier 1 just because c is ready.
    group = AgentGroup(
        name="join",
        topology={
            a: Edge(to=(b, c)),
            b: Edge(to=(d,)),
            c: Edge(to=(d,)),
            d: Edge.terminal(),
        },
    )
    tiers = group.topological_tiers()
    assert tiers[0] == (a,)
    assert set(tiers[1]) == {b, c}
    assert tiers[2] == (d,)


def test_topological_tiers_disconnected_entries_share_tier_zero() -> None:
    """Two independent entry nodes both end up in tier 0."""
    a, b, c, d = _agent("a"), _agent("b"), _agent("c"), _agent("d")
    group = AgentGroup(
        name="forest",
        topology={
            a: Edge(to=(c,)),
            b: Edge(to=(d,)),
            c: Edge.terminal(),
            d: Edge.terminal(),
        },
    )
    tiers = group.topological_tiers()
    assert tiers[0] == (a, b)
    assert tiers[1] == (c, d)


def test_topological_tiers_within_tier_order_independent_of_unlock_order() -> None:
    """Tier members follow topology declaration order even when earlier
    parents unlock them in a different sequence.

    Topology declared as ``{a, b, c, d}``. Edges: ``a -> d`` and
    ``b -> c``. Walking tier 0 ``(a, b)`` decrements ``d`` first
    (unlocked via ``a``), then ``c`` (unlocked via ``b``) — the
    natural unlock order is ``[d, c]``. Tier 1 must still be
    ``(c, d)`` because that's the topology declaration order;
    callers depend on this for stable execution-order contracts
    even when sibling dispatch is parallel.
    """
    a, b, c, d = _agent("a"), _agent("b"), _agent("c"), _agent("d")
    group = AgentGroup(
        name="unlock-order",
        topology={
            a: Edge(to=(d,)),
            b: Edge(to=(c,)),
            c: Edge.terminal(),
            d: Edge.terminal(),
        },
    )
    tiers = group.topological_tiers()
    assert tiers[0] == (a, b)
    assert tiers[1] == (c, d)


# ---------------------------------------------------------------------------
# Heterogeneous fan-out validators
# ---------------------------------------------------------------------------


class _ItemA(BaseModel):
    a: int


class _ItemB(BaseModel):
    b: str


class _ItemC(BaseModel):
    c: float


class _DecompUnion(BaseModel):
    """Output type for a heterogeneous fan-out source."""

    items: FanOut[list[_ItemA | _ItemB | _ItemC]]


class _DecompSingle(BaseModel):
    """Output type for the single-type fan-out path (control)."""

    items: FanOut[list[_ItemA]]


class _Animal(BaseModel):
    species: str


class _Dog(_Animal):
    breed: str = ""


class _SubclassUnion(BaseModel):
    """Union with a subclass relationship — should be rejected."""

    items: FanOut[list[_Animal | _Dog]]


class _Final(BaseModel):
    out: str


def _typed_agent(
    name: str,
    *,
    output_type: type[BaseModel],
    input_type: type[BaseModel] | None = None,
) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        input_type=input_type,
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


def test_heterogeneous_fanout_well_formed_topology_constructs() -> None:
    """A correctly-wired heterogeneous source builds without error: every
    union member has a matching downstream, every downstream's
    ``input_type`` is in the union, and no two downstreams overlap.
    """
    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    c = _typed_agent("c", output_type=_Final, input_type=_ItemC)
    AgentGroup(
        name="hetero-ok",
        topology={
            src: Edge(to=(a, b, c)),
            a: Edge.terminal(),
            b: Edge.terminal(),
            c: Edge.terminal(),
        },
    )


def test_heterogeneous_fanout_missing_input_type_raises() -> None:
    """A downstream of a heterogeneous source without ``input_type`` is
    unrootable for typed dispatch — reject at construction.
    """
    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    untyped = _typed_agent("untyped", output_type=_Final, input_type=None)
    with pytest.raises(SpecValidationError, match="must declare Agent.input_type"):
        AgentGroup(
            name="hetero-untyped",
            topology={
                src: Edge(to=(a, b, untyped)),
                a: Edge.terminal(),
                b: Edge.terminal(),
                untyped: Edge.terminal(),
            },
        )


def test_heterogeneous_fanout_duplicate_input_type_raises() -> None:
    """Two downstreams claiming the same ``input_type`` make routing
    ambiguous — reject."""
    src = _typed_agent("src", output_type=_DecompUnion)
    a1 = _typed_agent("a1", output_type=_Final, input_type=_ItemA)
    a2 = _typed_agent("a2", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    c = _typed_agent("c", output_type=_Final, input_type=_ItemC)
    with pytest.raises(SpecValidationError, match="ambiguous routing"):
        AgentGroup(
            name="hetero-dup",
            topology={
                src: Edge(to=(a1, a2, b, c)),
                a1: Edge.terminal(),
                a2: Edge.terminal(),
                b: Edge.terminal(),
                c: Edge.terminal(),
            },
        )


def test_heterogeneous_fanout_missing_union_member_raises() -> None:
    """If the union has a member with no matching downstream, items of
    that type would have nowhere to go — reject.
    """
    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    # Missing _ItemC handler.
    with pytest.raises(SpecValidationError, match="no matching downstream agent"):
        AgentGroup(
            name="hetero-missing",
            topology={
                src: Edge(to=(a, b)),
                a: Edge.terminal(),
                b: Edge.terminal(),
            },
        )


def test_heterogeneous_fanout_orphan_downstream_raises() -> None:
    """A downstream whose ``input_type`` isn't in the union would never
    receive any items — reject as a configuration bug.
    """

    class _Unrelated(BaseModel):
        z: bool

    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    c = _typed_agent("c", output_type=_Final, input_type=_ItemC)
    orphan = _typed_agent("orphan", output_type=_Final, input_type=_Unrelated)
    with pytest.raises(SpecValidationError, match="not in the union"):
        AgentGroup(
            name="hetero-orphan",
            topology={
                src: Edge(to=(a, b, c, orphan)),
                a: Edge.terminal(),
                b: Edge.terminal(),
                c: Edge.terminal(),
                orphan: Edge.terminal(),
            },
        )


def test_single_type_fanout_does_not_require_input_type_on_downstream() -> None:
    """Backward compat: ``FanOut[list[T]]`` (single-type) downstreams
    don't need ``input_type``. The validator only fires for multi-member
    union sources.
    """
    src = _typed_agent("src", output_type=_DecompSingle)
    untyped = _typed_agent("untyped", output_type=_Final, input_type=None)
    AgentGroup(
        name="single-type-ok",
        topology={
            src: Edge(to=(untyped,)),
            untyped: Edge.terminal(),
        },
    )


def test_heterogeneous_fanout_split_across_multiple_edges_validates() -> None:
    """Splitting union targets across separate ``Edge`` declarations
    (one edge per downstream) still validates — the validator collects
    targets across all outgoing edges of the source.
    """
    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    c = _typed_agent("c", output_type=_Final, input_type=_ItemC)
    AgentGroup(
        name="hetero-split-edges",
        topology={
            src: (Edge(to=(a,)), Edge(to=(b,)), Edge(to=(c,))),
            a: Edge.terminal(),
            b: Edge.terminal(),
            c: Edge.terminal(),
        },
    )


def test_heterogeneous_fanout_subclass_in_union_raises() -> None:
    """Two union members with a subclass relationship — isinstance
    routing would dispatch a child instance to BOTH handlers. Reject
    at construction.
    """
    src = _typed_agent("src", output_type=_SubclassUnion)
    a = _typed_agent("animal", output_type=_Final, input_type=_Animal)
    d = _typed_agent("dog", output_type=_Final, input_type=_Dog)
    with pytest.raises(SpecValidationError, match="subclass relationship"):
        AgentGroup(
            name="hetero-subclass",
            topology={
                src: Edge(to=(a, d)),
                a: Edge.terminal(),
                d: Edge.terminal(),
            },
        )


def test_heterogeneous_fanout_conditional_edge_raises() -> None:
    """A condition predicate on a heterogeneous source's outgoing edge
    would silently drop items routed to that downstream when the
    condition fires False. Reject at construction.
    """
    src = _typed_agent("src", output_type=_DecompUnion)
    a = _typed_agent("a", output_type=_Final, input_type=_ItemA)
    b = _typed_agent("b", output_type=_Final, input_type=_ItemB)
    c = _typed_agent("c", output_type=_Final, input_type=_ItemC)
    with pytest.raises(SpecValidationError, match="conditional edges are"):
        AgentGroup(
            name="hetero-conditional",
            topology={
                src: (
                    Edge(to=(a,)),
                    Edge(to=(b, c), condition=lambda _: True),
                ),
                a: Edge.terminal(),
                b: Edge.terminal(),
                c: Edge.terminal(),
            },
        )
