"""Unit tests for ``murmur.groups.spec.AgentGroup`` validation."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur.agent import Agent
from murmur.context.null import NullContextPasser
from murmur.core.errors import TopologyError
from murmur.groups.edge import Edge
from murmur.groups.spec import AgentGroup
from murmur.types import TrustLevel


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
