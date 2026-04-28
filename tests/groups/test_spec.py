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
