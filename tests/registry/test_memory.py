"""``InMemoryRegistry`` — runs the shared ``RegistryContract`` suite."""

from __future__ import annotations

import pytest
from pydantic import BaseModel
from tests.contracts.registry_contract import RegistryContract

from murmur.agent import Agent
from murmur.core.errors import RegistryError
from murmur.core.protocols.registry import Registry
from murmur.registry.memory import InMemoryRegistry
from murmur.types import TrustLevel


class _Out(BaseModel):
    x: int


def _agent(name: str) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
    )


class TestInMemoryRegistry(RegistryContract):
    @pytest.fixture
    def registry(self) -> Registry:
        return InMemoryRegistry([_agent("a"), _agent("b")])

    def test_get_returns_registered_agent(self, registry: Registry) -> None:
        agent = registry.get("a")
        assert agent.name == "a"

    def test_list_includes_registered_names(self, registry: Registry) -> None:
        assert registry.list() == frozenset({"a", "b"})


def test_register_duplicate_raises() -> None:
    reg = InMemoryRegistry([_agent("a")])
    with pytest.raises(RegistryError, match="already registered"):
        reg.register(_agent("a"))
