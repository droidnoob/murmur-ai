"""Shared contract suite for ``core.protocols.Registry``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur.agent import Agent
from murmur.core.errors import RegistryError
from murmur.core.protocols.registry import Registry
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


class RegistryContract:
    """Behavioural contract every ``Registry`` must satisfy."""

    @pytest.fixture
    def registry(self) -> Registry:
        raise NotImplementedError(
            "subclass must override `registry` fixture with a concrete instance"
        )

    def test_get_unknown_raises_registry_error(self, registry: Registry) -> None:
        with pytest.raises(RegistryError):
            registry.get("nope")

    def test_list_returns_frozenset(self, registry: Registry) -> None:
        names = registry.list()
        assert isinstance(names, frozenset)

    def test_validate_returns_list_of_strings(self, registry: Registry) -> None:
        errors = registry.validate()
        assert isinstance(errors, list)
        assert all(isinstance(e, str) for e in errors)
