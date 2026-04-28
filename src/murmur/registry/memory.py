"""In-memory spec registry — primarily for tests and ad-hoc scripts.

Satisfies :class:`murmur.core.protocols.registry.Registry` structurally —
required surface: ``get``, ``list``, ``validate``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from murmur.core.errors import RegistryError
from murmur.core.protocols.registry import ValidationErrors

if TYPE_CHECKING:
    from murmur.agent import Agent


class InMemoryRegistry:
    """Holds :class:`Agent` instances by name."""

    def __init__(self, agents: Iterable[Agent] = ()) -> None:
        self._agents: dict[str, Agent] = {a.name: a for a in agents}

    def register(self, agent: Agent) -> None:
        if agent.name in self._agents:
            raise RegistryError(f"agent '{agent.name}' already registered")
        self._agents[agent.name] = agent

    def get(self, name: str) -> Agent:
        try:
            return self._agents[name]
        except KeyError as exc:
            raise RegistryError(f"agent '{name}' not found") from exc

    def list(self) -> frozenset[str]:
        return frozenset(self._agents.keys())

    def validate(self) -> ValidationErrors:
        """In-memory specs are validated at registration time — nothing to do."""
        return []


__all__ = ["InMemoryRegistry"]
