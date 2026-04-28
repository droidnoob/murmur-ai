"""Registry Protocol — name → Agent lookup.

Concrete registries (`InMemoryRegistry`, `YamlRegistry`) match structurally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeAlias

if TYPE_CHECKING:
    from murmur.agent import Agent

# `list` is part of the Protocol surface and shadows the built-in inside the
# class body — alias the validation-error return type to keep annotations clear.
ValidationErrors: TypeAlias = "list[str]"


class Registry(Protocol):
    """Pluggable spec registry."""

    def get(self, name: str) -> Agent:
        """Return the registered agent ``name`` or raise ``RegistryError``."""
        ...

    def list(self) -> frozenset[str]:
        """Return the set of registered agent names."""
        ...

    def validate(self) -> ValidationErrors:
        """Return a list of human-readable validation errors (empty == OK)."""
        ...


__all__ = ["Registry"]
