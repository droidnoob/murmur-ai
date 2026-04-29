"""Tool registry + the default ``StaticToolProvider``.

The :class:`StaticToolProvider` here satisfies
:class:`murmur.core.protocols.tools.ToolProvider` structurally. The Protocol
itself lives in ``core.protocols`` — never duplicate it here.

:class:`ToolRegistry` is a concrete data store, not a pluggable, and stays
in this module.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from murmur.core.errors import RegistryError

ToolFunc = Callable[..., Awaitable[object]]
"""Concrete tool callables registered against a name."""


class StaticToolProvider:
    """Fixed allow-list ``ToolProvider``."""

    def __init__(self, allowed: frozenset[str]) -> None:
        self._allowed = allowed

    def resolve(
        self,
        agent_name: str,  # noqa: ARG002 — protocol arg
        requested: frozenset[str],
    ) -> frozenset[str]:
        return self._allowed & requested


class ToolRegistry:
    """In-memory map of tool name → callable. Not a pluggable; data store only."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolFunc] = {}

    def register(self, name: str, func: ToolFunc) -> None:
        if name in self._tools:
            raise RegistryError(f"tool '{name}' is already registered")
        self._tools[name] = func

    def get(self, name: str) -> ToolFunc:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise RegistryError(f"tool '{name}' not found") from exc

    def names(self) -> frozenset[str]:
        return frozenset(self._tools.keys())


__all__ = [
    "StaticToolProvider",
    "ToolFunc",
    "ToolRegistry",
]
