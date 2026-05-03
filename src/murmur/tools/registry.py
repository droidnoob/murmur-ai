"""Tool registry + the default ``StaticToolProvider``.

The :class:`StaticToolProvider` here satisfies
:class:`murmur.core.protocols.tools.ToolProvider` structurally. The Protocol
itself lives in ``core.protocols`` — never duplicate it here.

:class:`ToolRegistry` is a concrete data store, not a pluggable, and stays
in this module.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from murmur.core.errors import RegistryError

T = TypeVar("T")

ToolFunc = Callable[..., Awaitable[T]]
"""Tool callable parameterised by its result type.

Use bare ``ToolFunc`` (or explicitly ``ToolFunc[Any]``) when you don't care
about the result type — the registry stores tools heterogeneously and
returns ``ToolFunc[Any]``. Use ``ToolFunc[MyType]`` in user code to keep the
return-type information visible to type checkers when the caller knows the
shape of the tool's output.

>>> async def web_search(query: str) -> str: ...
>>> typed: ToolFunc[str] = web_search        # ty narrows to ``Awaitable[str]``
>>> registry.register("web_search", typed)   # registry erases T to Any
"""


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
        # ``Any`` here is genuinely correct, not a workaround: the registry is
        # heterogeneous by design (one slot holds ``ToolFunc[str]``, the next
        # ``ToolFunc[MyModel]``). There's no single ``T`` that fits the dict.
        self._tools: dict[str, ToolFunc[Any]] = {}

    def register(self, name: str, func: ToolFunc[T]) -> None:
        """Register a tool under ``name``, preserving its return type at the call site.

        The method is generic over ``T`` so a caller passing ``ToolFunc[str]``
        keeps the typed view in their own code. The registry erases ``T`` to
        ``Any`` on storage (see ``__init__``); retrieval via :meth:`get`
        returns ``ToolFunc[Any]`` because no caller can know the original
        ``T`` at lookup time.
        """
        if name in self._tools:
            raise RegistryError(f"tool '{name}' is already registered")
        self._tools[name] = func

    def get(self, name: str) -> ToolFunc[Any]:
        # ``Any`` because the registry forgets ``T`` on storage — see ``__init__``.
        try:
            return self._tools[name]
        except KeyError as exc:
            raise RegistryError(f"tool '{name}' not found") from exc

    def names(self) -> frozenset[str]:
        return frozenset(self._tools.keys())

    def unregister(self, name: str) -> None:
        """Remove ``name`` from the registry. Idempotent — silent on miss.

        Used for short-lived tool registrations (e.g. ``AgentTeam``'s
        per-run ``delegate`` tool) where the registration scope is one
        ``runtime.run_group(team, ...)`` call rather than the runtime's
        lifetime.
        """
        self._tools.pop(name, None)


__all__ = [
    "StaticToolProvider",
    "ToolFunc",
    "ToolRegistry",
]
