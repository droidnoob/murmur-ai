"""YAML-backed spec registry.

Loads :class:`Agent` definitions from a directory tree of ``.yaml`` files.
Satisfies :class:`murmur.core.protocols.registry.Registry` structurally —
required surface: ``get``, ``list``, ``validate``.

Phase 1 stub — full canonical YAML round-trip lands once :class:`Agent`
exposes its serialization surface.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from murmur.core.errors import RegistryError
from murmur.core.protocols.registry import ValidationErrors

if TYPE_CHECKING:
    from murmur.agent import Agent


class YamlRegistry:
    """File-backed spec registry rooted at a directory."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        if not self._root.is_dir():
            raise RegistryError(f"YAML registry root does not exist: {self._root}")

    @property
    def root(self) -> Path:
        return self._root

    def get(self, name: str) -> Agent:
        raise NotImplementedError(
            f"YamlRegistry.get({name!r}) — Phase 1 stub; awaiting Agent serialization"
        )

    def list(self) -> frozenset[str]:
        raise NotImplementedError(
            "YamlRegistry.list — Phase 1 stub; awaiting Agent serialization"
        )

    def validate(self) -> ValidationErrors:
        raise NotImplementedError(
            "YamlRegistry.validate — Phase 1 stub; awaiting Agent serialization"
        )


__all__ = ["YamlRegistry"]
