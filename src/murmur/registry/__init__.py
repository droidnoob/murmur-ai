"""Spec registries — concretes satisfying ``core.protocols.Registry``.

Import the ``Registry`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.registry.memory import InMemoryRegistry
from murmur.registry.yaml import YamlRegistry

__all__ = ["InMemoryRegistry", "YamlRegistry"]
