"""Context passers — concrete implementations of ``core.protocols.ContextPasser``.

Currently shipped: :class:`FullContextPasser` and :class:`NullContextPasser`.
Import the ``ContextPasser`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser

__all__ = [
    "FullContextPasser",
    "NullContextPasser",
]
