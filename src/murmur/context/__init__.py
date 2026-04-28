"""Context passers — concrete implementations of ``core.protocols.ContextPasser``.

Phase 1 ships :class:`FullContextPasser` and :class:`NullContextPasser`.
``SummaryContextPasser`` and ``SelectiveContextPasser`` arrive in Phase 3.

Import the ``ContextPasser`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser

__all__ = [
    "FullContextPasser",
    "NullContextPasser",
]
