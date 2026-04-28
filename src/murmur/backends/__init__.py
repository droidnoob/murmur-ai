"""Execution backends — concretes satisfying ``core.protocols.Backend``.

Phase 1 ships :class:`ThreadBackend` (default) and :class:`JobBackend`
(FastStream-driven, activated by passing a broker URL to ``AgentRuntime``).
``ProcessBackend`` and ``ContainerBackend`` arrive in later phases.

Import the ``Backend`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.backends.job import JobBackend
from murmur.backends.thread import ThreadBackend

__all__ = ["JobBackend", "ThreadBackend"]
