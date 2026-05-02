"""Execution backends — concretes satisfying ``core.protocols.Backend``.

Currently shipped: :class:`AsyncBackend` (default, in-process) and
:class:`JobBackend` (FastStream-driven, activated by passing a broker
URL to ``AgentRuntime``).

Import the ``Backend`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.backends.async_backend import AsyncBackend
from murmur.backends.job import JobBackend

__all__ = ["JobBackend", "AsyncBackend"]
