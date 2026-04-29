"""Execution backends — concretes satisfying ``core.protocols.Backend``.

Currently shipped: :class:`ThreadBackend` (default, in-process) and
:class:`JobBackend` (FastStream-driven, activated by passing a broker
URL to ``AgentRuntime``).

Import the ``Backend`` Protocol from :mod:`murmur.core.protocols`.
"""

from murmur.backends.job import JobBackend
from murmur.backends.thread import ThreadBackend

__all__ = ["JobBackend", "ThreadBackend"]
