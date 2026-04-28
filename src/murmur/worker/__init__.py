"""Distributed worker — concrete satisfying ``core.protocols.Worker``.

Import the ``Worker`` Protocol (and ``OnStart`` / ``OnComplete`` / ``OnError``
hook types) from :mod:`murmur.core.protocols`.
"""

from murmur.worker.worker import Worker

__all__ = ["Worker"]
