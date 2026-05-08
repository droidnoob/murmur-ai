"""Event-store concretes — :class:`InMemoryEventStore`, :class:`SQLiteEventStore`.

Both satisfy :class:`murmur.core.protocols.EventStore` structurally.
Pick the in-memory store for tests and ephemeral runs; pick the SQLite
store when ``murmur serve`` should retain history across restarts.
"""

from __future__ import annotations

from murmur.events.store.emitter import StoreEventEmitter
from murmur.events.store.memory import InMemoryEventStore
from murmur.events.store.sqlite import SQLiteEventStore
from murmur.events.store.usage import compute_usage

__all__ = [
    "InMemoryEventStore",
    "SQLiteEventStore",
    "StoreEventEmitter",
    "compute_usage",
]
