"""Runtime instrumentation events — value types + emitter concretes.

The Protocol :class:`murmur.core.protocols.events.EventEmitter` is the
abstract sink; concretes here (and the ``LogEventEmitter`` /
``SSEEventEmitter`` / ``MultiEventEmitter`` to be added) satisfy it
structurally. :class:`RuntimeEvent` is the typed envelope every event
flows through.
"""

from murmur.events.broker import BrokerEventBridge
from murmur.events.log import LogEventEmitter
from murmur.events.multi import MultiEventEmitter
from murmur.events.sse import SSEEventEmitter
from murmur.events.types import EventType, RuntimeEvent

__all__ = [
    "BrokerEventBridge",
    "EventType",
    "LogEventEmitter",
    "MultiEventEmitter",
    "RuntimeEvent",
    "SSEEventEmitter",
]
