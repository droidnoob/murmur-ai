"""Runtime instrumentation events — value types + emitter concretes.

The Protocol :class:`murmur.core.protocols.events.EventEmitter` is the
abstract sink; concretes here (and the ``LogEventEmitter`` /
``SSEEventEmitter`` / ``MultiEventEmitter`` to be added) satisfy it
structurally. :class:`RuntimeEvent` is the typed envelope every event
flows through.

:class:`OTelMetricsEmitter` is the OTel GenAI metrics adapter. Lazy-
imported because it depends on the optional ``murmur-ai[otel]`` extra;
``from murmur.events import OTelMetricsEmitter`` raises a clear
:class:`ImportError` when the extra isn't installed.
"""

from typing import TYPE_CHECKING, Any

from murmur.events.broker import BrokerEventBridge
from murmur.events.log import LogEventEmitter
from murmur.events.multi import MultiEventEmitter
from murmur.events.sse import SSEEventEmitter
from murmur.events.types import EventType, RuntimeEvent

if TYPE_CHECKING:
    from murmur.events.otel import OTelMetricsEmitter

__all__ = [
    "BrokerEventBridge",
    "EventType",
    "LogEventEmitter",
    "MultiEventEmitter",
    "OTelMetricsEmitter",
    "RuntimeEvent",
    "SSEEventEmitter",
]


def __getattr__(name: str) -> Any:
    """Lazy-load :class:`OTelMetricsEmitter` so the OTel SDK is only
    imported when the user actually opts in.

    Re-exports it as ``murmur.events.OTelMetricsEmitter`` without paying
    the import cost on ``import murmur.events`` for users that don't want
    metrics.
    """
    if name == "OTelMetricsEmitter":
        from murmur.events.otel import OTelMetricsEmitter

        return OTelMetricsEmitter
    raise AttributeError(f"module 'murmur.events' has no attribute {name!r}")
