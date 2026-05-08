"""All Murmur Protocols. Written before any concrete implementation.

Concrete classes (in sibling packages) satisfy these Protocols via
**structural typing** — never inheritance. Tests are keyed on the Protocol so
every implementation runs the same shared contract suite.
"""

from murmur.core.protocols.backend import Backend, BackendStatus
from murmur.core.protocols.broker import Broker, MessageHandler
from murmur.core.protocols.context import ContextPasser
from murmur.core.protocols.event_store import EventStore
from murmur.core.protocols.events import EventEmitter
from murmur.core.protocols.pipeline import Middleware, NextStage, Pipeline, Stage
from murmur.core.protocols.registry import Registry
from murmur.core.protocols.router import RouteDecision, Router
from murmur.core.protocols.tools import ToolExecutor, ToolProvider
from murmur.core.protocols.toolsets import ToolDescriptor, ToolsetProvider
from murmur.core.protocols.worker import OnComplete, OnError, OnStart, Worker

__all__ = [
    "Backend",
    "BackendStatus",
    "Broker",
    "ContextPasser",
    "EventEmitter",
    "EventStore",
    "MessageHandler",
    "Middleware",
    "NextStage",
    "OnComplete",
    "OnError",
    "OnStart",
    "Pipeline",
    "Registry",
    "RouteDecision",
    "Router",
    "Stage",
    "ToolDescriptor",
    "ToolExecutor",
    "ToolProvider",
    "ToolsetProvider",
    "Worker",
]
