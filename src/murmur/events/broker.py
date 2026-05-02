"""BrokerEventBridge — publish RuntimeEvents onto a broker topic.

The :class:`Worker` installs one of these in its runtime's emitter chain
so per-agent / per-tool events fired worker-side can flow back to the
publisher's runtime — which subscribes to the same topic and forwards
each received event into its own emitter.

The target topic varies per task (the publisher includes
``events_topic`` on each :class:`TaskMessage`), so the bridge reads the
target from a :class:`contextvars.ContextVar`. The worker binds the
contextvar for the duration of each run via :func:`bind_event_topic` /
:func:`reset_event_topic`. When unbound, the bridge no-ops — it never
publishes events that weren't explicitly opted into.

Wire format reuses :meth:`RuntimeEvent.model_dump_json` /
:meth:`RuntimeEvent.model_validate_json`. Errors during publish are
swallowed: observability must never take an agent run down. Pair via
:class:`MultiEventEmitter` so local sinks (Log, SSE, custom) keep
working alongside the relay.
"""

from __future__ import annotations

import contextlib
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from murmur.core.protocols.broker import Broker
    from murmur.events.types import RuntimeEvent


_EVENT_TOPIC: ContextVar[str | None] = ContextVar("murmur_event_topic", default=None)
"""Active relay topic for the current asyncio task tree.

Set by :class:`Worker` per-task before calling ``runtime.run``; reset
immediately after. AsyncIO automatically inherits contextvars into child
tasks, so events fired from nested coroutines (tool calls, sub-stages)
are routed to the same topic as their parent run.
"""


def bind_event_topic(topic: str | None) -> object:
    """Set the active relay topic for this asyncio context tree.

    Returns the reset token that callers must pass to
    :func:`reset_event_topic` once the run finishes. Pass ``topic=None``
    to explicitly suppress relay for a child run.
    """
    return _EVENT_TOPIC.set(topic)


def reset_event_topic(token: object) -> None:
    """Restore the previous relay topic. ``token`` comes from
    :func:`bind_event_topic`."""
    _EVENT_TOPIC.reset(token)  # ty: ignore[invalid-argument-type]  # ContextVar.Token


def current_event_topic() -> str | None:
    """The relay topic bound for the current context, or ``None``.

    Exposed for tests; production code goes through the bridge."""
    return _EVENT_TOPIC.get()


class BrokerEventBridge:
    """:class:`EventEmitter` that relays each event to a broker topic.

    The topic is read from the per-task contextvar — see
    :func:`bind_event_topic`. When no topic is bound, ``emit`` is a
    no-op, so adding the bridge to a runtime's emitter chain has zero
    cost when distributed observability isn't requested.
    """

    def __init__(self, broker: Broker) -> None:
        self._broker = broker

    async def emit(self, event: RuntimeEvent) -> None:
        topic = _EVENT_TOPIC.get()
        if topic is None:
            return
        # Observability must never take an agent run down. A broker that
        # isn't yet started, a serialisation hiccup, or a network blip
        # all swallow here — the local emitter (paired via Multi) still
        # captured the event before we got to the bridge.
        with contextlib.suppress(Exception):
            await self._broker.publish(topic, event.model_dump_json().encode())


__all__ = [
    "BrokerEventBridge",
    "bind_event_topic",
    "current_event_topic",
    "reset_event_topic",
]
