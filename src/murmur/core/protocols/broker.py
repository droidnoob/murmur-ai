"""Broker Protocol — message-bus abstraction used by ``JobBackend`` / ``Worker``.

This is **not** part of the public API. End users never see ``Broker`` —
they pass a broker URL string to :class:`murmur.AgentRuntime`, and the
runtime constructs the right concrete internally. The Protocol exists so
that:

- The single in-memory broker (``backends._inmemory_broker``) and the four
  FastStream wrappers (``backends._faststream_broker``) all match the same
  shape — Murmur dispatch logic is broker-agnostic.
- Tests can inject a fake without pulling in FastStream.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, TypeAlias

MessageHandler: TypeAlias = Callable[[bytes], Awaitable[None]]
"""Callback fired for each message that arrives on a subscribed topic."""


class Broker(Protocol):
    """Minimum viable pub/sub surface.

    Implementations must be safe to call concurrently. ``publish`` does not
    wait for handlers to finish — fire-and-forget — but should report failure
    to schedule the dispatch (e.g. broker is closed) by raising.
    """

    async def start(self) -> None:
        """Connect / open the broker. Idempotent."""
        ...

    async def stop(self) -> None:
        """Disconnect cleanly and drop all subscriptions. Idempotent."""
        ...

    async def publish(self, topic: str, payload: bytes) -> None:
        """Publish ``payload`` to ``topic``. Does not wait for delivery."""
        ...

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
        prefetch: int | None = None,
    ) -> None:
        """Register ``handler`` for messages on ``topic``.

        Two delivery semantics, picked via ``group``:

        - ``group=None`` (default) — **broadcast / pub-sub**. Every subscriber
          on the topic receives every message. Used for runtime-id-scoped
          reply topics (only ever one subscriber anyway) and event-stream
          observers that all want the same payload.
        - ``group=<str>`` — **competing-consumer**. All subscribers that
          share the same ``(topic, group)`` pair are pooled, and each
          published message is delivered to exactly one of them. Used by
          :class:`murmur.worker.Worker` so multiple workers serving the same
          agent split the workload instead of duplicating it. Per-broker
          mapping: Redis Streams consumer group, Kafka consumer ``group_id``,
          NATS queue group, a named RabbitMQ queue.

        Multiple handlers per topic are allowed regardless of mode.
        Subscriptions persist until :meth:`stop` is called — there is no
        per-subscription unsubscribe in this Protocol; restart the broker
        if you need to fully reset.

        ``prefetch`` (when not ``None``) bounds how many messages this
        subscriber holds at once. **Effective semantics differ by broker:**

        - Redis: forwarded to ``StreamSub(max_records=...)`` — true
          per-poll batch cap. ``prefetch=1`` gives the most uniform
          fan-out across a Worker fleet.
        - NATS: forwarded to ``pending_msgs_limit`` — in-flight backpressure
          cap, not per-poll batch. Bounds buffer size before the server
          stops pushing.
        - Kafka, RabbitMQ: currently a no-op. FastStream's Kafka
          ``DefaultSubscriber`` ignores ``max_records``, and AMQP channel
          QoS lives on a different API than the wrapper exposes. Future
          change will switch Kafka to batch mode and call
          ``channel.set_qos`` for Rabbit.

        ``None`` (default) lets the underlying broker pick.
        """
        ...


__all__ = ["Broker", "MessageHandler"]
