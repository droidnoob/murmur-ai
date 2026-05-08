"""FastStream-backed broker wrapper.

Handles ``kafka://``, ``nats://``, ``amqp://``, and ``redis://`` URLs by
constructing the matching FastStream broker class internally. The chosen
broker class is imported **lazily** so that users only need the relevant
extra installed (``murmur-ai[kafka]`` etc.) rather than all four.

This is the only place in the package outside :mod:`murmur.interop` that
imports from :mod:`faststream`. Per CLAUDE.md §2, FastStream is a hidden
dependency — users never see it.

Subscriber registration uses FastStream's runtime-add API
(``sub = broker.subscriber(topic); sub(handler); await sub.start()``)
so subscriptions can be added after the broker is started — matching
the :class:`Broker` Protocol's flexibility.

Tests inject a FastStream broker via the ``_fs_broker`` constructor arg
and wrap it with ``faststream.<broker>.TestBroker`` so the full wire
format is exercised in-memory without Docker. Real-broker integration
tests (testcontainers) land separately.
"""

from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING, Any

from murmur.core.errors import SpecValidationError

if TYPE_CHECKING:
    from murmur.core.protocols.broker import MessageHandler


_SCHEME_TO_EXTRA: dict[str, str] = {
    "kafka": "kafka",
    "nats": "nats",
    "amqp": "rabbitmq",
    "redis": "redis",
}


class FastStreamBroker:
    """Wraps a FastStream broker behind the :class:`Broker` Protocol."""

    def __init__(
        self,
        *,
        scheme: str,
        url: str,
        _fs_broker: Any | None = None,
    ) -> None:
        if scheme not in _SCHEME_TO_EXTRA:
            raise SpecValidationError(
                f"unsupported broker scheme {scheme!r}; "
                f"expected one of {sorted(_SCHEME_TO_EXTRA)}"
            )
        self._scheme = scheme
        self._url = url
        self._extra = _SCHEME_TO_EXTRA[scheme]
        # ``_fs_broker`` is the public test seam: callers wrapping the
        # FastStream broker with its ``TestBroker`` context manager pass
        # the underlying broker in here so this wrapper drives it directly
        # without spinning up a real connection.
        self._fs_broker: Any | None = _fs_broker
        self._injected: bool = _fs_broker is not None
        self._started: bool = False
        self._subscriptions: list[Any] = []

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def url(self) -> str:
        return self._url

    @property
    def fs_broker(self) -> Any:
        """The underlying FastStream broker (``KafkaBroker``, ``NatsBroker``,
        ``RabbitBroker``, or ``RedisBroker``).

        ``None`` until :meth:`start` lazily constructs it (production mode)
        — call ``await broker.start()`` first if you need it pre-warmed. In
        injected/test mode (``_fs_broker=`` was passed at construction) this
        is non-``None`` from the start.

        Exposed so users mounting Murmur via :class:`AgentRouter` can register
        their own ``@fs_broker.subscriber("user.events")`` handlers next to
        Murmur's. Treated as a documented re-export of FastStream's broker —
        consult the FastStream docs for its full surface.
        """
        return self._fs_broker

    async def start(self) -> None:
        """Connect the FastStream broker. Idempotent.

        When ``_fs_broker`` was injected (test mode) the broker is assumed
        to already be running inside the caller's ``TestBroker`` context;
        we just flip the started flag. In production mode we build the
        per-scheme broker lazily and call its ``start`` method.
        """
        if self._started:
            return
        if self._fs_broker is None:
            self._fs_broker = self._build_fs_broker()
            await self._fs_broker.start()
        self._started = True

    async def stop(self) -> None:
        """Stop subscribers and close the broker. Idempotent."""
        if not self._started:
            return
        for sub in self._subscriptions:
            with contextlib.suppress(Exception):
                await sub.stop()
        self._subscriptions.clear()
        if not self._injected and self._fs_broker is not None:
            with contextlib.suppress(Exception):
                await self._fs_broker.close()
        self._started = False

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._started or self._fs_broker is None:
            raise RuntimeError("FastStreamBroker.publish called before start()")
        # Redis: publish to a Stream rather than a Pub/Sub channel so the
        # message persists and consumer groups (the competing-consumer
        # path used by Worker fleets) can claim it. Channels would silently
        # drop messages whose subscribers happen to be on Streams. Other
        # schemes — Kafka topics, NATS subjects, RabbitMQ queues — already
        # share one namespace between publish and subscribe.
        if self._scheme == "redis":
            await self._fs_broker.publish(payload, stream=topic)
            return
        await self._fs_broker.publish(payload, topic)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
    ) -> None:
        if not self._started or self._fs_broker is None:
            raise RuntimeError("FastStreamBroker.subscribe called before start()")
        sub = self._build_subscriber(topic, group)
        sub(_wrap_handler(handler))
        await sub.start()
        self._subscriptions.append(sub)

    def _build_subscriber(self, topic: str, group: str | None) -> Any:
        """Construct a FastStream subscriber matching ``group`` semantics.

        ``group=None`` keeps the per-broker default (Pub/Sub channel for
        Redis, fresh consumer group per process for Kafka, no queue group
        for NATS, anonymous queue for RabbitMQ — all broadcast-flavoured).

        ``group=<str>`` forces competing-consumer semantics so multiple
        subscribers sharing a ``(topic, group)`` pair pool one queue and
        each message is delivered to exactly one of them. The mapping
        differs by broker — Redis uses Streams + consumer group, Kafka
        uses ``group_id``, NATS uses queue groups, RabbitMQ binds a named
        durable queue to the topic exchange.
        """
        broker = self._fs_broker
        assert broker is not None  # checked by caller
        # Redis is special: publish goes to a Stream (see ``publish``), so
        # both branches here must subscribe to the same Stream namespace.
        # Without a group, each subscriber reads the whole stream (broadcast
        # — used for runtime-id-scoped reply topics that only ever have
        # one subscriber anyway). With a group, FastStream wires a Redis
        # Streams consumer group so multiple subscribers compete.
        if self._scheme == "redis":
            from faststream.redis import StreamSub

            if group is None:
                return broker.subscriber(stream=StreamSub(topic))
            # Consumer name must be unique per subscriber inside the group;
            # the group itself is shared across the fleet.
            return broker.subscriber(
                stream=StreamSub(topic, group=group, consumer=uuid.uuid4().hex),
            )

        if group is None:
            return broker.subscriber(topic)

        if self._scheme == "kafka":
            return broker.subscriber(topic, group_id=group)
        if self._scheme == "nats":
            return broker.subscriber(topic, queue=group)
        # RabbitMQ: ``broker.subscriber(topic)`` declares a queue named
        # ``topic`` and binds it to the default exchange — multiple
        # consumers attaching to that same queue compete for messages
        # natively (AMQP basic.consume). The ``group`` parameter has no
        # additional effect beyond what the default already provides, so
        # we use the same call as the broadcast branch. Note: Rabbit is
        # therefore *always* competing-consumer in this wrapper. That's
        # fine for current production callers — every broadcast use
        # (``ResultCollector`` reply topic, ``JobBackend`` events relay)
        # is per-runtime-id and only ever has one subscriber.
        return broker.subscriber(topic)

    # ------------------------------------------------------------------ helpers

    def _build_fs_broker(self) -> Any:
        """Lazy-import the right FastStream broker class for ``self._scheme``.

        Passes ``logger=None`` to every concrete to suppress FastStream's
        own subscriber-registration / startup chatter — Murmur's worker
        emits its own banner via :func:`murmur.worker.worker.Worker.start`
        listing the agents and topics, so two flavours of the same
        information would just be noise. Underlying-library loggers
        (``aiokafka``, ``aio_pika``, ``redis``, ``nats``) get bumped to
        ``WARNING`` separately in :func:`_quiet_underlying_loggers`.
        """
        _quiet_underlying_loggers()
        if self._scheme == "kafka":
            from faststream.kafka import KafkaBroker

            # ``KafkaBroker`` wants raw bootstrap servers, not a URL.
            servers = self._url.removeprefix("kafka://") or "localhost:9092"
            return KafkaBroker(servers, logger=None)
        if self._scheme == "nats":
            from faststream.nats import NatsBroker

            return NatsBroker(self._url, logger=None)
        if self._scheme == "amqp":
            from faststream.rabbit import RabbitBroker

            return RabbitBroker(self._url, logger=None)
        if self._scheme == "redis":
            from faststream.redis import RedisBroker

            return RedisBroker(self._url, logger=None)
        raise SpecValidationError(  # pragma: no cover — caught in __init__
            f"no FastStream broker class wired for scheme {self._scheme!r}"
        )


_QUIETED_LOGGERS = False


def _quiet_underlying_loggers() -> None:
    """Raise broker-lib loggers to WARNING so successful runs are silent.

    ``aiokafka`` etc. log DNS / connection / consumer-group lifecycle at INFO
    by default — useful for debugging, noisy in routine startup. Errors
    (WARNING+) still surface. Idempotent: only adjusts levels once per
    process so user-overridden levels don't get fought over.
    """
    global _QUIETED_LOGGERS
    if _QUIETED_LOGGERS:
        return
    import logging

    for name in ("aiokafka", "aio_pika", "redis", "nats", "faststream"):
        logger = logging.getLogger(name)
        if logger.level < logging.WARNING:
            logger.setLevel(logging.WARNING)
    _QUIETED_LOGGERS = True


def _wrap_handler(handler: MessageHandler) -> Any:
    """Wrap our ``bytes -> Awaitable[None]`` handler for FastStream.

    FastStream's signature-driven dispatcher will parse a JSON-shaped body
    into a dict if the annotation is ``bytes`` (it sees the leading ``{`` and
    auto-decodes), then fail Pydantic validation against ``bytes``. We side-
    step that by pulling the raw frame via ``Context("message.body")`` —
    this stays bytes regardless of payload shape and keeps our
    :class:`Broker` Protocol format-agnostic.
    """
    from faststream import Context

    async def _on_message(body: bytes = Context("message.body")) -> None:
        await handler(body)

    return _on_message


__all__ = ["FastStreamBroker"]
