"""FastStream-backed broker concretes — one class per scheme.

Each scheme — ``kafka://``, ``nats://``, ``amqp://``, ``redis://`` — has
its own concrete class implementing the :class:`Broker` Protocol with
honest, scheme-specific behaviour. Lazy imports keep the dependency
fan-out clean: a user only needs the relevant ``murmur-ai[<scheme>]``
extra installed to construct that scheme's class.

This module is the only place in the package outside :mod:`murmur.interop`
that imports from :mod:`faststream`. Per CLAUDE.md §2, FastStream is a
hidden dependency — users never see it.

Design — composition, not inheritance. Each scheme class holds a private
:class:`_FastStreamCore` that owns the lifecycle state (started flag,
``_fs_broker`` test seam, subscription bookkeeping). Per CLAUDE.md §13
("no inheritance for code reuse — composition only" / "Protocols over
ABCs"), the four scheme classes are independent and satisfy the Broker
Protocol structurally. Per-call paths carry zero scheme branching; the
URL → class dispatch happens once at construction via the
:func:`FastStreamBroker` factory.

Tests inject a FastStream broker via the ``_fs_broker`` constructor arg
and wrap it with ``faststream.<broker>.TestBroker`` so the full wire
format is exercised in-memory without Docker. Real-broker integration
tests (``testcontainers``) cover the production path.
"""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeAlias

from murmur.core.errors import SpecValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from murmur.core.protocols.broker import MessageHandler


_SCHEME_TO_EXTRA: dict[str, str] = {
    "kafka": "kafka",
    "nats": "nats",
    "amqp": "rabbitmq",
    "redis": "redis",
}


# ---------------------------------------------------------------------------
# Shared lifecycle scaffolding (composed into every scheme class).
# ---------------------------------------------------------------------------


@dataclass
class _FastStreamCore:
    """Shared lifecycle state composed into each scheme broker.

    Owns the ``_fs_broker`` test seam, the started flag, and the
    subscription roster. Each scheme broker delegates ``start`` /
    ``stop`` here and otherwise reaches in only for ``fs_broker``.
    """

    scheme: str
    url: str
    fs_broker: Any | None = None
    injected: bool = False
    started: bool = False
    subscriptions: list[Any] = field(default_factory=list)

    async def start(self, build_fs: Callable[[], Any]) -> None:
        """Connect the FastStream broker. Idempotent.

        Injected (test) mode: the broker is already running inside the
        caller's ``TestBroker`` context — just flip the started flag.
        Production mode: build the per-scheme broker lazily (``build_fs``)
        and call its ``start`` method.
        """
        if self.started:
            return
        if self.fs_broker is None:
            self.fs_broker = build_fs()
            await self.fs_broker.start()
        self.started = True

    async def stop(self) -> None:
        """Stop subscribers and close the broker. Idempotent."""
        if not self.started:
            return
        for sub in self.subscriptions:
            with contextlib.suppress(Exception):
                await sub.stop()
        self.subscriptions.clear()
        if not self.injected and self.fs_broker is not None:
            with contextlib.suppress(Exception):
                await self.fs_broker.close()
        self.started = False


# ---------------------------------------------------------------------------
# Per-scheme concretes. Each satisfies the Broker Protocol structurally.
# ---------------------------------------------------------------------------


class _FastStreamSchemeMixin:
    """Shared property surface — ``scheme`` / ``url`` / ``fs_broker``.

    Not a base class for behaviour. Each concrete inherits *only* the
    three property accessors so the dispatch layer (``server.router``)
    and operator-facing inspection points see a uniform shape regardless
    of scheme. Polymorphic methods (``publish``, ``subscribe``,
    ``_build_fs_broker``, ``_build_subscriber``) live on each concrete.
    """

    _core: _FastStreamCore

    @property
    def scheme(self) -> str:
        return self._core.scheme

    @property
    def url(self) -> str:
        return self._core.url

    @property
    def fs_broker(self) -> Any:
        """The underlying FastStream broker instance.

        ``None`` until :meth:`start` lazily constructs it (production
        mode). In injected/test mode (``_fs_broker=`` was passed at
        construction) this is non-``None`` from the start.

        Exposed so users mounting Murmur via :class:`AgentRouter` can
        register their own ``@fs_broker.subscriber("user.events")``
        handlers next to Murmur's. Treated as a documented re-export of
        FastStream's broker — consult the FastStream docs for its full
        surface.
        """
        return self._core.fs_broker


class FastStreamRedisBroker(_FastStreamSchemeMixin):
    """Redis Streams broker — the only scheme with first-class fan-out.

    Publishes go to a Redis Stream (``XADD``) so messages persist and
    consumer groups (``XREADGROUP``) can claim them. Subscribers always
    use Streams — never Pub/Sub channels — so publish and subscribe
    share one namespace.

    Honours every Worker fan-out knob:

    - ``group``: Redis Streams consumer group. Members compete for
      entries via ``XREADGROUP``.
    - ``prefetch``: ``StreamSub.max_records`` — true per-poll batch cap.
      ``prefetch=1`` produces uniform fan-out across a Worker fleet at
      one extra round-trip per task.
    - ``consumer_id``: stable consumer name inside the group. Restart
      with the same id reclaims that consumer's pending entries; the
      ``XINFO GROUPS`` consumer roster stays bounded by fleet size,
      not restart count. Falls back to ``uuid4`` when ``None``.
    """

    def __init__(self, *, url: str, _fs_broker: Any | None = None) -> None:
        self._core = _FastStreamCore(
            scheme="redis",
            url=url,
            fs_broker=_fs_broker,
            injected=_fs_broker is not None,
        )

    async def start(self) -> None:
        await self._core.start(self._build_fs_broker)

    async def stop(self) -> None:
        await self._core.stop()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamRedisBroker.publish called before start()")
        # Stream — not Pub/Sub channel — so messages persist for any
        # consumer group still draining the backlog. Channels would
        # silently drop messages whose subscribers happen to be on
        # Streams (the Worker-fan-out path).
        await self._core.fs_broker.publish(payload, stream=topic)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
        prefetch: int | None = None,
        consumer_id: str | None = None,
    ) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamRedisBroker.subscribe called before start()")
        from faststream.redis import StreamSub

        if group is None:
            stream = (
                StreamSub(topic, max_records=prefetch)
                if prefetch is not None
                else StreamSub(topic)
            )
            sub = self._core.fs_broker.subscriber(stream=stream)
        else:
            # Stable ``consumer_id`` lets the same Worker reclaim its
            # pending entries on restart and keeps ``XINFO GROUPS``
            # consumer count bounded by fleet size; ``None`` falls back
            # to a random uuid (leaks pending entries if the subscriber
            # dies without acking — caller's responsibility).
            consumer = consumer_id if consumer_id is not None else uuid.uuid4().hex
            sub = self._core.fs_broker.subscriber(
                stream=StreamSub(
                    topic,
                    group=group,
                    consumer=consumer,
                    max_records=prefetch,
                ),
            )
        sub(_wrap_handler(handler))
        await sub.start()
        self._core.subscriptions.append(sub)

    def _build_fs_broker(self) -> Any:
        _quiet_underlying_loggers()
        from faststream.redis import RedisBroker

        return RedisBroker(self._core.url, logger=None)


class FastStreamKafkaBroker(_FastStreamSchemeMixin):
    """Kafka broker — competing-consumer via consumer ``group_id``.

    ``prefetch`` is currently a no-op: ``max_records`` is only honoured
    by FastStream's ``BatchSubscriber``, and Worker subscribes in
    single-message mode. ``consumer_id`` is also a no-op — Kafka
    identifies consumers via ``group_id`` plus partition assignment,
    not an operator-supplied name. A future change can switch the
    Worker path to batch mode if per-poll fairness ever matters here.
    """

    def __init__(self, *, url: str, _fs_broker: Any | None = None) -> None:
        self._core = _FastStreamCore(
            scheme="kafka",
            url=url,
            fs_broker=_fs_broker,
            injected=_fs_broker is not None,
        )

    async def start(self) -> None:
        await self._core.start(self._build_fs_broker)

    async def stop(self) -> None:
        await self._core.stop()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamKafkaBroker.publish called before start()")
        await self._core.fs_broker.publish(payload, topic)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
        prefetch: int | None = None,  # noqa: ARG002 — see class docstring
        consumer_id: str | None = None,  # noqa: ARG002 — see class docstring
    ) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamKafkaBroker.subscribe called before start()")
        if group is not None:
            sub = self._core.fs_broker.subscriber(topic, group_id=group)
        else:
            sub = self._core.fs_broker.subscriber(topic)
        sub(_wrap_handler(handler))
        await sub.start()
        self._core.subscriptions.append(sub)

    def _build_fs_broker(self) -> Any:
        _quiet_underlying_loggers()
        from faststream.kafka import KafkaBroker

        # ``KafkaBroker`` wants raw bootstrap servers, not a URL.
        servers = self._core.url.removeprefix("kafka://") or "localhost:9092"
        return KafkaBroker(servers, logger=None)


class FastStreamNatsBroker(_FastStreamSchemeMixin):
    """NATS broker — competing-consumer via queue groups.

    ``prefetch`` is forwarded to ``pending_msgs_limit`` — semantically
    an in-flight backpressure cap, *not* a per-poll batch size. Set it
    to bound per-subscriber buffer growth; do not expect per-message
    fairness. ``consumer_id`` is a no-op: NATS identifies consumers
    by queue group membership, not by operator-supplied name.
    """

    def __init__(self, *, url: str, _fs_broker: Any | None = None) -> None:
        self._core = _FastStreamCore(
            scheme="nats",
            url=url,
            fs_broker=_fs_broker,
            injected=_fs_broker is not None,
        )

    async def start(self) -> None:
        await self._core.start(self._build_fs_broker)

    async def stop(self) -> None:
        await self._core.stop()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamNatsBroker.publish called before start()")
        await self._core.fs_broker.publish(payload, topic)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,
        prefetch: int | None = None,
        consumer_id: str | None = None,  # noqa: ARG002 — see class docstring
    ) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamNatsBroker.subscribe called before start()")
        kwargs: dict[str, Any] = {}
        if group is not None:
            kwargs["queue"] = group
        if prefetch is not None:
            kwargs["pending_msgs_limit"] = prefetch
        sub = self._core.fs_broker.subscriber(topic, **kwargs)
        sub(_wrap_handler(handler))
        await sub.start()
        self._core.subscriptions.append(sub)

    def _build_fs_broker(self) -> Any:
        _quiet_underlying_loggers()
        from faststream.nats import NatsBroker

        return NatsBroker(self._core.url, logger=None)


class FastStreamRabbitBroker(_FastStreamSchemeMixin):
    """RabbitMQ broker — competing-consumer is the natural default.

    ``broker.subscriber(topic)`` declares a queue named ``topic`` and
    binds it to the default exchange. Multiple consumers attaching to
    the same queue compete for messages via AMQP ``basic.consume`` —
    no special ``group`` machinery needed. Both production callers that
    want broadcast (``ResultCollector`` reply topic, ``JobBackend``
    events relay) are per-runtime-id and only ever have one subscriber,
    so the queue-share-by-default behaviour is fine.

    ``prefetch`` is currently a no-op: AMQP channel QoS is set via
    ``channel.set_qos(prefetch_count=...)`` *before* consume, and the
    wrapper does not own that lifecycle hook today. ``consumer_id`` is
    a no-op — Rabbit identifies consumers at the channel level.
    """

    def __init__(self, *, url: str, _fs_broker: Any | None = None) -> None:
        self._core = _FastStreamCore(
            scheme="amqp",
            url=url,
            fs_broker=_fs_broker,
            injected=_fs_broker is not None,
        )

    async def start(self) -> None:
        await self._core.start(self._build_fs_broker)

    async def stop(self) -> None:
        await self._core.stop()

    async def publish(self, topic: str, payload: bytes) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamRabbitBroker.publish called before start()")
        await self._core.fs_broker.publish(payload, topic)

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        group: str | None = None,  # noqa: ARG002 — see class docstring
        prefetch: int | None = None,  # noqa: ARG002 — see class docstring
        consumer_id: str | None = None,  # noqa: ARG002 — see class docstring
    ) -> None:
        if not self._core.started or self._core.fs_broker is None:
            raise RuntimeError("FastStreamRabbitBroker.subscribe called before start()")
        sub = self._core.fs_broker.subscriber(topic)
        sub(_wrap_handler(handler))
        await sub.start()
        self._core.subscriptions.append(sub)

    def _build_fs_broker(self) -> Any:
        _quiet_underlying_loggers()
        from faststream.rabbit import RabbitBroker

        return RabbitBroker(self._core.url, logger=None)


# ---------------------------------------------------------------------------
# Factory — URL scheme to concrete class.
# ---------------------------------------------------------------------------


_SCHEME_TO_CLASS: dict[
    str,
    type[
        FastStreamRedisBroker
        | FastStreamKafkaBroker
        | FastStreamNatsBroker
        | FastStreamRabbitBroker
    ],
] = {
    "redis": FastStreamRedisBroker,
    "kafka": FastStreamKafkaBroker,
    "nats": FastStreamNatsBroker,
    "amqp": FastStreamRabbitBroker,
}


def FastStreamBroker(  # noqa: N802 — callable type-name for back-compat
    *,
    scheme: str,
    url: str,
    _fs_broker: Any | None = None,
) -> FastStreamBrokerType:
    """Construct the FastStream broker concrete for ``scheme``.

    Dispatches on the URL scheme to one of the four scheme-specific
    classes (``FastStreamRedisBroker`` etc.). Returned object satisfies
    the :class:`Broker` Protocol.

    The four scheme classes are also importable directly when a caller
    wants the explicit type — most production code routes through the
    factory and stays scheme-agnostic.
    """
    if scheme not in _SCHEME_TO_CLASS:
        raise SpecValidationError(
            f"unsupported broker scheme {scheme!r}; "
            f"expected one of {sorted(_SCHEME_TO_EXTRA)}"
        )
    cls = _SCHEME_TO_CLASS[scheme]
    return cls(url=url, _fs_broker=_fs_broker)


# Tuple convenient for ``isinstance`` checks where a caller wants to
# detect "is this any of our FastStream wrappers?" without listing
# four names manually. Used by :mod:`murmur.server.router`.
FastStreamBrokerTypes: tuple[type, ...] = (
    FastStreamRedisBroker,
    FastStreamKafkaBroker,
    FastStreamNatsBroker,
    FastStreamRabbitBroker,
)

# Union type alias for annotations that span every FastStream concrete.
# ``FastStreamBroker`` itself is the construction *factory* (a function),
# not a type, so it can't be used as an annotation; use this alias when
# a test or API wants "any of the four".
FastStreamBrokerType: TypeAlias = (
    FastStreamRedisBroker
    | FastStreamKafkaBroker
    | FastStreamNatsBroker
    | FastStreamRabbitBroker
)


# ---------------------------------------------------------------------------
# Module-level helpers (unchanged across the refactor).
# ---------------------------------------------------------------------------


_QUIETED_LOGGERS = False


def _quiet_underlying_loggers() -> None:
    """Raise broker-lib loggers to WARNING so successful runs are silent.

    ``aiokafka`` etc. log DNS / connection / consumer-group lifecycle at
    INFO by default — useful for debugging, noisy in routine startup.
    Errors (WARNING+) still surface. Idempotent: only adjusts levels
    once per process so user-overridden levels don't get fought over.
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

    FastStream's signature-driven dispatcher will parse a JSON-shaped
    body into a dict if the annotation is ``bytes`` (it sees the leading
    ``{`` and auto-decodes), then fail Pydantic validation against
    ``bytes``. We side-step that by pulling the raw frame via
    ``Context("message.body")`` — this stays bytes regardless of payload
    shape and keeps our :class:`Broker` Protocol format-agnostic.
    """
    from faststream import Context

    async def _on_message(body: bytes = Context("message.body")) -> None:
        await handler(body)

    return _on_message


__all__ = [
    "FastStreamBroker",
    "FastStreamBrokerType",
    "FastStreamBrokerTypes",
    "FastStreamKafkaBroker",
    "FastStreamNatsBroker",
    "FastStreamRabbitBroker",
    "FastStreamRedisBroker",
]
