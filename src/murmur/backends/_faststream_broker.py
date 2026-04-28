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
        await self._fs_broker.publish(payload, topic)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        if not self._started or self._fs_broker is None:
            raise RuntimeError("FastStreamBroker.subscribe called before start()")
        sub = self._fs_broker.subscriber(topic)
        sub(_wrap_handler(handler))
        await sub.start()
        self._subscriptions.append(sub)

    # ------------------------------------------------------------------ helpers

    def _build_fs_broker(self) -> Any:
        """Lazy-import the right FastStream broker class for ``self._scheme``."""
        if self._scheme == "kafka":
            from faststream.kafka import KafkaBroker

            # ``KafkaBroker`` wants raw bootstrap servers, not a URL.
            servers = self._url.removeprefix("kafka://") or "localhost:9092"
            return KafkaBroker(servers)
        if self._scheme == "nats":
            from faststream.nats import NatsBroker

            return NatsBroker(self._url)
        if self._scheme == "amqp":
            from faststream.rabbit import RabbitBroker

            return RabbitBroker(self._url)
        if self._scheme == "redis":
            from faststream.redis import RedisBroker

            return RedisBroker(self._url)
        raise SpecValidationError(  # pragma: no cover — caught in __init__
            f"no FastStream broker class wired for scheme {self._scheme!r}"
        )


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
