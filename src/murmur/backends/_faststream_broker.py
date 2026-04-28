"""FastStream-backed broker wrapper.

Handles ``kafka://``, ``nats://``, ``amqp://``, and ``redis://`` URLs by
constructing the matching FastStream broker class internally. The chosen
broker class is imported lazily so that users only need the relevant extra
installed (``murmur-ai[kafka]`` etc.) rather than all four.

This is the only place in the package outside :mod:`murmur.interop` that
imports from :mod:`faststream`. Per CLAUDE.md §2, FastStream is a hidden
dependency — users never see it.

**Phase 1 implementation status: stub.** Constructor + URL parsing are in
place; ``start`` / ``publish`` / ``subscribe`` raise :class:`NotImplementedError`
until the full per-broker integration lands. The :class:`InMemoryBroker`
covers the single-process ``memory://`` path and all unit tests today; real
broker dispatch lights up when integration tests under ``testcontainers``
arrive.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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

    def __init__(self, *, scheme: str, url: str) -> None:
        if scheme not in _SCHEME_TO_EXTRA:
            raise SpecValidationError(
                f"unsupported broker scheme {scheme!r}; "
                f"expected one of {sorted(_SCHEME_TO_EXTRA)}"
            )
        self._scheme = scheme
        self._url = url
        self._extra = _SCHEME_TO_EXTRA[scheme]

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def url(self) -> str:
        return self._url

    async def start(self) -> None:
        raise NotImplementedError(
            f"FastStreamBroker[{self._scheme}] dispatch is not yet implemented; "
            f"install `murmur-ai[{self._extra}]` and use the in-process "
            "`memory://` URL for now (real-broker support lands with the "
            "testcontainers integration suite)"
        )

    async def stop(self) -> None:
        # No-op while the start() path is stubbed.
        return

    async def publish(self, topic: str, payload: bytes) -> None:  # noqa: ARG002
        raise NotImplementedError(
            f"FastStreamBroker[{self._scheme}].publish — start() must succeed first"
        )

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:  # noqa: ARG002
        raise NotImplementedError(
            f"FastStreamBroker[{self._scheme}].subscribe — start() must succeed first"
        )


__all__ = ["FastStreamBroker"]
