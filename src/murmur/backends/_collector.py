"""ResultCollector — routes broker-delivered ``ResultMessage`` to waiters.

Long-lived. One per :class:`murmur.AgentRuntime` instance. Subscribes to the
runtime-specific reply topic (``murmur.results.{runtime_id}``) and routes
incoming :class:`murmur.messages.ResultMessage` envelopes to either:

- a *batch* awaiter — :meth:`gather_batch` — for ``runtime.gather`` calls, or
- a *single-handle* waiter — :meth:`await_handle` — for ``runtime.run`` calls.

Both flows go through the same in-memory map keyed by ``batch_id``: a single
spawn is just a degenerate one-task batch. Late or orphan results are logged
and discarded.

Returns the raw :class:`ResultMessage` envelopes — the publisher-side
:class:`JobBackend` rehydrates them into typed :class:`AgentResult` values
because only the publisher knows the agent's ``output_type``.

Internal — leading underscore, never re-exported.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

from murmur.core.errors import SpawnError
from murmur.messages import ResultMessage, result_topic

if TYPE_CHECKING:
    from murmur.core.protocols.broker import Broker


log: structlog.stdlib.BoundLogger = structlog.get_logger()


@dataclass
class _BatchState:
    expected: int
    results: dict[str, ResultMessage] = field(default_factory=dict)
    done: asyncio.Event = field(default_factory=asyncio.Event)


class ResultCollector:
    """Routes ``ResultMessage`` envelopes by ``batch_id`` to awaiting callers."""

    def __init__(self, runtime_id: str, broker: Broker) -> None:
        self._runtime_id = runtime_id
        self._broker = broker
        self._pending: dict[str, _BatchState] = {}
        self._started: bool = False

    @property
    def reply_topic(self) -> str:
        return result_topic(self._runtime_id)

    async def start(self) -> None:
        if self._started:
            return
        await self._broker.subscribe(self.reply_topic, self._on_message)
        self._started = True

    def register(self, batch_id: str, expected: int) -> None:
        """Register a batch *before* publishing tasks (race-safe).

        Caller MUST invoke this synchronously prior to ``broker.publish`` so
        a result that arrives between publish and ``gather_batch`` doesn't
        get discarded as an orphan.
        """
        if batch_id in self._pending:
            raise SpawnError(f"batch_id {batch_id!r} already registered")
        self._pending[batch_id] = _BatchState(expected=expected)

    async def gather_batch(
        self,
        *,
        batch_id: str,
        timeout: float | None = None,
    ) -> list[ResultMessage | None]:
        """Await ``register``-ed batch.

        Returns one entry per expected slot, in order. ``None`` for slots
        that did not produce a result by the timeout — never raises.
        """
        state = self._pending.get(batch_id)
        if state is None:
            raise SpawnError(f"batch_id {batch_id!r} was not registered")
        try:
            if timeout is not None:
                async with asyncio.timeout(timeout):
                    await state.done.wait()
            else:
                await state.done.wait()
        except TimeoutError:
            await log.awarning(
                "batch_timeout",
                batch_id=batch_id,
                received=len(state.results),
                expected=state.expected,
            )
        finally:
            self._pending.pop(batch_id, None)

        return [state.results.get(f"{batch_id}-{i}") for i in range(state.expected)]

    async def await_handle(
        self,
        *,
        batch_id: str,
        timeout: float | None = None,
    ) -> ResultMessage | None:
        """Single-task convenience. Returns the one envelope or ``None`` on timeout."""
        slots = await self.gather_batch(batch_id=batch_id, timeout=timeout)
        if not slots:  # pragma: no cover — shape invariant
            return None
        return slots[0]

    # ------------------------------------------------------------------ private

    async def _on_message(self, payload: bytes) -> None:
        try:
            msg = ResultMessage.model_validate_json(payload)
        except Exception as exc:
            await log.aerror("collector_decode_failed", error=str(exc))
            return

        state = self._pending.get(msg.batch_id)
        if state is None:
            await log.awarning("collector_orphan_result", batch_id=msg.batch_id)
            return

        state.results[msg.task_id] = msg
        if len(state.results) >= state.expected:
            state.done.set()


__all__ = ["ResultCollector"]
