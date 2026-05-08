"""``Worker`` — the broker-side consumer.

A ``Worker`` subscribes to broker topics for a set of agents and dispatches
incoming tasks through an internal ``AsyncBackend``-backed runtime, then
publishes the result back to the message's ``reply_to``. Lifecycle hooks
let users plug in observability without subclassing.

The worker's runtime MUST be a *local* one — give it a broker URL and
you get an infinite loop where tasks are republished instead of
dispatched. The constructor builds a default ``AgentRuntime()`` (no
broker, AsyncBackend) when none is supplied.

Satisfies :class:`murmur.core.protocols.worker.Worker` structurally.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

import structlog
import structlog.contextvars

from murmur.core.errors import SpecValidationError
from murmur.core.protocols.worker import OnComplete, OnError, OnStart
from murmur.messages import (
    ResultMessage,
    TaskMessage,
    result_topic,
    task_topic,
    verify_task_message,
)

if TYPE_CHECKING:
    from murmur.agent import Agent
    from murmur.core.protocols.broker import Broker, MessageHandler
    from murmur.runtime import AgentRuntime


log: structlog.stdlib.BoundLogger = structlog.get_logger()


class Worker:
    """Consumer for one or more registered agents."""

    def __init__(
        self,
        *,
        broker: Broker,
        agents: Mapping[str, Agent],
        runtime: AgentRuntime | None = None,
        concurrency: int = 10,
        prefetch: int = 5,
        signing_key: bytes | tuple[bytes, ...] | None = None,
    ) -> None:
        if not agents:
            raise SpecValidationError("Worker requires at least one agent")
        if concurrency < 1:
            raise SpecValidationError("concurrency must be >= 1")
        if prefetch < 1:
            raise SpecValidationError("prefetch must be >= 1")
        # Normalise signing_key to a tuple; ``None`` means "verification
        # disabled, broker is trusted" (default — unchanged behaviour).
        # A tuple supports key rotation: stamp new workers with
        # ``(new, old)``, swap publishers to ``new``, then drop ``old``
        # once the queue has drained. Validate non-empty bytes upfront
        # so a misconfiguration like ``signing_key=()`` doesn't silently
        # turn into "every signature rejected".
        self._signing_keys: tuple[bytes, ...] | None
        if signing_key is None:
            self._signing_keys = None
        elif isinstance(signing_key, bytes):
            if not signing_key:
                raise SpecValidationError("signing_key must be non-empty bytes")
            self._signing_keys = (signing_key,)
        else:
            keys = tuple(signing_key)
            if not keys:
                raise SpecValidationError(
                    "signing_key tuple must contain at least one key"
                )
            for k in keys:
                if not isinstance(k, bytes) or not k:
                    raise SpecValidationError(
                        "every key in signing_key must be non-empty bytes"
                    )
            self._signing_keys = keys

        from murmur.events.broker import BrokerEventBridge
        from murmur.events.log import LogEventEmitter
        from murmur.events.multi import MultiEventEmitter
        from murmur.runtime import AgentRuntime as _AgentRuntime

        self._broker = broker
        self._agents: dict[str, Agent] = dict(agents)
        # NB: the worker's runtime MUST be AsyncBackend-backed. Passing a
        # broker-backed runtime here re-publishes tasks → infinite loop.
        #
        # When constructing our own runtime, we wire the distributed event
        # bridge into its emitter chain so per-agent / per-tool events
        # fire BOTH locally (to structlog via LogEventEmitter) AND, when
        # a TaskMessage carries an ``events_topic``, onto the broker for
        # the publisher to relay through its own emitter. The bridge is
        # contextvar-driven — no-op when no topic is bound — so installing
        # it has zero cost when distributed observability isn't used.
        #
        # Users supplying their own runtime keep full control of their
        # emitter chain; document the workaround (wrap their emitter in
        # a Multi with BrokerEventBridge themselves) in docstrings.
        if runtime is None:
            bridge = BrokerEventBridge(broker)
            runtime = _AgentRuntime(
                event_emitter=MultiEventEmitter([LogEventEmitter(), bridge])
            )
        self._runtime: AgentRuntime = runtime
        self._concurrency = concurrency
        self._prefetch = prefetch
        self._semaphore = asyncio.Semaphore(concurrency)
        self._active: dict[str, asyncio.Task[None]] = {}

        self._on_start: OnStart | None = None
        self._on_complete: OnComplete | None = None
        self._on_error: OnError | None = None
        self._started: bool = False

    # ------------------------------------------------------------------ hooks

    def on_task_start(self, fn: OnStart) -> OnStart:
        self._on_start = fn
        return fn

    def on_task_complete(self, fn: OnComplete) -> OnComplete:
        self._on_complete = fn
        return fn

    def on_task_error(self, fn: OnError) -> OnError:
        self._on_error = fn
        return fn

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        """Begin consuming tasks. Subscribes ``broker`` to each agent's topic.

        Emits a Murmur-branded startup banner (multi-line, written to
        stderr) plus a structured ``worker_started`` event that includes
        the per-agent task topics, broker scheme + URL, and concurrency.
        FastStream's own subscriber chatter is silenced upstream in
        :class:`murmur.backends.FastStreamBroker`.
        """
        if self._started:
            return
        await self._broker.start()
        subscriptions: dict[str, str] = {}
        for agent_name in self._agents:
            topic = task_topic(agent_name)
            # ``group=topic`` puts every Worker serving this agent into one
            # competing-consumer pool — each TaskMessage is delivered to
            # exactly one Worker rather than fanned out to all of them.
            # Without the group, Redis pub-sub / per-process Kafka groups /
            # NATS without queue groups all broadcast, which triples LLM
            # cost and produces orphan results on the publisher.
            await self._broker.subscribe(
                topic, self._make_handler(agent_name), group=topic
            )
            subscriptions[agent_name] = topic
        self._started = True

        broker_repr = _broker_repr(self._broker)
        runtime_id = self._runtime.runtime_id
        _print_banner(
            broker=broker_repr,
            runtime_id=runtime_id,
            subscriptions=subscriptions,
            concurrency=self._concurrency,
        )
        await log.ainfo(
            "worker_started",
            agents=list(self._agents.keys()),
            subscriptions=subscriptions,
            results_topic=result_topic(runtime_id),
            broker=broker_repr,
            concurrency=self._concurrency,
        )

    async def stop(self) -> None:
        """Drain in-flight tasks then disconnect the broker."""
        if not self._started:
            return
        active = list(self._active.values())
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        await self._broker.stop()
        self._started = False
        await log.ainfo("worker_stopped")

    # ------------------------------------------------------------------ private

    def _make_handler(self, agent_name: str) -> MessageHandler:
        async def handler(payload: bytes) -> None:
            try:
                msg = TaskMessage.model_validate_json(payload)
            except Exception as exc:
                await log.aerror("worker_decode_failed", error=str(exc))
                return
            current = asyncio.current_task()
            if current is not None:
                self._active[msg.task_id] = current
            try:
                await self._run_one(agent_name, msg)
            finally:
                self._active.pop(msg.task_id, None)

        return handler

    async def _run_one(self, agent_name: str, msg: TaskMessage) -> None:
        from murmur.events.broker import bind_event_topic, reset_event_topic
        from murmur.runtime import _current_spawn, _SpawnFrame
        from murmur.types import AgentContext

        agent = self._agents[agent_name]
        # Authenticated envelopes (opt-in): when ``signing_key`` was
        # supplied, every inbound TaskMessage MUST carry a valid
        # signature over (agent_name, request_id, parent_spawn). Missing
        # signatures count as rejection too — otherwise an attacker just
        # omits the field. Failure publishes a structured
        # ``ResultMessage(success=False)`` to ``reply_to`` so the
        # publisher's ``await runtime.run(...)`` resolves cleanly with
        # ``result.error`` set; the agent never dispatches. Never raises
        # out of the handler — broken envelopes don't take the worker
        # down (DoS-resistance).
        if self._signing_keys is not None and not verify_task_message(
            msg, agent_name=agent_name, keys=self._signing_keys
        ):
            await log.aerror(
                "worker_signature_rejected",
                agent_name=agent_name,
                task_id=msg.task_id,
                request_id=msg.request_id,
                reason="missing" if msg.signature is None else "mismatch",
            )
            response = ResultMessage(
                batch_id=msg.batch_id,
                task_id=msg.task_id,
                request_id=msg.request_id,
                success=False,
                output_payload=None,
                error_message="signature verification failed",
                agent_name=agent_name,
            )
            await self._broker.publish(
                msg.reply_to, response.model_dump_json().encode()
            )
            return

        # A Worker is a re-entry point. With an in-memory broker the
        # publisher and consumer share an asyncio context, so any spawn
        # frame on the publisher's contextvar leaks into the worker's
        # ``runtime.run`` and mis-fires cascade detection. Reset to a
        # known value derived from the message envelope:
        #
        # - ``msg.parent_spawn is None``  → top-level dispatch; clear.
        # - ``msg.parent_spawn`` set      → a sub-spawn that crossed a
        #   broker boundary; rebuild the parent ``SpawnFrame`` so the
        #   worker-side run derives the same depth / ancestors /
        #   parent_trace_id the publisher would have.
        #
        # Trust model: ``parent_spawn`` is taken at face value. Murmur
        # treats the broker as a trusted channel between trusted
        # publishers and trusted workers — any party with broker write
        # access can already publish arbitrary tasks for arbitrary
        # agents, so forging a lower ``depth`` or empty ``ancestors`` is
        # the least of the resulting problems. Cascading-spawn controls
        # are defensive programming against runaway LLM tool loops, not
        # a security boundary against hostile producers. Deployments
        # exposing the broker to untrusted writers must layer broker
        # auth, topic ACLs, and (if the threat model warrants it) signed
        # envelopes on top — that work lives outside Murmur.
        if msg.parent_spawn is None:
            spawn_token = _current_spawn.set(None)
        else:
            parent_frame = _SpawnFrame(
                agent_name=msg.parent_spawn.agent_name,
                trace_id=msg.parent_spawn.trace_id,
                agent_context=AgentContext(
                    depth=msg.parent_spawn.depth,
                    ancestors=msg.parent_spawn.ancestors,
                ),
            )
            spawn_token = _current_spawn.set(parent_frame)
        structlog.contextvars.bind_contextvars(
            request_id=msg.request_id,
            batch_id=msg.batch_id,
            task_id=msg.task_id,
            agent_name=agent_name,
        )
        # Bind the per-task event-relay target so any BrokerEventBridge in
        # the runtime's emitter chain forwards events to the publisher's
        # events topic. No-op when ``events_topic`` is None — the bridge
        # already early-returns when the contextvar is unset.
        events_token = bind_event_topic(msg.events_topic)
        async with self._semaphore:
            start = time.perf_counter()
            response: ResultMessage
            try:
                if self._on_start is not None:
                    await self._on_start(msg.task_id, agent_name)
                result = await self._runtime.run(agent, msg.task)
                duration_ms = int((time.perf_counter() - start) * 1000)
                if self._on_complete is not None:
                    await self._on_complete(msg.task_id, agent_name, duration_ms)
                if result.is_ok() and result.output is not None:
                    response = ResultMessage(
                        batch_id=msg.batch_id,
                        task_id=msg.task_id,
                        request_id=msg.request_id,
                        success=True,
                        output_payload=result.output.model_dump(),
                        error_message=None,
                        duration_ms=result.metadata.duration_ms,
                        tokens_used=result.metadata.tokens_used,
                        backend=result.metadata.backend,
                        agent_name=agent_name,
                    )
                else:
                    response = ResultMessage(
                        batch_id=msg.batch_id,
                        task_id=msg.task_id,
                        request_id=msg.request_id,
                        success=False,
                        output_payload=None,
                        error_message=str(result.error)
                        if result.error
                        else "agent failed",
                        duration_ms=result.metadata.duration_ms,
                        tokens_used=result.metadata.tokens_used,
                        backend=result.metadata.backend,
                        agent_name=agent_name,
                    )
            except Exception as exc:
                if self._on_error is not None:
                    await self._on_error(msg.task_id, agent_name, exc)
                response = ResultMessage(
                    batch_id=msg.batch_id,
                    task_id=msg.task_id,
                    request_id=msg.request_id,
                    success=False,
                    output_payload=None,
                    error_message=str(exc),
                    agent_name=agent_name,
                )
            finally:
                structlog.contextvars.unbind_contextvars(
                    "request_id", "batch_id", "task_id", "agent_name"
                )
                reset_event_topic(events_token)
                _current_spawn.reset(spawn_token)
        await self._broker.publish(msg.reply_to, response.model_dump_json().encode())


def _broker_repr(broker: Broker) -> str:
    """Best-effort human-readable broker description for the banner.

    :class:`FastStreamBroker` exposes ``scheme`` + ``url`` so we render
    the original URL the user passed. The in-memory broker has neither;
    we render ``memory://`` to match the URL scheme that constructs it.
    """
    scheme = getattr(broker, "scheme", None)
    url = getattr(broker, "url", None)
    if scheme and url:
        return url if url.startswith(f"{scheme}://") else f"{scheme}://{url}"
    if url:
        return str(url)
    if broker.__class__.__name__ == "InMemoryBroker":
        return "memory://"
    return broker.__class__.__name__


def _print_banner(
    *,
    broker: str,
    runtime_id: str,
    subscriptions: Mapping[str, str],
    concurrency: int,
) -> None:
    """Write a multi-line Murmur banner to stderr.

    Mirrors the visual weight of the startup output other broker frameworks
    (FastStream, Celery) produce. Goes through ``sys.stderr`` directly
    rather than ``structlog`` so the structure stays multi-line / scannable
    instead of being collapsed into one log line.
    """
    import sys

    name_width = max((len(n) for n in subscriptions), default=0)
    lines = [
        "",
        "  ╭─ Murmur worker ─────────────────────────────────────────────",
        f"  │  broker      : {broker}",
        f"  │  runtime     : {runtime_id}",
        f"  │  concurrency : {concurrency}",
        f"  │  agents      : {len(subscriptions)}",
        "  │  subscriptions:",
    ]
    for agent_name, topic in subscriptions.items():
        lines.append(f"  │    · {agent_name:<{name_width}}  →  {topic}")
    lines.append("  ╰─────────────────────────────────────────────────────────────")
    lines.append("")
    sys.stderr.write("\n".join(lines))
    sys.stderr.flush()


__all__ = ["Worker"]
