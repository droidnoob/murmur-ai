"""HMAC-signed broker envelopes — ``broker_signing_key`` round-trips.

Exercises the opt-in authenticated-envelope layer end-to-end through an
in-memory broker:

* signing helpers (``sign_task_message`` / ``verify_task_message``) on
  their own — pure unit tests for the canonical payload + HMAC contract,
* publisher/worker round-trips with both sides keyed — the success path,
* tampered ``parent_spawn`` and ``agent_name`` — must be rejected,
* missing signature when worker requires one — must be rejected,
* unsigned mode — backwards-compatible default,
* key rotation — worker accepts a tuple, verifies against any.

Failed verification publishes a structured failure :class:`ResultMessage`
to ``msg.reply_to``; the publisher's ``await runtime.run(...)`` resolves
with ``result.error`` set to ``"signature verification failed"`` and
the worker side never dispatches the agent.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.messages import (
    ParentSpawn,
    TaskMessage,
    sign_task_message,
    signing_payload,
    task_topic,
    verify_task_message,
)
from murmur.runtime import AgentRuntime, RuntimeOptions
from murmur.types import TaskSpec, TrustLevel
from murmur.worker.worker import Worker

# ---------------------------------------------------------------------------
# Test fixtures — TestModel-backed worker so no LLM call is made.
# ---------------------------------------------------------------------------


class _Out(BaseModel):
    text: str


async def _stub_pa_agent(
    agent: Agent,
    _allowed: frozenset[str],
    _task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_worker_runtime() -> AgentRuntime:
    backend = AsyncBackend()
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(backend=backend)


@pytest.fixture
def echo_agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel injected
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# Pure unit tests on the signing helpers.
# ---------------------------------------------------------------------------


def test_signing_payload_is_deterministic() -> None:
    """Canonical bytes are stable regardless of insertion order — the
    contract any reimplementation must match."""
    a = signing_payload(
        agent_name="x",
        request_id="r",
        parent_spawn=ParentSpawn(
            agent_name="p", trace_id="t", depth=1, ancestors=frozenset({"a", "b"})
        ),
    )
    b = signing_payload(
        agent_name="x",
        request_id="r",
        parent_spawn=ParentSpawn(
            agent_name="p", trace_id="t", depth=1, ancestors=frozenset({"b", "a"})
        ),
    )
    assert a == b


def test_signing_payload_excludes_unsigned_fields() -> None:
    """``events_topic`` / ``reply_to`` / ``task`` are publisher-controlled
    routing — not part of the signature. Two messages that differ only on
    those fields produce the same canonical payload."""
    base = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="reply-1",
        request_id="req-1",
        task=TaskSpec(input="alpha"),
    )
    other = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="reply-2",  # different
        request_id="req-1",
        task=TaskSpec(input="beta"),  # different
        events_topic="events-x",  # different
    )
    a = signing_payload(
        agent_name="x", request_id=base.request_id, parent_spawn=base.parent_spawn
    )
    b = signing_payload(
        agent_name="x", request_id=other.request_id, parent_spawn=other.parent_spawn
    )
    assert a == b


def test_sign_then_verify_roundtrip() -> None:
    key = b"k" * 32
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="r",
        request_id="req-1",
        task=TaskSpec(input="hi"),
    )
    signed = sign_task_message(msg, agent_name="agent", key=key)
    assert signed.signature is not None and len(signed.signature) == 64  # sha256 hex
    assert verify_task_message(signed, agent_name="agent", keys=(key,))


def test_verify_rejects_wrong_agent_name() -> None:
    """``agent_name`` is part of the signed payload — routing the same
    envelope at a different worker subscription must fail verification."""
    key = b"k" * 32
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="r",
        request_id="req-1",
        task=TaskSpec(input="hi"),
    )
    signed = sign_task_message(msg, agent_name="echo", key=key)
    assert not verify_task_message(signed, agent_name="other", keys=(key,))


def test_verify_rejects_unsigned_message() -> None:
    """No signature attached → False (no crash)."""
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="r",
        request_id="req-1",
        task=TaskSpec(input="hi"),
    )
    assert msg.signature is None
    assert not verify_task_message(msg, agent_name="echo", keys=(b"k" * 32,))


def test_verify_rejects_empty_keys() -> None:
    """Defensive: empty key tuple rejects rather than silently passing."""
    key = b"k" * 32
    signed = sign_task_message(
        TaskMessage(
            batch_id="b",
            task_id="b-0",
            reply_to="r",
            request_id="req-1",
            task=TaskSpec(input="hi"),
        ),
        agent_name="echo",
        key=key,
    )
    assert not verify_task_message(signed, agent_name="echo", keys=())


def test_verify_accepts_any_key_in_rotation_tuple() -> None:
    """Worker rotation: stamp with ``(new, old)``, accept either."""
    old, new = b"o" * 32, b"n" * 32
    signed_old = sign_task_message(
        TaskMessage(
            batch_id="b",
            task_id="b-0",
            reply_to="r",
            request_id="req-1",
            task=TaskSpec(input="x"),
        ),
        agent_name="a",
        key=old,
    )
    assert verify_task_message(signed_old, agent_name="a", keys=(new, old))
    assert verify_task_message(signed_old, agent_name="a", keys=(old, new))


# ---------------------------------------------------------------------------
# End-to-end through the in-memory broker.
# ---------------------------------------------------------------------------


@pytest.fixture
async def signed_wired(
    echo_agent: Agent,
) -> AsyncIterator[tuple[AgentRuntime, Worker, bytes]]:
    """Publisher + Worker keyed with the same secret. Round-trip should
    succeed and behave identically to the unsigned path."""
    key = b"shared-secret-key-thirty-two-byt"  # 32 bytes
    broker = InMemoryBroker()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-signed",
        options=RuntimeOptions(broker_signing_key=key),
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
        concurrency=4,
        signing_key=key,
    )
    await worker.start()
    try:
        yield publisher, worker, key
    finally:
        await worker.stop()


async def test_signed_round_trip_succeeds(
    signed_wired: tuple[AgentRuntime, Worker, bytes],
    echo_agent: Agent,
) -> None:
    """Both sides keyed with the same secret — happy path. Result lands
    typed, no errors, signature was validated mid-flight."""
    publisher, _, _ = signed_wired
    result = await publisher.run(echo_agent, TaskSpec(input="hi"))
    assert result.is_ok()
    assert isinstance(result.output, _Out)


async def test_signed_gather_round_trip_succeeds(
    signed_wired: tuple[AgentRuntime, Worker, bytes],
    echo_agent: Agent,
) -> None:
    """``gather`` signs every slot in the batch."""
    publisher, _, _ = signed_wired
    results = await publisher.gather(
        echo_agent, [TaskSpec(input=f"q-{i}") for i in range(3)]
    )
    assert len(results) == 3
    assert all(r.is_ok() for r in results)


async def test_unsigned_mode_unchanged(
    echo_agent: Agent,
) -> None:
    """Default ``broker_signing_key=None`` — nothing signed, nothing
    verified. Pre-signing wire format is preserved."""
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-unsigned")
    assert publisher.options.broker_signing_key is None
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
    )
    await worker.start()
    try:
        result = await publisher.run(echo_agent, TaskSpec(input="hi"))
        assert result.is_ok()
    finally:
        await worker.stop()


async def test_missing_signature_rejected_when_key_required(
    echo_agent: Agent,
) -> None:
    """Worker keyed, publisher unkeyed → every message arrives unsigned
    and is rejected. The publisher's ``run()`` resolves with
    ``result.error`` set; the worker NEVER dispatches the agent."""
    key = b"k" * 32
    broker = InMemoryBroker()
    publisher = AgentRuntime(broker_instance=broker, runtime_id="rt-mismatch")
    # Sentinel: was the worker's runtime asked to run anything?
    worker_rt = _make_worker_runtime()
    dispatched: list[str] = []
    real_run = worker_rt.run

    async def tracking_run(*args: Any, **kwargs: Any) -> Any:
        dispatched.append("yes")
        return await real_run(*args, **kwargs)

    worker_rt.run = tracking_run  # ty: ignore[invalid-assignment]  # test seam

    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=worker_rt,
        signing_key=key,
    )
    await worker.start()
    try:
        result = await publisher.run(echo_agent, TaskSpec(input="hi"))
    finally:
        await worker.stop()

    assert not result.is_ok()
    assert result.error is not None
    assert "signature verification failed" in str(result.error)
    assert dispatched == []  # worker side did NOT execute the agent


async def test_tampered_parent_spawn_rejected(
    echo_agent: Agent,
) -> None:
    """Republish a signed envelope with ``parent_spawn.depth`` mutated
    after signing. Worker rejects (signature no longer matches), and
    never dispatches the agent."""
    key = b"k" * 32
    broker = InMemoryBroker()
    worker_rt = _make_worker_runtime()
    dispatched: list[str] = []
    real_run = worker_rt.run

    async def tracking_run(*args: Any, **kwargs: Any) -> Any:
        dispatched.append("yes")
        return await real_run(*args, **kwargs)

    worker_rt.run = tracking_run  # ty: ignore[invalid-assignment]  # test seam

    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=worker_rt,
        signing_key=key,
    )
    await worker.start()

    # Subscribe to the result topic so we can synchronously observe the
    # rejection ResultMessage.
    received: list[bytes] = []

    async def capture(payload: bytes) -> None:
        received.append(payload)

    reply_topic = "murmur.test.reply"
    await broker.subscribe(reply_topic, capture)

    # Build a fully-signed message...
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to=reply_topic,
        request_id="req-tamper",
        task=TaskSpec(input="hi"),
        parent_spawn=ParentSpawn(
            agent_name="parent",
            trace_id="t-parent",
            depth=2,
            ancestors=frozenset({"g"}),
        ),
    )
    signed = sign_task_message(msg, agent_name=echo_agent.name, key=key)
    # ...then mutate ``parent_spawn.depth`` after signing.
    assert signed.parent_spawn is not None  # ty narrowing
    tampered = signed.model_copy(
        update={
            "parent_spawn": signed.parent_spawn.model_copy(update={"depth": 0}),
        }
    )

    # Publish tampered payload directly onto the agent's task topic.
    import asyncio

    await broker.publish(
        task_topic(echo_agent.name), tampered.model_dump_json().encode()
    )
    # Drain the in-memory broker's handler tasks so the rejection
    # ResultMessage has been published before assertions run.
    for _ in range(10):
        await asyncio.sleep(0)
    try:
        # The worker should have published one rejection ResultMessage.
        assert len(received) == 1
        from murmur.messages import ResultMessage

        rm = ResultMessage.model_validate_json(received[0])
        assert rm.success is False
        assert rm.error_message == "signature verification failed"
        assert dispatched == []  # never dispatched
    finally:
        await worker.stop()


async def test_tampered_agent_name_rejected(
    echo_agent: Agent,
) -> None:
    """An envelope signed for one agent and routed to another agent's
    topic must be rejected. ``agent_name`` is part of the signed
    payload, so the worker's bound subscriber name catches the
    cross-routing."""
    key = b"k" * 32
    broker = InMemoryBroker()
    other_agent = Agent(
        name="other",
        model="anthropic:claude-sonnet-4-6",
        instructions="other",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )
    worker_rt = _make_worker_runtime()
    dispatched: list[str] = []
    real_run = worker_rt.run

    async def tracking_run(*args: Any, **kwargs: Any) -> Any:
        dispatched.append("yes")
        return await real_run(*args, **kwargs)

    worker_rt.run = tracking_run  # ty: ignore[invalid-assignment]  # test seam

    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent, other_agent.name: other_agent},
        runtime=worker_rt,
        signing_key=key,
    )
    await worker.start()

    received: list[bytes] = []

    async def capture(payload: bytes) -> None:
        received.append(payload)

    reply_topic = "murmur.test.reply.cross"
    await broker.subscribe(reply_topic, capture)

    # Sign for ``echo`` but publish onto ``other``'s topic.
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to=reply_topic,
        request_id="req-cross",
        task=TaskSpec(input="hi"),
    )
    signed_for_echo = sign_task_message(msg, agent_name="echo", key=key)

    import asyncio

    await broker.publish(
        task_topic("other"), signed_for_echo.model_dump_json().encode()
    )
    for _ in range(10):
        await asyncio.sleep(0)

    try:
        assert len(received) == 1
        from murmur.messages import ResultMessage

        rm = ResultMessage.model_validate_json(received[0])
        assert rm.success is False
        assert rm.error_message == "signature verification failed"
        assert dispatched == []
    finally:
        await worker.stop()


async def test_key_rotation_old_publisher_new_worker(
    echo_agent: Agent,
) -> None:
    """Worker accepts a rotation tuple ``(new, old)``; publisher still
    uses ``old`` mid-rollout. Round-trip succeeds."""
    old = b"o" * 32
    new = b"n" * 32
    broker = InMemoryBroker()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-rot",
        options=RuntimeOptions(broker_signing_key=old),
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
        signing_key=(new, old),
    )
    await worker.start()
    try:
        result = await publisher.run(echo_agent, TaskSpec(input="hi"))
        assert result.is_ok()
    finally:
        await worker.stop()


async def test_key_rotation_new_publisher_during_rollout(
    echo_agent: Agent,
) -> None:
    """Once the publisher rolls forward to ``new``, workers still
    keyed with ``(new, old)`` keep accepting."""
    old = b"o" * 32
    new = b"n" * 32
    broker = InMemoryBroker()
    publisher = AgentRuntime(
        broker_instance=broker,
        runtime_id="rt-rot-2",
        options=RuntimeOptions(broker_signing_key=new),
    )
    worker = Worker(
        broker=broker,
        agents={echo_agent.name: echo_agent},
        runtime=_make_worker_runtime(),
        signing_key=(new, old),
    )
    await worker.start()
    try:
        result = await publisher.run(echo_agent, TaskSpec(input="hi"))
        assert result.is_ok()
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Worker constructor — input validation on signing_key.
# ---------------------------------------------------------------------------


def test_worker_rejects_empty_signing_key_bytes(
    echo_agent: Agent,
) -> None:
    """Defensive: ``signing_key=b""`` is a config error, not a no-op."""
    from murmur.core.errors import SpecValidationError

    broker = InMemoryBroker()
    with pytest.raises(SpecValidationError, match="non-empty"):
        Worker(
            broker=broker,
            agents={echo_agent.name: echo_agent},
            runtime=_make_worker_runtime(),
            signing_key=b"",
        )


def test_worker_rejects_empty_signing_key_tuple(
    echo_agent: Agent,
) -> None:
    """Defensive: ``signing_key=()`` is a config error, not "verify
    against nothing → always reject"."""
    from murmur.core.errors import SpecValidationError

    broker = InMemoryBroker()
    with pytest.raises(SpecValidationError, match="at least one"):
        Worker(
            broker=broker,
            agents={echo_agent.name: echo_agent},
            runtime=_make_worker_runtime(),
            signing_key=(),
        )
