"""Wire-format messages and topic naming for distributed dispatch.

Used by :class:`murmur.backends.JobBackend` (publishes ``TaskMessage``,
collects ``ResultMessage``) and the broker-side :class:`murmur.worker.Worker`
(consumes ``TaskMessage``, publishes ``ResultMessage``). Centralized here so
both sides agree on the envelope.

``ResultMessage`` carries the agent's output as a dict (``output_payload``)
rather than a nested ``AgentResult[BaseModel]``: a generic ``BaseModel``
field is not Pydantic-deserialisable (you cannot instantiate the abstract
base class), so the publisher-side :class:`JobBackend` re-validates the
payload against the agent's known ``output_type`` and reconstructs the
typed :class:`murmur.types.AgentResult` locally.

``request_id`` propagates end-to-end so logs, HTTP requests, and broker
traffic correlate cleanly.

Optional :attr:`TaskMessage.signature` carries a stdlib-only HMAC-SHA256
hex digest over the safety-relevant fields (``agent_name``,
``request_id``, ``parent_spawn``). Off by default — see
:func:`sign_task_message` and :func:`verify_task_message` plus
:attr:`murmur.runtime.RuntimeOptions.broker_signing_key`.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from murmur.types import TaskSpec


class ParentSpawn(BaseModel):
    """Cascading-spawn parent metadata serialised onto a :class:`TaskMessage`.

    When a sub-spawn crosses a broker boundary (publisher in process X,
    worker in process Y), the worker has no in-memory access to process
    X's spawn frame. This snapshot carries just enough state for the
    worker to reconstruct the parent frame and continue cycle / depth /
    ``parent_trace_id`` enforcement on its side.

    Conversation history (``AgentContext.messages``) is intentionally
    omitted — it's application data, not orchestration metadata, and it's
    the context-passer's job to assemble it on each side.
    """

    model_config = ConfigDict(frozen=True)

    agent_name: str
    """Name of the parent agent (the one that issued this sub-spawn)."""

    trace_id: str
    """``trace_id`` of the parent run — child events attribute back via this."""

    depth: int
    """Depth of the parent's ``AgentContext`` (the child becomes ``depth + 1``)."""

    ancestors: frozenset[str] = Field(default_factory=frozenset)
    """Ancestor names *of the parent itself* (i.e. excluding the parent's own
    name). The worker derives the child's full ancestor set as
    ``ancestors | {agent_name}``."""


class TaskMessage(BaseModel):
    """Envelope published onto a task topic; consumed by a worker."""

    model_config = ConfigDict(frozen=True)

    batch_id: str
    """Identifies the gather() invocation this task belongs to."""

    task_id: str
    """Unique within ``batch_id``. Conventionally ``"{batch_id}-{index}"``."""

    reply_to: str
    """Topic the worker should publish the matching ``ResultMessage`` onto."""

    request_id: str
    """Correlates this task with the originating client request."""

    task: TaskSpec
    """The actual unit of work."""

    events_topic: str | None = None
    """Optional topic the worker should relay :class:`RuntimeEvent`
    envelopes onto for the duration of this task.

    Set by the publisher-side :class:`JobBackend` when constructed with
    ``publish_events=True`` so per-agent / per-tool events fire on the
    publisher's local emitter alongside the worker's. Defaults to
    ``None`` — log-aggregation pipelines (Datadog/Loki) cover the common
    case without doubling broker load."""

    parent_spawn: ParentSpawn | None = None
    """Cascading-spawn parent metadata, when this task is a sub-spawn.
    ``None`` for top-level dispatches. The worker uses this to rebuild
    the parent frame so cycle / depth / ``parent_trace_id`` enforcement
    survives the broker hop."""

    signature: str | None = None
    """Optional HMAC-SHA256 hex digest authenticating the safety-relevant
    fields of this envelope. ``None`` (default) means "no signature was
    attached" — workers run unverified, matching the documented "broker
    is trusted" baseline.

    When the publisher's :class:`murmur.runtime.RuntimeOptions` carries
    ``broker_signing_key``, :class:`murmur.backends.JobBackend` calls
    :func:`sign_task_message` to compute the digest before
    serialisation and stamps it here. A worker constructed with the
    matching key calls :func:`verify_task_message` on receipt and
    rejects mismatched / missing signatures with a structured failure
    :class:`ResultMessage` (no exception escapes the handler — broken
    envelopes don't take the worker down).

    Recommended key length is **at least 32 random bytes**
    (e.g. ``secrets.token_bytes(32)``); pass them as raw ``bytes`` —
    no key derivation layer. The worker accepts a sequence of keys
    for rotation; the publisher signs with one. See
    :func:`signing_payload` for the exact canonical format."""


class ResultMessage(BaseModel):
    """Envelope published onto a reply topic; consumed by the runtime.

    The payload is serialised as a primitive dict + flag, *not* a
    parametrised :class:`AgentResult`. The publisher rehydrates against
    the agent's ``output_type`` to reconstruct the typed envelope.
    """

    model_config = ConfigDict(frozen=True)

    batch_id: str
    task_id: str
    request_id: str

    success: bool
    """``True`` iff the worker produced a typed output (``output_payload``)."""

    output_payload: Mapping[str, Any] | None = None
    """``model_dump()`` of the typed output. ``None`` when ``success`` is False."""

    error_message: str | None = None
    """Stringified error. ``None`` when ``success`` is True."""

    duration_ms: int = 0
    tokens_used: int = 0
    backend: str = ""
    agent_name: str = ""

    metadata_extras: Mapping[str, Any] = Field(default_factory=dict)
    """Future-proof: extra metadata fields for forward compatibility."""


def signing_payload(
    *,
    agent_name: str,
    request_id: str,
    parent_spawn: ParentSpawn | None,
) -> bytes:
    """Canonical bytes that ``signature`` covers.

    Deterministic JSON of a flat dict with three keys::

        {"agent_name": "...", "parent_spawn": {...} | null, "request_id": "..."}

    Encoded via ``json.dumps(..., sort_keys=True, separators=(",", ":"))``
    so the same logical envelope always serialises to identical bytes
    regardless of insertion order — required for HMAC determinism.

    ``parent_spawn`` is rendered with its keys also sorted; ``ancestors``
    is materialised as a sorted list (frozensets are not JSON-native,
    and sorting both ends matches every ordering). The result is UTF-8
    encoded for :func:`hmac.new`.

    The exact format is part of the wire contract — any reimplementation
    on a worker fleet that doesn't share this codebase must match
    byte-for-byte. The publisher choosing ``events_topic`` / ``reply_to``
    is intentionally NOT signed: those are publisher-controlled routing
    that the worker cannot validate, and signing them would conflate
    routing with authenticity.
    """
    if parent_spawn is None:
        parent_blob: dict[str, Any] | None = None
    else:
        parent_blob = {
            "agent_name": parent_spawn.agent_name,
            "ancestors": sorted(parent_spawn.ancestors),
            "depth": parent_spawn.depth,
            "trace_id": parent_spawn.trace_id,
        }
    payload = {
        "agent_name": agent_name,
        "parent_spawn": parent_blob,
        "request_id": request_id,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_task_message(
    msg: TaskMessage,
    *,
    agent_name: str,
    key: bytes,
) -> TaskMessage:
    """Return a copy of ``msg`` with :attr:`TaskMessage.signature` set.

    ``agent_name`` is signed separately from the envelope (it is not a
    field on :class:`TaskMessage`). The publisher passes the agent it's
    routing to; the worker's bound handler validates against the same
    name, so a forged envelope routed to the wrong topic still fails.

    HMAC algorithm: ``hmac.new(key, msg=payload, digestmod=hashlib.sha256).hexdigest()``
    where ``payload`` comes from :func:`signing_payload`.
    """
    digest = hmac.new(
        key,
        msg=signing_payload(
            agent_name=agent_name,
            request_id=msg.request_id,
            parent_spawn=msg.parent_spawn,
        ),
        digestmod=hashlib.sha256,
    ).hexdigest()
    return msg.model_copy(update={"signature": digest})


def verify_task_message(
    msg: TaskMessage,
    *,
    agent_name: str,
    keys: Sequence[bytes],
) -> bool:
    """``True`` iff ``msg.signature`` matches the digest under any key.

    Constant-time compare via :func:`hmac.compare_digest` so the worker
    can't be timed into discovering which key matched. ``keys`` is a
    sequence so deployments can roll keys without downtime: stamp new
    workers with ``(new, old)``, swap publishers to ``new``, then drop
    ``old`` once the queue has drained.

    Returns ``False`` when ``msg.signature`` is ``None`` or no key
    matches. Caller decides what to do with the rejection — this helper
    never raises on a bad signature (callers want to publish a structured
    failure, not crash).
    """
    if msg.signature is None or not keys:
        return False
    payload = signing_payload(
        agent_name=agent_name,
        request_id=msg.request_id,
        parent_spawn=msg.parent_spawn,
    )
    expected = msg.signature
    for key in keys:
        candidate = hmac.new(key, msg=payload, digestmod=hashlib.sha256).hexdigest()
        if hmac.compare_digest(candidate, expected):
            return True
    return False


def task_topic(agent_name: str) -> str:
    """Topic where ``TaskMessage`` envelopes for ``agent_name`` are published."""
    return f"murmur.{agent_name}.tasks"


def result_topic(runtime_id: str) -> str:
    """Reply topic for a single runtime instance.

    Each runtime gets its own unique reply topic so concurrent runtimes never
    cross-contaminate result traffic.
    """
    return f"murmur.results.{runtime_id}"


def events_topic(runtime_id: str) -> str:
    """Topic the distributed event bridge publishes :class:`RuntimeEvent`
    envelopes onto for one publisher runtime.

    Per-publisher namespacing matches :func:`result_topic` — concurrent
    runtimes never cross-contaminate event traffic. The publisher
    subscribes here on :meth:`JobBackend.start` (when
    ``publish_events=True``) and forwards each decoded event to its
    local emitter; workers publish here when a task's
    :attr:`TaskMessage.events_topic` instructs them to.
    """
    return f"murmur.events.{runtime_id}"


__all__ = [
    "ParentSpawn",
    "ResultMessage",
    "TaskMessage",
    "events_topic",
    "result_topic",
    "sign_task_message",
    "signing_payload",
    "task_topic",
    "verify_task_message",
]
