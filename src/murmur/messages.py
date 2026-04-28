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
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from murmur.types import TaskSpec


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


def task_topic(agent_name: str) -> str:
    """Topic where ``TaskMessage`` envelopes for ``agent_name`` are published."""
    return f"murmur.{agent_name}.tasks"


def result_topic(runtime_id: str) -> str:
    """Reply topic for a single runtime instance.

    Each runtime gets its own unique reply topic so concurrent runtimes never
    cross-contaminate result traffic.
    """
    return f"murmur.results.{runtime_id}"


__all__ = [
    "ResultMessage",
    "TaskMessage",
    "result_topic",
    "task_topic",
]
