"""Unit tests for ``murmur.types``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from murmur.types import (
    AgentContext,
    AgentHandle,
    AgentResult,
    ResultMetadata,
    TaskSpec,
    TrustLevel,
)


class _Out(BaseModel):
    value: int


def test_taskspec_is_frozen() -> None:
    task = TaskSpec(input="x")
    with pytest.raises(ValidationError):
        task.input = "y"


def test_taskspec_assigns_id_when_missing() -> None:
    a = TaskSpec(input="x")
    b = TaskSpec(input="x")
    assert a.id != b.id


def test_trust_level_is_str_enum() -> None:
    assert TrustLevel.HIGH == "high"
    assert TrustLevel("medium") is TrustLevel.MEDIUM


def test_agent_result_is_ok_when_output_present() -> None:
    res = AgentResult[_Out](
        output=_Out(value=1),
        agent_name="a",
        task_id="t",
        metadata=ResultMetadata(),
    )
    assert res.is_ok()


def test_agent_result_is_not_ok_when_error_present() -> None:
    res = AgentResult[_Out](
        output=None,
        error=RuntimeError("boom"),
        agent_name="a",
        task_id="t",
        metadata=ResultMetadata(),
    )
    assert not res.is_ok()


def test_agent_handle_carries_backend_name() -> None:
    h = AgentHandle(agent_name="a", task_id="t", backend="thread")
    assert h.backend == "thread"


def test_agent_context_default_is_empty() -> None:
    ctx = AgentContext()
    assert ctx.messages == ()
    assert ctx.depth == 0
    assert ctx.parent_agent is None


def test_taskspec_assigns_request_id_when_missing() -> None:
    a = TaskSpec(input="x")
    b = TaskSpec(input="x")
    assert a.request_id != b.request_id


def test_taskspec_keeps_supplied_request_id() -> None:
    task = TaskSpec(input="x", request_id="req-abc-123")
    assert task.request_id == "req-abc-123"
