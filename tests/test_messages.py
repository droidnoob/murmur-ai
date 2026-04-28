"""Unit tests for ``murmur.messages``."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from murmur.messages import ResultMessage, TaskMessage, result_topic, task_topic
from murmur.types import TaskSpec


def test_task_topic_format() -> None:
    assert task_topic("research-minion") == "murmur.research-minion.tasks"


def test_result_topic_format() -> None:
    assert result_topic("rt-abc") == "murmur.results.rt-abc"


def test_task_message_is_frozen() -> None:
    msg = TaskMessage(
        batch_id="b",
        task_id="b-0",
        reply_to="murmur.results.rt",
        request_id="req",
        task=TaskSpec(input="x"),
    )
    with pytest.raises(ValidationError):
        msg.batch_id = "other"


def test_result_message_success_path() -> None:
    msg = ResultMessage(
        batch_id="b",
        task_id="b-0",
        request_id="req",
        success=True,
        output_payload={"text": "hi"},
        error_message=None,
        duration_ms=42,
        tokens_used=10,
        backend="thread",
        agent_name="echo",
    )
    assert msg.success
    assert msg.output_payload == {"text": "hi"}


def test_result_message_failure_path() -> None:
    msg = ResultMessage(
        batch_id="b",
        task_id="b-0",
        request_id="req",
        success=False,
        output_payload=None,
        error_message="boom",
    )
    assert not msg.success
    assert msg.error_message == "boom"
    assert msg.output_payload is None


def test_result_message_round_trips_through_json() -> None:
    msg = ResultMessage(
        batch_id="b",
        task_id="b-0",
        request_id="req",
        success=True,
        output_payload={"text": "hi", "n": 3},
        agent_name="echo",
    )
    raw = msg.model_dump_json().encode()
    restored = ResultMessage.model_validate_json(raw)
    assert restored == msg
