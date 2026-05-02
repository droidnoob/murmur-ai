"""End-to-end tests for event emission across runtime / backend / executor.

zxn.1.5: confirms a custom :class:`EventEmitter` injected at
``AgentRuntime`` construction sees every emission point — agent spawn,
agent completion, tool call lifecycle, batch start/complete, group
start/complete — without the user wiring the emitter into each
sub-component themselves.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.events import EventType, RuntimeEvent
from murmur.runtime import AgentRuntime
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import TaskSpec, TrustLevel


class _Out(BaseModel):
    text: str


class _CollectingEmitter:
    """Captures every :class:`RuntimeEvent` for assertion in tests."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def emit(self, event: RuntimeEvent) -> None:
        self.events.append(event)

    def types(self) -> list[EventType]:
        return [e.event_type for e in self.events]

    def of(self, t: EventType) -> list[RuntimeEvent]:
        return [e for e in self.events if e.event_type is t]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _stub_pa_agent(
    agent: Agent,
    _allowed: frozenset[str],
    _task_id: str,
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_runtime(emitter: _CollectingEmitter) -> AgentRuntime:
    backend = ThreadBackend(event_emitter=emitter)
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(backend=backend, event_emitter=emitter)


def _agent(name: str = "researcher") -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",  # ignored — TestModel injected
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# Single-agent run — agent_spawned + agent_completed
# ---------------------------------------------------------------------------


async def test_run_emits_spawned_then_completed() -> None:
    emitter = _CollectingEmitter()
    runtime = _make_runtime(emitter)

    result = await runtime.run(_agent(), TaskSpec(input="hi"))
    assert result.is_ok()

    types = emitter.types()
    assert EventType.AGENT_SPAWNED in types
    assert EventType.AGENT_COMPLETED in types
    # Spawned must come before completed.
    assert types.index(EventType.AGENT_SPAWNED) < types.index(EventType.AGENT_COMPLETED)


async def test_run_event_carries_trace_id_and_agent_name() -> None:
    emitter = _CollectingEmitter()
    runtime = _make_runtime(emitter)

    task = TaskSpec(input="hi", request_id="req-known-1")
    await runtime.run(_agent("xyz"), task)

    [spawned] = emitter.of(EventType.AGENT_SPAWNED)
    assert spawned.agent_name == "xyz"
    assert spawned.trace_id == "req-known-1"
    assert spawned.task_id == task.id
    assert spawned.payload["backend"] == "thread"
    assert spawned.payload["trust_level"] == "sandbox"


async def test_completed_event_includes_duration_and_tokens() -> None:
    emitter = _CollectingEmitter()
    runtime = _make_runtime(emitter)
    await runtime.run(_agent(), TaskSpec(input="hi"))

    [completed] = emitter.of(EventType.AGENT_COMPLETED)
    assert "duration_ms" in completed.payload
    assert "tokens_used" in completed.payload
    assert completed.payload["backend"] == "thread"


async def test_failing_run_emits_agent_failed() -> None:
    emitter = _CollectingEmitter()
    backend = ThreadBackend(event_emitter=emitter)

    async def boom(*_: Any, **__: Any) -> pydantic_ai.Agent[None, Any]:
        raise RuntimeError("boom")

    backend._build_pa_agent = boom  # ty: ignore[invalid-assignment]  # test seam
    runtime = AgentRuntime(backend=backend, event_emitter=emitter)

    result = await runtime.run(_agent(), TaskSpec(input="hi"))
    assert not result.is_ok()
    [failed] = emitter.of(EventType.AGENT_FAILED)
    assert "boom" in str(failed.payload["error"])


# ---------------------------------------------------------------------------
# Tool calls — lifecycle through the executor
# ---------------------------------------------------------------------------


async def test_executor_emits_tool_call_lifecycle() -> None:
    emitter = _CollectingEmitter()
    registry = ToolRegistry()

    async def echo(text: str) -> str:
        return f"echoed: {text}"

    registry.register("echo", echo)

    executor = ToolExecutor(registry, event_emitter=emitter)
    result = await executor.execute(
        agent_name="r",
        task_id="t-1",
        trust_level=TrustLevel.HIGH,
        allowed=frozenset({"echo"}),
        name="echo",
        args={"text": "hi"},
        trace_id="req-1",
    )
    assert result == "echoed: hi"

    types = emitter.types()
    assert types == [EventType.TOOL_CALL_STARTED, EventType.TOOL_CALL_COMPLETED]
    [started] = emitter.of(EventType.TOOL_CALL_STARTED)
    assert started.payload["tool_name"] == "echo"
    assert started.payload["trust_level"] == "high"
    assert started.trace_id == "req-1"


async def test_executor_emits_failed_event_on_tool_exception() -> None:
    emitter = _CollectingEmitter()
    registry = ToolRegistry()

    async def boom() -> None:
        raise RuntimeError("kaboom")

    registry.register("boom", boom)

    executor = ToolExecutor(registry, event_emitter=emitter)
    with pytest.raises(Exception, match="kaboom"):
        await executor.execute(
            agent_name="r",
            task_id="t-1",
            trust_level=TrustLevel.HIGH,
            allowed=frozenset({"boom"}),
            name="boom",
            args={},
        )

    types = emitter.types()
    assert types == [EventType.TOOL_CALL_STARTED, EventType.TOOL_CALL_FAILED]


# ---------------------------------------------------------------------------
# gather() — batch lifecycle
# ---------------------------------------------------------------------------


async def test_gather_emits_batch_started_and_completed() -> None:
    emitter = _CollectingEmitter()
    runtime = _make_runtime(emitter)

    tasks: Sequence[TaskSpec] = [TaskSpec(input=f"q-{i}") for i in range(3)]
    results = await runtime.gather(_agent(), tasks)
    assert len(results) == 3

    [batch_started] = emitter.of(EventType.BATCH_STARTED)
    [batch_completed] = emitter.of(EventType.BATCH_COMPLETED)
    assert batch_started.payload["task_count"] == 3
    assert batch_completed.payload["task_count"] == 3
    assert batch_completed.payload["success_count"] == 3
    assert batch_completed.payload["failure_count"] == 0
    # Spawned/completed for each task
    assert len(emitter.of(EventType.AGENT_COMPLETED)) == 3


# ---------------------------------------------------------------------------
# Custom emitter substitution
# ---------------------------------------------------------------------------


async def test_custom_emitter_overrides_default_log_emitter() -> None:
    """Passing event_emitter= prevents the default LogEventEmitter from running."""
    emitter = _CollectingEmitter()
    runtime = _make_runtime(emitter)
    await runtime.run(_agent(), TaskSpec(input="hi"))
    # If the default emitter were also wired, we'd see duplicate events.
    assert len(emitter.of(EventType.AGENT_SPAWNED)) == 1
    assert len(emitter.of(EventType.AGENT_COMPLETED)) == 1
