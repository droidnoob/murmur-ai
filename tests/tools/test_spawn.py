"""Tests for ``make_spawn_agents_tool`` (apm).

Builds a spawn_agents tool against a stubbed ``runtime.run`` so the tool's
own logic — child materialisation via the template, semaphore-bounded
fan-out, per-child failure capture, ordered results — gets exercised
without dragging in a real LLM.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from murmur import Agent, AgentRuntime, AgentTemplate, TrustLevel
from murmur.core.errors import SpawnError
from murmur.tools import (
    SpawnResult,
    SpawnSpec,
    make_spawn_agents_tool,
)
from murmur.tools.registry import ToolRegistry
from murmur.types import AgentResult, ResultMetadata, TaskSpec


class _Out(BaseModel):
    text: str


def _template(**overrides: Any) -> AgentTemplate:
    defaults: dict[str, Any] = {"model": "test", "trust_level": TrustLevel.MEDIUM}
    defaults.update(overrides)
    return AgentTemplate(**defaults)


def _ok_result(agent: Agent, task: TaskSpec, body: str) -> AgentResult[BaseModel]:
    return AgentResult[BaseModel](
        output=_Out(text=body),
        error=None,
        metadata=ResultMetadata(backend="StubBackend"),
        agent_name=agent.name,
        task_id=task.id,
    )


def _fail_result(agent: Agent, task: TaskSpec, msg: str) -> AgentResult[BaseModel]:
    return AgentResult[BaseModel](
        output=None,
        error=SpawnError(msg),
        metadata=ResultMetadata(backend="StubBackend"),
        agent_name=agent.name,
        task_id=task.id,
    )


@pytest.fixture
def runtime() -> AgentRuntime:
    return AgentRuntime()


# ---------------------------------------------------------------------------
# Factory shape
# ---------------------------------------------------------------------------


def test_factory_returns_callable(runtime: AgentRuntime) -> None:
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )
    assert callable(tool)


def test_factory_rejects_zero_concurrency(runtime: AgentRuntime) -> None:
    with pytest.raises(ValueError, match=">= 1"):
        make_spawn_agents_tool(
            runtime=runtime,
            template=_template(),
            output_type=_Out,
            max_concurrency=0,
        )


def test_factory_rejects_negative_concurrency(runtime: AgentRuntime) -> None:
    with pytest.raises(ValueError, match=">= 1"):
        make_spawn_agents_tool(
            runtime=runtime,
            template=_template(),
            output_type=_Out,
            max_concurrency=-3,
        )


# ---------------------------------------------------------------------------
# Empty / single / multiple specs — happy path
# ---------------------------------------------------------------------------


async def test_empty_specs_returns_empty_list(runtime: AgentRuntime) -> None:
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )
    out = await tool([])
    assert out == []


async def test_single_spec_dispatches_one_child(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[tuple[str, str]] = []

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        captured.append((agent.name, task.input))
        return _ok_result(agent, task, body=f"reply for {agent.name}")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )

    out = await tool([SpawnSpec(name="a", instructions="be terse", input="hello")])

    assert captured == [("a", "hello")]
    assert out == [
        SpawnResult(name="a", success=True, output={"text": "reply for a"}),
    ]


async def test_multiple_specs_preserve_order(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """asyncio.gather preserves submission order — verify the result list
    matches the spec list 1:1 even when children complete out of order."""

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        # Reverse the natural completion order: 'c' resolves first, 'a' last.
        delay = {"a": 0.03, "b": 0.02, "c": 0.01}[agent.name]
        await asyncio.sleep(delay)
        return _ok_result(agent, task, body=agent.name)

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )

    out = await tool(
        [
            SpawnSpec(name="a", instructions="x", input="task-a"),
            SpawnSpec(name="b", instructions="x", input="task-b"),
            SpawnSpec(name="c", instructions="x", input="task-c"),
        ]
    )
    assert [r.name for r in out] == ["a", "b", "c"]
    assert all(r.success for r in out)


# ---------------------------------------------------------------------------
# Children inherit from the template
# ---------------------------------------------------------------------------


async def test_children_inherit_model_from_template(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_models: list[object] = []

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        seen_models.append(agent.model)
        return _ok_result(agent, task, body="ok")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime,
        template=_template(model="anthropic:claude-sonnet-4-6"),
        output_type=_Out,
    )

    await tool([SpawnSpec(name="a", instructions="x", input="i")])
    assert seen_models == ["anthropic:claude-sonnet-4-6"]


async def test_children_inherit_trust_level_from_template(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_trust: list[TrustLevel] = []

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        seen_trust.append(agent.trust_level)
        return _ok_result(agent, task, body="ok")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime,
        template=_template(trust_level=TrustLevel.LOW),
        output_type=_Out,
    )

    await tool([SpawnSpec(name="a", instructions="x", input="i")])
    assert seen_trust == [TrustLevel.LOW]


async def test_children_share_configured_output_type(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_types: list[type[BaseModel]] = []

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        seen_types.append(agent.output_type)
        return _ok_result(agent, task, body="ok")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )

    await tool(
        [
            SpawnSpec(name="a", instructions="x", input="i"),
            SpawnSpec(name="b", instructions="y", input="j"),
        ]
    )
    assert seen_types == [_Out, _Out]


async def test_pre_instruction_concatenates_into_child_instructions(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_instructions: list[str] = []

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        seen_instructions.append(agent.instructions)
        return _ok_result(agent, task, body="ok")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime,
        template=_template(pre_instruction="Always JSON."),
        output_type=_Out,
    )

    await tool([SpawnSpec(name="a", instructions="Find X.", input="i")])
    assert seen_instructions == ["Always JSON.\n\nFind X."]


# ---------------------------------------------------------------------------
# Failure capture
# ---------------------------------------------------------------------------


async def test_per_child_runtime_error_captured_not_propagated(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        if agent.name == "bad":
            raise SpawnError("provider down")
        return _ok_result(agent, task, body="ok")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )

    out = await tool(
        [
            SpawnSpec(name="good", instructions="x", input="i"),
            SpawnSpec(name="bad", instructions="x", input="i"),
        ]
    )

    assert [r.success for r in out] == [True, False]
    assert "provider down" in (out[1].error or "")


async def test_per_child_result_error_captured(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing child returns an AgentResult with error set (not raised)."""

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        return _fail_result(agent, task, msg="validation drift")

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )

    out = await tool([SpawnSpec(name="a", instructions="x", input="i")])
    assert out[0].success is False
    assert "validation drift" in (out[0].error or "")


# ---------------------------------------------------------------------------
# Concurrency cap — semaphore actually limits simultaneity
# ---------------------------------------------------------------------------


async def test_max_concurrency_caps_simultaneous_children(
    runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    in_flight = 0
    peak = 0

    async def fake_run(agent: Agent, task: TaskSpec) -> AgentResult[BaseModel]:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await asyncio.sleep(0.01)
            return _ok_result(agent, task, body="ok")
        finally:
            in_flight -= 1

    monkeypatch.setattr(runtime, "run", fake_run)
    tool = make_spawn_agents_tool(
        runtime=runtime,
        template=_template(),
        output_type=_Out,
        max_concurrency=2,
    )

    await tool([SpawnSpec(name=f"a{i}", instructions="x", input="i") for i in range(8)])
    assert peak <= 2, f"peak in-flight was {peak}, exceeded cap of 2"


# ---------------------------------------------------------------------------
# Tool integrates with the runtime's ToolRegistry
# ---------------------------------------------------------------------------


def test_registers_on_a_tool_registry(runtime: AgentRuntime) -> None:
    """The factory's return value plugs into ToolRegistry like any other tool."""
    tool = make_spawn_agents_tool(
        runtime=runtime, template=_template(), output_type=_Out
    )
    reg = ToolRegistry()
    reg.register("spawn_agents", tool)
    assert "spawn_agents" in reg.names()


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


def test_spawn_types_publicly_exported() -> None:
    from murmur import tools

    assert tools.SpawnSpec is SpawnSpec
    assert tools.SpawnResult is SpawnResult
    assert tools.make_spawn_agents_tool is make_spawn_agents_tool
    assert "SpawnSpec" in tools.__all__
    assert "SpawnResult" in tools.__all__
    assert "make_spawn_agents_tool" in tools.__all__


def test_spawn_spec_and_result_are_frozen() -> None:
    from pydantic import ValidationError

    s = SpawnSpec(name="a", instructions="x", input="i")
    with pytest.raises(ValidationError):
        s.name = "b"

    r = SpawnResult(name="a", success=True, output={"text": "x"})
    with pytest.raises(ValidationError):
        r.success = False
