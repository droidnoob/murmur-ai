"""Tests for the MCP path in ``murmur._dispatch.build_pydantic_ai_agent``.

Covers ``9mt.4`` wiring without spawning real MCP servers:

- :class:`Agent.mcp_servers` providers are started before tool discovery.
- Discovered tool names are added to the executor's ``allowed`` set so
  trust gating sees them.
- ``SANDBOX`` trust short-circuits — providers aren't even started, no
  MCP tools exposed to the model.
- ``_PolicyMCPToolset.call_tool`` routes through ``ToolExecutor.execute``
  via ``external_call``, firing the same lifecycle events native tools do.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest
from pydantic import BaseModel
from pydantic_ai.toolsets import AbstractToolset
from structlog.testing import capture_logs

from murmur._dispatch import _PolicyMCPToolset, build_pydantic_ai_agent
from murmur.agent import Agent
from murmur.core.protocols.toolsets import ToolDescriptor
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry
from murmur.types import TrustLevel

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Out(BaseModel):
    text: str


@dataclass
class _StubAbstractToolset(AbstractToolset[Any]):
    """Minimal AbstractToolset that records ``call_tool`` invocations."""

    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    return_value: object = "remote-result"

    @property
    def id(self) -> str | None:
        return "stub-toolset"

    async def get_tools(self, ctx: object) -> dict[str, Any]:  # noqa: ARG002
        return {}

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: object,  # noqa: ARG002
        tool: object,  # noqa: ARG002
    ) -> Any:
        self.calls.append((name, dict(tool_args)))
        return self.return_value


class _StubProvider:
    """Murmur :class:`ToolsetProvider` whose ``_mcp`` is a fake AbstractToolset."""

    def __init__(
        self,
        descriptors: Sequence[ToolDescriptor],
        *,
        allow: Sequence[str] | None = None,
    ) -> None:
        self._mcp = _StubAbstractToolset()
        self._descriptors = tuple(descriptors)
        self._allow: frozenset[str] | None = (
            frozenset(allow) if allow is not None else None
        )
        self.start_count = 0
        self.stop_count = 0
        self.list_tools_count = 0

    @property
    def allow(self) -> frozenset[str] | None:
        return self._allow

    async def start(self) -> None:
        self.start_count += 1

    async def stop(self) -> None:
        self.stop_count += 1

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        self.list_tools_count += 1
        return self._descriptors

    async def call_tool(self, name: str, args: Mapping[str, object]) -> object:
        return await self._mcp.call_tool(name, dict(args), None, None)


def _agent_with_mcp(
    *providers: _StubProvider, trust_level: TrustLevel = TrustLevel.MEDIUM
) -> Agent:
    return Agent(
        name="researcher",
        model="test",
        instructions="...",
        output_type=_Out,
        mcp_servers=tuple(providers),
        trust_level=trust_level,
    )


# ---------------------------------------------------------------------------
# build_pydantic_ai_agent — MCP path
# ---------------------------------------------------------------------------


async def test_build_discovers_provider_tools() -> None:
    """Discovery runs through the underlying toolset's ``list_tools``.

    The stub's ``_StubAbstractToolset`` doesn't define ``list_tools`` so the
    fallback branch hits ``provider.list_tools()`` instead. Either way,
    the discovered names propagate into the dispatch pipeline.
    """
    provider = _StubProvider([ToolDescriptor(name="read_file")])
    registry = ToolRegistry()
    executor = ToolExecutor(registry)
    agent = _agent_with_mcp(provider)

    pa_agent = await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-1",
    )

    assert provider.list_tools_count == 1
    assert pa_agent is not None


async def test_sandbox_trust_skips_mcp_entirely() -> None:
    provider = _StubProvider([ToolDescriptor(name="read_file")])
    registry = ToolRegistry()
    executor = ToolExecutor(registry)
    agent = _agent_with_mcp(provider, trust_level=TrustLevel.SANDBOX)

    await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-2",
    )

    assert provider.list_tools_count == 0


async def test_no_mcp_servers_skips_resolution() -> None:
    """Agent with empty mcp_servers tuple — no toolset construction at all."""
    registry = ToolRegistry()
    executor = ToolExecutor(registry)
    agent = Agent(
        name="r",
        model="test",
        instructions="...",
        output_type=_Out,
    )

    pa_agent = await build_pydantic_ai_agent(
        agent=agent,
        allowed=frozenset(),
        registry=registry,
        executor=executor,
        task_id="t-3",
    )
    assert pa_agent is not None


# ---------------------------------------------------------------------------
# _PolicyMCPToolset — routes through executor
# ---------------------------------------------------------------------------


async def test_policy_toolset_routes_call_through_executor() -> None:
    inner = _StubAbstractToolset()
    executor = ToolExecutor(ToolRegistry())
    wrapper = _PolicyMCPToolset(
        wrapped=inner,
        agent_name="r",
        task_id="t",
        trust_level=TrustLevel.MEDIUM,
        allowed=frozenset({"read_file"}),
        executor=executor,
    )

    with capture_logs() as captured:
        result = await wrapper.call_tool("read_file", {"path": "/a"}, None, None)  # ty: ignore[invalid-argument-type]

    assert result == "remote-result"
    assert inner.calls == [("read_file", {"path": "/a"})]

    events = [c["event"] for c in captured]
    assert "tool_call_started" in events
    assert "tool_call_completed" in events


async def test_policy_toolset_rejects_when_not_in_allowed() -> None:
    from murmur.core.errors import TrustViolationError

    inner = _StubAbstractToolset()
    executor = ToolExecutor(ToolRegistry())
    wrapper = _PolicyMCPToolset(
        wrapped=inner,
        agent_name="r",
        task_id="t",
        trust_level=TrustLevel.MEDIUM,
        allowed=frozenset({"read_file"}),  # write_file NOT in allow-list
        executor=executor,
    )

    with pytest.raises(TrustViolationError):
        await wrapper.call_tool("write_file", {}, None, None)  # ty: ignore[invalid-argument-type]

    assert inner.calls == []


# ---------------------------------------------------------------------------
# Trust gating — allow-list (9mt.5)
# ---------------------------------------------------------------------------


async def test_low_trust_without_allow_list_skips_provider() -> None:
    """LOW trust + ``allow=None`` → provider skipped entirely (safe default)."""
    provider = _StubProvider(
        [ToolDescriptor(name="read_file"), ToolDescriptor(name="delete_file")]
    )
    registry = ToolRegistry()
    executor = ToolExecutor(registry)
    agent = _agent_with_mcp(provider, trust_level=TrustLevel.LOW)

    # Build directly to avoid pydantic_ai trying to register MCP tools.
    from murmur._dispatch import _resolve_mcp_toolsets

    toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert toolsets == []
    assert names == frozenset()


async def test_low_trust_with_allow_list_exposes_only_listed() -> None:
    provider = _StubProvider(
        [ToolDescriptor(name="read_file"), ToolDescriptor(name="delete_file")],
        allow=["read_file"],
    )
    executor = ToolExecutor(ToolRegistry())
    agent = _agent_with_mcp(provider, trust_level=TrustLevel.LOW)

    from murmur._dispatch import _resolve_mcp_toolsets

    toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert names == frozenset({"read_file"})
    assert len(toolsets) == 1


async def test_low_trust_allow_listed_tool_passes_executor_gate() -> None:
    """LOW + allow-listed → ``low_trust_overrides`` lets the call through."""
    inner = _StubAbstractToolset()
    executor = ToolExecutor(ToolRegistry())
    wrapper = _PolicyMCPToolset(
        wrapped=inner,
        agent_name="r",
        task_id="t",
        trust_level=TrustLevel.LOW,
        allowed=frozenset({"delete_file"}),  # not in default _READ_ONLY_TOOLS
        executor=executor,
        low_trust_overrides=frozenset({"delete_file"}),
    )

    result = await wrapper.call_tool("delete_file", {"path": "/x"}, None, None)  # ty: ignore[invalid-argument-type]
    assert result == "remote-result"
    assert inner.calls == [("delete_file", {"path": "/x"})]


async def test_low_trust_not_allow_listed_blocked_at_executor() -> None:
    """LOW without override → executor's read-only gate rejects."""
    from murmur.core.errors import TrustViolationError

    inner = _StubAbstractToolset()
    executor = ToolExecutor(ToolRegistry())
    wrapper = _PolicyMCPToolset(
        wrapped=inner,
        agent_name="r",
        task_id="t",
        trust_level=TrustLevel.LOW,
        allowed=frozenset({"delete_file"}),
        executor=executor,
        low_trust_overrides=frozenset(),  # no overrides
    )

    with pytest.raises(TrustViolationError, match="not read-only"):
        await wrapper.call_tool("delete_file", {}, None, None)  # ty: ignore[invalid-argument-type]
    assert inner.calls == []


async def test_medium_trust_allow_list_narrows_exposed_tools() -> None:
    """MEDIUM + allow=(read_file,) → only read_file exposed though server has 2."""
    provider = _StubProvider(
        [ToolDescriptor(name="read_file"), ToolDescriptor(name="write_file")],
        allow=["read_file"],
    )
    executor = ToolExecutor(ToolRegistry())
    agent = _agent_with_mcp(provider, trust_level=TrustLevel.MEDIUM)

    from murmur._dispatch import _resolve_mcp_toolsets

    _toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert names == frozenset({"read_file"})


async def test_medium_trust_no_allow_exposes_everything() -> None:
    provider = _StubProvider(
        [ToolDescriptor(name="read_file"), ToolDescriptor(name="write_file")],
    )
    executor = ToolExecutor(ToolRegistry())
    agent = _agent_with_mcp(provider, trust_level=TrustLevel.MEDIUM)

    from murmur._dispatch import _resolve_mcp_toolsets

    _toolsets, names = await _resolve_mcp_toolsets(
        agent=agent, allowed=frozenset(), executor=executor, task_id="t"
    )
    assert names == frozenset({"read_file", "write_file"})


# ---------------------------------------------------------------------------
# Runtime lifecycle (9mt.5)
# ---------------------------------------------------------------------------


async def test_runtime_shutdown_stops_seen_providers() -> None:
    """``runtime.shutdown()`` calls ``stop()`` on every provider seen via ``run``."""
    from murmur import AgentRuntime

    provider = _StubProvider([ToolDescriptor(name="read_file")])
    rt = AgentRuntime()
    agent = _agent_with_mcp(provider)
    # Trigger _resolve so the runtime registers the provider.
    rt._resolve(agent)

    await rt.shutdown()
    assert provider.stop_count == 1


async def test_runtime_shutdown_idempotent_across_resolves() -> None:
    """Resolving the same agent twice — provider tracked once."""
    from murmur import AgentRuntime

    provider = _StubProvider([ToolDescriptor(name="read_file")])
    rt = AgentRuntime()
    agent = _agent_with_mcp(provider)
    rt._resolve(agent)
    rt._resolve(agent)

    await rt.shutdown()
    assert provider.stop_count == 1


async def test_runtime_shutdown_swallows_provider_stop_errors() -> None:
    """One provider's stop() raising shouldn't block others from stopping."""
    from murmur import AgentRuntime

    class _BadProvider(_StubProvider):
        async def stop(self) -> None:
            self.stop_count += 1
            raise RuntimeError("nope")

    bad = _BadProvider([ToolDescriptor(name="x")])
    good = _StubProvider([ToolDescriptor(name="y")])
    rt = AgentRuntime()
    agent = _agent_with_mcp(bad, good)
    rt._resolve(agent)

    await rt.shutdown()
    assert bad.stop_count == 1
    assert good.stop_count == 1
