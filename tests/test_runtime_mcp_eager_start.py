"""Tests for the MCP eager-start lifecycle (mp5).

When ``RuntimeOptions(mcp_eager_start=True)``, :class:`AgentRuntime`
spawns one supervisor task per MCP provider on first dispatch. The
supervisor enters the provider's context once, holds the entry open
until shutdown, then releases — keeping the underlying subprocess /
HTTP session warm across runs while satisfying anyio's
same-task-entry-and-exit constraint on cancel scopes.

The tests use a ``ToolsetProvider`` stand-in (no real PydanticAI
MCPServer) and assert:

- start/stop counts reflect supervisor-held lifecycle, not per-call respawn
- concurrent first-runs deduplicate to one supervisor
- shutdown cleanly stops every supervised provider
- start() failures surface to every concurrent waiter
- the option defaults to off — no behaviour change for existing callers
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.thread import ThreadBackend
from murmur.context.null import NullContextPasser
from murmur.core.protocols.toolsets import ToolDescriptor
from murmur.runtime import AgentRuntime, RuntimeOptions
from murmur.types import TaskSpec, TrustLevel


class _Out(BaseModel):
    text: str


class _StubProvider:
    """Minimal :class:`ToolsetProvider` for supervisor-lifecycle tests.

    Records start / stop counts so tests can assert "entered once across N
    runs" semantics. ``allow=()`` is the trust-gate-friendly default — at
    LOW trust the runtime requires an explicit allow-list, but the agents
    in these tests use SANDBOX (which skips MCP entirely) or HIGH (which
    exposes everything), so allow doesn't gate anything. ``list_tools``
    returns an empty descriptor list — the dispatch path's
    ``_resolve_mcp_toolsets`` filter sees no names to expose, which is
    fine; we're testing the supervisor lifecycle, not tool discovery.
    """

    allow: frozenset[str] | None = None

    def __init__(self, *, fail_on_start: BaseException | None = None) -> None:
        self.start_count = 0
        self.stop_count = 0
        self._started = False
        self._fail = fail_on_start

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._fail is not None:
            raise self._fail
        self.start_count += 1
        self._started = True

    async def stop(self) -> None:
        self.stop_count += 1
        self._started = False

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        return ()

    async def call_tool(
        self, name: str, args: Mapping[str, object]
    ) -> object:  # pragma: no cover — not exercised
        raise NotImplementedError


def _agent(provider: _StubProvider) -> Agent:
    return Agent(
        name="r",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        # SANDBOX trust skips the MCP provider entirely in dispatch — but the
        # supervisor still warms it because warming is independent of trust.
        # That's actually the desired behaviour: warming is about subprocess
        # lifecycle, trust is about what tools the agent can see.
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
        mcp_servers=(provider,),
    )


async def _stub_pa_agent(
    agent: Agent, _allowed: frozenset[str], _task_id: str
) -> pydantic_ai.Agent[None, Any]:
    return pydantic_ai.Agent(
        model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
        instructions=agent.instructions,
        output_type=agent.output_type,
    )


def _make_runtime(*, eager: bool) -> AgentRuntime:
    backend = ThreadBackend()
    backend._build_pa_agent = _stub_pa_agent  # ty: ignore[invalid-assignment]  # test seam
    return AgentRuntime(
        backend=backend,
        options=RuntimeOptions(mcp_eager_start=eager),
    )


# ---------------------------------------------------------------------------
# Default-off: no supervisor spawned, no behaviour change
# ---------------------------------------------------------------------------


async def test_default_does_not_eager_start_provider() -> None:
    provider = _StubProvider()
    runtime = _make_runtime(eager=False)

    await runtime.run(_agent(provider), TaskSpec(input="hi"))

    # Per-call respawn semantics are PydanticAI's job; from the runtime's
    # POV the eager-start machinery shouldn't touch the provider.
    assert provider.start_count == 0
    assert provider.stop_count == 0


# ---------------------------------------------------------------------------
# Eager-start basic: one start across many runs, one stop on shutdown
# ---------------------------------------------------------------------------


async def test_eager_start_warms_provider_once_across_runs() -> None:
    provider = _StubProvider()
    runtime = _make_runtime(eager=True)

    for _ in range(5):
        await runtime.run(_agent(provider), TaskSpec(input="hi"))

    # Supervisor holds the provider open — only one start across all runs.
    assert provider.start_count == 1
    # Stop hasn't fired yet (no shutdown).
    assert provider.stop_count == 0
    assert provider.started is True

    await runtime.shutdown()
    assert provider.stop_count == 1
    assert provider.started is False


async def test_shutdown_drains_supervisor_tasks() -> None:
    """After shutdown, no warm-events / supervisor tasks remain."""
    provider = _StubProvider()
    runtime = _make_runtime(eager=True)

    await runtime.run(_agent(provider), TaskSpec(input="hi"))
    assert len(runtime._mcp_supervisor_tasks) == 1
    assert len(runtime._mcp_warm_events) == 1

    await runtime.shutdown()
    assert runtime._mcp_supervisor_tasks == {}
    assert runtime._mcp_warm_events == {}
    assert runtime._mcp_shutdown_events == {}


# ---------------------------------------------------------------------------
# Concurrency: first-run dedup
# ---------------------------------------------------------------------------


async def test_concurrent_first_runs_share_one_supervisor() -> None:
    """Two runs racing to first-warm the same provider must not double-start."""
    provider = _StubProvider()
    runtime = _make_runtime(eager=True)

    await asyncio.gather(
        runtime.run(_agent(provider), TaskSpec(input="a")),
        runtime.run(_agent(provider), TaskSpec(input="b")),
        runtime.run(_agent(provider), TaskSpec(input="c")),
    )

    assert provider.start_count == 1
    await runtime.shutdown()
    assert provider.stop_count == 1


# ---------------------------------------------------------------------------
# Multi-provider: one supervisor per provider
# ---------------------------------------------------------------------------


async def test_multiple_providers_each_get_their_own_supervisor() -> None:
    p1 = _StubProvider()
    p2 = _StubProvider()
    agent = Agent(
        name="r",
        model="anthropic:claude-sonnet-4-6",
        instructions="...",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
        mcp_servers=(p1, p2),
    )
    runtime = _make_runtime(eager=True)

    await runtime.run(agent, TaskSpec(input="hi"))

    assert p1.start_count == 1
    assert p2.start_count == 1
    assert len(runtime._mcp_supervisor_tasks) == 2

    await runtime.shutdown()
    assert p1.stop_count == 1
    assert p2.stop_count == 1


# ---------------------------------------------------------------------------
# Failure surface: start() error propagates to all concurrent waiters
# ---------------------------------------------------------------------------


async def test_start_failure_surfaces_to_all_waiters() -> None:
    """When the supervisor's start() raises, every concurrent first-run sees it."""
    provider = _StubProvider(fail_on_start=RuntimeError("subprocess failed"))
    runtime = _make_runtime(eager=True)

    results = await asyncio.gather(
        runtime.run(_agent(provider), TaskSpec(input="a")),
        runtime.run(_agent(provider), TaskSpec(input="b")),
        return_exceptions=True,
    )

    # AgentRuntime.run wraps backend errors in AgentResult; the warm-up
    # error currently propagates as a raw exception to the run() caller.
    # Either way, both runs should see the same kind of failure (no
    # silent half-success).
    failures = [
        r
        for r in results
        if isinstance(r, BaseException) or (hasattr(r, "is_ok") and not r.is_ok())
    ]
    assert len(failures) == 2

    # Subsequent runs after a failed warm continue to fail until the
    # runtime is reset — caching the error on the supervisor key is what
    # protects against half-warmed dispatches.
    with pytest.raises(RuntimeError, match="subprocess failed"):
        await runtime.run(_agent(provider), TaskSpec(input="c"))

    await runtime.shutdown()
