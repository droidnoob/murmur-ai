"""Tests for MCP enrollment + the agent→MCP bridge.

Covers the data layer (enrollment value object, defaults), the bridge
(per-agent wrapper that turns ``runtime.run`` into an MCP-callable
coroutine), and ``AgentServer.register_mcp`` enrollment semantics.
The transport-level tests live alongside in ``test_serve_mcp.py``.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.core.errors import RegistryError, SpawnError
from murmur.mcp_server import MCPEnrollment
from murmur.mcp_server._bridge import make_agent_tool
from murmur.server import AgentServer
from murmur.types import AgentResult, ResultMetadata


class _Out(BaseModel):
    text: str


def _agent(name: str = "researcher", instructions: str = "Find facts.") -> Agent:
    return Agent(
        name=name,
        model="test",
        instructions=instructions,
        output_type=_Out,
        trust_level=TrustLevel.MEDIUM,
    )


def _ok_result(agent: Agent, task: TaskSpec, body: str) -> AgentResult[BaseModel]:
    return AgentResult[BaseModel](
        output=_Out(text=body),
        error=None,
        metadata=ResultMetadata(backend="StubBackend"),
        agent_name=agent.name,
        task_id=task.id,
    )


# ---------------------------------------------------------------------------
# MCPEnrollment value object
# ---------------------------------------------------------------------------


def test_enrollment_is_frozen() -> None:
    e = MCPEnrollment(agent=_agent(), tool_name="research", description="...")
    with pytest.raises(ValidationError):
        e.tool_name = "other"


def test_enrollment_requires_non_empty_tool_name() -> None:
    with pytest.raises(ValidationError):
        MCPEnrollment(agent=_agent(), tool_name="", description="x")


def test_enrollment_requires_non_empty_description() -> None:
    with pytest.raises(ValidationError):
        MCPEnrollment(agent=_agent(), tool_name="research", description="")


# ---------------------------------------------------------------------------
# Bridge — make_agent_tool
# ---------------------------------------------------------------------------


async def test_bridge_dispatches_through_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AgentRuntime()
    agent = _agent()
    captured: list[tuple[str, str]] = []

    async def fake_run(a: Agent, t: TaskSpec) -> AgentResult[BaseModel]:
        captured.append((a.name, t.input))
        return _ok_result(a, t, body="hello back")

    monkeypatch.setattr(runtime, "run", fake_run)
    enrollment = MCPEnrollment(agent=agent, tool_name="research", description="...")
    tool = make_agent_tool(runtime=runtime, enrollment=enrollment)

    payload = await tool("hello")
    assert captured == [("researcher", "hello")]
    assert payload == {"text": "hello back"}


async def test_bridge_raises_on_runtime_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AgentRuntime()
    agent = _agent()

    async def failing_run(_a: Agent, _t: TaskSpec) -> AgentResult[BaseModel]:
        return AgentResult[BaseModel](
            output=None,
            error=SpawnError("provider down"),
            metadata=ResultMetadata(backend="StubBackend"),
            agent_name=agent.name,
            task_id="t-1",
        )

    monkeypatch.setattr(runtime, "run", failing_run)
    enrollment = MCPEnrollment(agent=agent, tool_name="research", description="...")
    tool = make_agent_tool(runtime=runtime, enrollment=enrollment)

    with pytest.raises(RuntimeError, match="provider down"):
        await tool("hello")


async def test_bridge_raises_on_unexpected_run_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised exception inside runtime.run propagates — MCP SDK
    converts it into a tool error returned to the client."""
    runtime = AgentRuntime()
    agent = _agent()

    async def raising_run(_a: Agent, _t: TaskSpec) -> AgentResult[BaseModel]:
        raise SpawnError("boom")

    monkeypatch.setattr(runtime, "run", raising_run)
    enrollment = MCPEnrollment(agent=agent, tool_name="research", description="...")
    tool = make_agent_tool(runtime=runtime, enrollment=enrollment)

    with pytest.raises(SpawnError, match="boom"):
        await tool("hello")


def test_bridge_sets_friendly_name_and_doc() -> None:
    runtime = AgentRuntime()
    enrollment = MCPEnrollment(
        agent=_agent(),
        tool_name="research",
        description="Run a research query.",
    )
    tool = make_agent_tool(runtime=runtime, enrollment=enrollment)
    # ``Callable`` doesn't expose ``__name__`` in its protocol type even
    # though the underlying function does — go through getattr so static
    # type-checkers don't reject the access.
    assert getattr(tool, "__name__") == "research"  # noqa: B009
    assert getattr(tool, "__doc__") == "Run a research query."  # noqa: B009


# ---------------------------------------------------------------------------
# AgentServer.register_mcp — opt-in semantics
# ---------------------------------------------------------------------------


def test_register_does_not_enroll_for_mcp() -> None:
    """The HTTP register() must not implicitly enroll an agent for MCP —
    that's the whole point of the per-agent opt-in tier."""
    server = AgentServer()
    server.register(_agent("internal_admin"))
    assert server._mcp_enrollments == {}


def test_register_mcp_enrolls_with_default_tool_name() -> None:
    server = AgentServer()
    agent = _agent("researcher")
    server.register_mcp(agent)
    assert "researcher" in server._mcp_enrollments
    assert server._mcp_enrollments["researcher"].agent is agent


def test_register_mcp_uses_explicit_tool_name() -> None:
    server = AgentServer()
    agent = _agent("researcher-v3")
    server.register_mcp(agent, tool_name="research")
    assert "research" in server._mcp_enrollments
    assert "researcher-v3" not in server._mcp_enrollments


def test_register_mcp_default_description_is_first_line_of_instructions() -> None:
    server = AgentServer()
    agent = _agent(
        instructions="Look up capital cities.\nMore detailed instructions follow.",
    )
    server.register_mcp(agent, tool_name="cap")
    assert server._mcp_enrollments["cap"].description == "Look up capital cities."


def test_register_mcp_long_instructions_truncated() -> None:
    server = AgentServer()
    long = "A" * 500
    agent = _agent(instructions=long)
    server.register_mcp(agent, tool_name="t")
    description = server._mcp_enrollments["t"].description
    assert len(description) <= 200
    assert description.endswith("…")


def test_register_mcp_explicit_description_wins() -> None:
    server = AgentServer()
    agent = _agent(instructions="lengthy original instructions")
    server.register_mcp(agent, tool_name="t", description="Public-facing summary.")
    assert server._mcp_enrollments["t"].description == "Public-facing summary."


def test_register_mcp_auto_registers_for_runtime_dispatch() -> None:
    """The bridge dispatches via runtime.run by agent reference; making
    the operator manage two registries (HTTP + MCP) for the same agent
    would be a footgun. ``register_mcp`` adds to the HTTP map too."""
    server = AgentServer()
    agent = _agent("researcher")
    server.register_mcp(agent)
    assert "researcher" in server._agents


def test_register_mcp_re_enrollment_replaces() -> None:
    server = AgentServer()
    a1 = _agent("a")
    a2 = _agent("a")  # same name, different instance
    server.register_mcp(a1, tool_name="t", description="d1")
    server.register_mcp(a2, tool_name="t", description="d2")
    assert server._mcp_enrollments["t"].agent is a2
    assert server._mcp_enrollments["t"].description == "d2"


def test_register_mcp_two_agents_one_server() -> None:
    server = AgentServer()
    server.register_mcp(_agent("alpha"), tool_name="alpha")
    server.register_mcp(_agent("beta"), tool_name="beta")
    assert set(server._mcp_enrollments.keys()) == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# AgentServer.serve_mcp — pre-flight checks
# ---------------------------------------------------------------------------


async def test_serve_mcp_with_no_enrollments_raises() -> None:
    """Silently starting an empty MCP server would hide an operator
    misconfiguration. Prefer a clear error."""
    server = AgentServer()
    with pytest.raises(RegistryError, match="no agents enrolled"):
        await server.serve_mcp(transport="stdio")


# ---------------------------------------------------------------------------
# Public API exposure
# ---------------------------------------------------------------------------


def test_mcp_enrollment_is_exported_from_mcp_server_package() -> None:
    from murmur import mcp_server

    assert mcp_server.MCPEnrollment is MCPEnrollment
    assert "MCPEnrollment" in mcp_server.__all__


def test_register_mcp_is_method_on_agent_server() -> None:
    """Sanity: the method is on the class, not a free function."""
    assert callable(getattr(AgentServer, "register_mcp", None))
    assert callable(getattr(AgentServer, "serve_mcp", None))
