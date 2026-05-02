"""End-to-end test of FastMCP construction from enrolled agents.

Builds a real FastMCP via ``build_fastmcp`` — same code path
``AgentServer.serve_mcp`` runs — and inspects the registered tool
metadata. Exercises the bridge + schema-derivation + name/description
threading without actually starting a transport (stdio/http would
block).
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.mcp_server import MCPEnrollment
from murmur.mcp_server._server import build_fastmcp
from murmur.types import AgentResult, ResultMetadata


class _Out(BaseModel):
    text: str


def _agent(name: str = "researcher") -> Agent:
    return Agent(
        name=name,
        model="test",
        instructions="Find facts.",
        output_type=_Out,
        trust_level=TrustLevel.MEDIUM,
    )


def test_fastmcp_registers_tools_for_each_enrollment() -> None:
    runtime = AgentRuntime()
    enrollments = (
        MCPEnrollment(agent=_agent("a"), tool_name="alpha", description="alpha desc"),
        MCPEnrollment(agent=_agent("b"), tool_name="beta", description="beta desc"),
    )
    fastmcp = build_fastmcp(runtime=runtime, enrollments=enrollments)

    # FastMCP exposes a private list_tools coroutine; for the synchronous
    # check we go through the internal tool manager which the SDK
    # populates eagerly at add_tool time.
    tools = fastmcp._tool_manager.list_tools()
    names = {t.name for t in tools}
    assert names == {"alpha", "beta"}

    by_name = {t.name: t for t in tools}
    assert by_name["alpha"].description == "alpha desc"
    assert by_name["beta"].description == "beta desc"


def test_fastmcp_tool_input_schema_has_input_string_param() -> None:
    """Each agent's MCP tool wraps a Murmur agent — every wrapper takes
    a single ``input: str`` parameter at the wire level (mirroring
    ``TaskSpec.input``). The MCP JSON schema should reflect that."""
    runtime = AgentRuntime()
    enrollment = MCPEnrollment(agent=_agent(), tool_name="research", description="...")
    fastmcp = build_fastmcp(runtime=runtime, enrollments=(enrollment,))
    tool = next(t for t in fastmcp._tool_manager.list_tools() if t.name == "research")

    # FuncMetadata builds the schema; it lives on the registered Tool.
    schema = tool.parameters
    assert isinstance(schema, dict)
    properties = schema.get("properties", {})
    assert "input" in properties
    assert properties["input"]["type"] == "string"


async def test_fastmcp_tool_call_dispatches_through_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: invoke the registered tool through FastMCP's call
    path, verify the agent's output reaches the caller."""
    runtime = AgentRuntime()
    agent = _agent()

    async def fake_run(a: Agent, t: TaskSpec) -> AgentResult[BaseModel]:
        return AgentResult[BaseModel](
            output=_Out(text=f"echo: {t.input}"),
            error=None,
            metadata=ResultMetadata(backend="StubBackend"),
            agent_name=a.name,
            task_id=t.id,
        )

    monkeypatch.setattr(runtime, "run", fake_run)
    enrollment = MCPEnrollment(agent=agent, tool_name="research", description="...")
    fastmcp = build_fastmcp(runtime=runtime, enrollments=(enrollment,))

    # FastMCP.call_tool is the SDK's dispatch entry point; it goes
    # through the tool manager + FuncMetadata pre-validated args.
    result = await fastmcp.call_tool("research", {"input": "hello"})
    # call_tool returns a tuple (content_blocks, structured_output) per
    # MCP SDK 1.27; structured_output for a dict-returning tool is the
    # dict itself.
    structured = result[1] if isinstance(result, tuple) else result
    assert structured == {"text": "echo: hello"}


def test_fastmcp_server_name_threads_through() -> None:
    runtime = AgentRuntime()
    enrollment = MCPEnrollment(agent=_agent(), tool_name="research", description="...")
    fastmcp = build_fastmcp(
        runtime=runtime,
        enrollments=(enrollment,),
        server_name="my-fleet",
        instructions="Murmur agents exposed here.",
    )
    assert fastmcp.name == "my-fleet"
