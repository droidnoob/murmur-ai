"""Bridge — turn a Murmur agent + runtime into an MCP-callable coroutine.

The MCP SDK's ``FastMCP.add_tool`` introspects a callable's signature
to build the tool's JSON schema. We construct a per-agent wrapper
function whose signature is ``async def call(input: str) -> dict``,
matching what every Murmur agent accepts at the wire level
(``TaskSpec.input: str``). FastMCP turns that into a tool with a
single string parameter.

Output: the agent's ``output_type.model_dump()`` payload as a dict.
On failure, raises so the MCP SDK reports a tool error to the client.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from murmur.types import TaskSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from murmur.mcp_server._enrollment import MCPEnrollment
    from murmur.runtime import AgentRuntime


def make_agent_tool(
    *,
    runtime: AgentRuntime,
    enrollment: MCPEnrollment,
) -> Callable[[str], Awaitable[dict[str, Any]]]:
    """Build the per-agent MCP tool callable.

    Captures ``runtime`` and ``enrollment`` in a closure; the returned
    coroutine takes the MCP client's ``input`` string, dispatches
    through ``runtime.run``, and returns the agent's structured output
    as a JSON-friendly dict.
    """
    agent = enrollment.agent

    async def call(input: str) -> dict[str, Any]:
        result = await runtime.run(agent, TaskSpec(input=input))
        if not result.is_ok() or result.output is None:
            error = result.error
            raise RuntimeError(
                f"agent {agent.name!r} failed: "
                f"{error if error is not None else 'no output produced'}"
            )
        return result.output.model_dump()

    # Friendly __name__ / __doc__ so introspecting tools (FastMCP uses
    # fn name as a fallback when ``name`` isn't passed) show the public
    # tool_name, not "call". ``setattr`` rather than direct attribute
    # write so ty doesn't reject the assignment against the inferred
    # Callable protocol return type — the underlying object is a
    # function and accepts both attributes.
    setattr(call, "__name__", enrollment.tool_name)  # noqa: B010
    setattr(call, "__doc__", enrollment.description)  # noqa: B010
    return call


__all__ = ["make_agent_tool"]
