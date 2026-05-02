"""``MCPEnrollment`` — frozen record of an agent's MCP exposure.

Carries the per-agent metadata the MCP server needs at construction
time. Stays free of ``mcp`` SDK imports so :class:`AgentServer` can
hold a list of these without pulling the optional dep.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from murmur.agent import Agent


class MCPEnrollment(BaseModel):
    """One agent enrolled for MCP exposure.

    The :attr:`agent` is held by reference; :class:`AgentServer` is
    responsible for ensuring it's also registered with the runtime so
    the bridge can dispatch via ``runtime.run``.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    agent: Agent
    """The Murmur agent this enrollment exposes. Must also be
    registered with the runtime (``runtime.register(agent)`` or
    ``server.register(agent)`` — the bridge dispatches by name)."""

    tool_name: str = Field(min_length=1)
    """Public tool name shown to MCP clients. Defaults to
    ``agent.name`` when not overridden — but operators usually want a
    distinct outward-facing name (e.g. agent ``"researcher-v3"`` →
    tool ``"research"``)."""

    description: str = Field(min_length=1)
    """Human-readable description sent to MCP clients in the tool list.
    Defaults to ``agent.instructions`` truncated when not overridden.
    LLMs reading the tool catalogue use this to decide when to call
    the tool — make it specific."""
