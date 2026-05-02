"""MCP expose side — Murmur agents callable as MCP tools.

Opt-in at two levels:

1. **Surface.** :class:`AgentServer` doesn't speak MCP unless
   :meth:`AgentServer.serve_mcp` is explicitly called. Constructing
   :class:`AgentServer` does not start an MCP server.
2. **Per-agent.** Even when ``serve_mcp()`` is on, agents are not
   exposed by default. Use :meth:`AgentServer.register_mcp` to
   explicitly enroll each agent. ``register()`` is HTTP-only.

The implementation lives behind the ``murmur-ai[mcp-server]`` extra so
``import murmur.server`` doesn't pull the MCP SDK. Symbols here are
imported lazily by :class:`AgentServer` — they raise a clear,
actionable error if the extra wasn't installed.

>>> server = AgentServer(runtime=runtime)
>>> server.register_mcp(researcher, tool_name="research")
>>> await server.serve_mcp(transport="stdio")
"""

from __future__ import annotations

from murmur.mcp_server._enrollment import MCPEnrollment

__all__ = ["MCPEnrollment"]
