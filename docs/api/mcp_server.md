# MCP server

MCP expose side — Murmur agents callable as MCP tools by Claude
Desktop / Cursor / the MCP Inspector / any other MCP-aware client.
Behind the `murmur-ai[mcp-server]` extra.

```bash
pip install 'murmur-ai[mcp-server]'
```

The two opt-in tiers (surface + per-agent) live as methods on
`AgentServer`. The value type for an enrollment is exposed for users
who want to construct the registry programmatically; the
`register_mcp(...)` method is the usual path. See the
[MCP concept page](../concepts/mcp.md#expose-side-agentserverserve_mcp)
for the end-to-end usage pattern.

## `AgentServer.register_mcp`

`register_mcp(agent, *, tool_name=None, description=None) -> None`

Enrolls an agent for MCP exposure. Distinct from `register()` (which
is HTTP-only). Auto-registers the agent on the runtime so the bridge
can dispatch by name. Re-enrollment under the same `tool_name`
replaces.

| Argument | Default | Effect |
|---|---|---|
| `agent` | — | The Murmur agent to expose. |
| `tool_name` | `agent.name` | Public MCP tool name. Override when the operator-facing name differs from the agent's internal one. |
| `description` | First line of `agent.instructions`, truncated to 200 chars | Human-readable summary the calling LLM uses to decide when to invoke the tool. |

## `AgentServer.serve_mcp`

`serve_mcp(*, transport="stdio", server_name="murmur", instructions=None, host="127.0.0.1", port=8765) -> None`

Async — blocks until the transport exits. Builds a fresh `FastMCP`
per call so multiple invocations on the same server work cleanly.

Raises `RegistryError` when no agents are enrolled — silently starting
an empty MCP server would mask a misconfiguration.

| Argument | Default | Effect |
|---|---|---|
| `transport` | `"stdio"` | `"stdio"` for desktop clients; `"http"` for remote/hosted use. |
| `server_name` | `"murmur"` | Reported to MCP clients in the server identification handshake. |
| `instructions` | `None` | Free-text guidance shown to the calling LLM (alongside per-tool descriptions). |
| `host` / `port` | `"127.0.0.1"` / `8765` | HTTP transport only. Ignored for stdio. |

## `MCPEnrollment`

::: murmur.mcp_server.MCPEnrollment
    options:
      heading_level: 3
      show_bases: false
