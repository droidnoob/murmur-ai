# Model Context Protocol (MCP)

Murmur agents consume MCP servers as tool sources. MCP-discovered tools
flow through the same `ToolExecutor` policy gate as native tools — same
trust enforcement, same lifecycle events, same observability.

## Attaching an MCP server

```python
from murmur import Agent
from murmur.tools import mcp_stdio, mcp_http, mcp_sse
from murmur.types import TrustLevel

git_mcp = mcp_stdio("npx", ["@modelcontextprotocol/server-git"])
files_mcp = mcp_http("http://localhost:7000/mcp", allow=["read_file", "list_dir"])

agent = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=ResearchFinding,
    mcp_servers=(git_mcp, files_mcp),
    trust_level=TrustLevel.MEDIUM,
)
```

Three transport factories:

| Factory | Transport | Use when |
|---|---|---|
| `mcp_stdio(command, args, env=…, prefix=…)` | stdio subprocess | Local CLI MCP servers (most servers). |
| `mcp_http(url, headers=…, prefix=…, allow=…)` | HTTP | Hosted MCP services. |
| `mcp_sse(url, headers=…, prefix=…, allow=…)` | Server-Sent Events | Streaming HTTP MCP. |

## Trust matrix

| Trust level | Behaviour |
|---|---|
| `SANDBOX` | MCP servers skipped entirely. |
| `LOW` | **Requires** explicit `allow=[…]` per server. No allow list → no MCP tools. |
| `MEDIUM` | All discovered tools exposed unless `allow=` narrows. |
| `HIGH` | All discovered tools exposed unless `allow=` narrows. |

MCP servers self-declare a `readOnlyHint` flag. Murmur explicitly does
**not** trust that for security gating — `allow=[…]` is the only way to
expose MCP tools at `LOW`.

## Tool prefixing

Two MCP servers exposing same-named tools coexist on one agent via
`prefix=`:

```python
git_a = mcp_stdio("server-git", ["--repo=A"], prefix="repo_a_")
git_b = mcp_stdio("server-git", ["--repo=B"], prefix="repo_b_")

agent = Agent(
    ...,
    mcp_servers=(git_a, git_b),
)
```

The prefix is forwarded as PydanticAI's `MCPServer.tool_prefix`, so the
agent sees `repo_a_status`, `repo_b_status`, etc. Allow-list entries
match the **prefixed** name when set.

## Lifecycle modes

### Default — per-call respawn

PydanticAI's `MCPServer.list_tools` and `direct_call_tool` each manage
their own `__aenter__`/`__aexit__`, spawning the stdio subprocess fresh
every dispatch. No configuration required; backwards-compatible with
every existing caller.

### Eager-start — keep subprocesses warm

Opt in via `RuntimeOptions(mcp_eager_start=True)`:

```python
from murmur import AgentRuntime, RuntimeOptions

runtime = AgentRuntime(
    options=RuntimeOptions(mcp_eager_start=True),
)

try:
    await runtime.run(agent, task)
finally:
    await runtime.shutdown()         # required — releases MCP subprocesses
```

When set, the runtime spawns one supervisor task per provider that holds
`__aenter__` open until the per-provider shutdown event fires, then
runs `__aexit__` on the same task. Inner dispatch enter/exit pairs
become no-ops via PydanticAI's upstream `_running_count` ref-counting.

> **The supervisor pattern is non-negotiable.** anyio cancel scopes are
> task-bound, so a cross-task `__aenter__`/`__aexit__` raises at runtime.
> Pair eager-start with a guaranteed `runtime.shutdown()`.

`AgentRouter` and `AgentServer` lifespans call `runtime.shutdown()`
automatically. Plain runtimes need explicit shutdown.

## What's enforced where

- Trust gate, allow-list, lifecycle events: `_PolicyMCPToolset` (a
  `WrapperToolset`) routes every MCP call through `ToolExecutor.execute`
  with `external_call=…`.
- Subprocess lifecycle: `AgentRuntime._warm_mcp_providers` /
  `_warm_one_provider` / `_supervise_provider` (eager-start path).
- Runtime cleanup: `AgentRuntime.shutdown()`.

## Expose side — `AgentServer.serve_mcp()`

The opposite direction: a Murmur agent **becomes** an MCP tool that
Claude Desktop, Cursor, the MCP Inspector, or any other MCP-aware
client invokes. Lives behind the `murmur-ai[mcp-server]` extra so the
SDK only loads when the operator opts in.

```bash
pip install 'murmur-ai[mcp-server]'
```

```python
from murmur import Agent, AgentRuntime, TrustLevel
from murmur.server import AgentServer

researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Answer research questions with cited sources.",
    output_type=Findings,
    trust_level=TrustLevel.MEDIUM,
)

internal_admin = Agent(
    name="db-migrate",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=Plan,
)

runtime = AgentRuntime()
server = AgentServer(runtime=runtime)

# HTTP — both agents reachable here (your auth gates the admin one).
server.register(internal_admin)

# MCP — only researcher enrolled. internal_admin stays invisible to
# MCP clients even though it's registered for HTTP. This is the
# per-agent opt-in tier.
server.register_mcp(
    researcher,
    tool_name="research",
    description="Run a research query against Murmur's researcher agent.",
)

await server.serve_mcp(transport="stdio")    # transport="http" also supported
```

### Two opt-in tiers

| Tier | What you call | Default state |
|---|---|---|
| **Surface** | `await server.serve_mcp(transport=...)` | **off** — constructing `AgentServer` does not start an MCP server. |
| **Per-agent** | `server.register_mcp(agent, ...)` | **off** — `register()` is HTTP-only. An agent registered with `register()` does not appear as an MCP tool. |

The two tiers are independent. Calling `serve_mcp()` on a server with
no MCP enrollments raises `RegistryError` — silently starting an empty
MCP server would mask an operator misconfiguration.

### Tool shape

Each enrolled agent appears to MCP clients as one tool. Today's
shape (intentionally minimal):

- **Name** — the `tool_name=` you pass to `register_mcp` (defaults to `agent.name`).
- **Description** — the `description=` you pass (defaults to the first
  line of `agent.instructions`, truncated to 200 chars).
- **Input schema** — single string parameter `input` matching
  `TaskSpec.input` at the wire level.
- **Output** — `agent.output_type.model_dump()` returned as a dict.

Future iterations will derive structured-input schemas from
`agent.input_type` and expose `AgentGroup` instances as tools too.

### Transports

- `transport="stdio"` — spawned as a subprocess by Claude Desktop /
  Cursor / etc. via their MCP server config. Most desktop clients use
  this.
- `transport="http"` — streamable-HTTP transport per the MCP spec.
  Uses `host=` / `port=` (defaults `127.0.0.1:8765`). Useful when a
  remote service wants to register your Murmur fleet as a hosted MCP
  endpoint.

### Events

Every MCP tool call dispatches through `runtime.run`, so the standard
`AGENT_SPAWNED` / `AGENT_COMPLETED` / `AGENT_FAILED` events fire per
invocation — same observability you have for direct calls.

### Configuring Claude Desktop

Drop a stanza into `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or the equivalent on your platform:

```json
{
  "mcpServers": {
    "murmur-fleet": {
      "command": "uv",
      "args": ["run", "python", "-m", "my_app.mcp_server"]
    }
  }
}
```

Where `my_app.mcp_server` is a small entry-point that constructs the
`AgentServer`, enrolls the agents you want exposed, and calls
`await server.serve_mcp(transport="stdio")`.

## Worked example — two MCP servers, same tool name

Two MCP servers can expose same-named tools; `prefix=` keeps them apart.
`allow=` then operates on the **prefixed** name:

```python
from murmur import Agent
from murmur.tools import mcp_stdio
from murmur.types import TrustLevel

repo_a = mcp_stdio(
    "server-git", ["--repo=A"], prefix="repo_a_",
    allow=["repo_a_status", "repo_a_log"],
)
repo_b = mcp_stdio(
    "server-git", ["--repo=B"], prefix="repo_b_",
    allow=["repo_b_status", "repo_b_log"],
)

agent = Agent(
    name="git-twins",
    model="anthropic:claude-sonnet-4-6",
    instructions="You can inspect two git repos. Be explicit which one.",
    output_type=Out,
    mcp_servers=(repo_a, repo_b),
    trust_level=TrustLevel.LOW,
)
```

The agent now sees four tools: `repo_a_status`, `repo_a_log`,
`repo_b_status`, `repo_b_log`. Without prefixing, both servers' `status`
tools would collide and PydanticAI would reject the agent at construction.

Lifecycle events fire with the prefixed name in the payload — observers
correlate per-server activity from a single `RuntimeEvent` stream.

A runnable variant against the bundled stub server (no third-party
install) lives at
[`examples/mcp.py`](https://github.com/droidnoob/murmur-ai/blob/main/examples/mcp.py).
