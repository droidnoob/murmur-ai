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
expose MCP tools at `LOW`. Decision D17.

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

Decision D25.

## What's enforced where

- Trust gate, allow-list, lifecycle events: `_PolicyMCPToolset` (a
  `WrapperToolset`) routes every MCP call through `ToolExecutor.execute`
  with `external_call=…`.
- Subprocess lifecycle: `AgentRuntime._warm_mcp_providers` /
  `_warm_one_provider` / `_supervise_provider` (eager-start path).
- Runtime cleanup: `AgentRuntime.shutdown()`.

## Expose side — coming

The opposite direction (Murmur agents callable as MCP tools by Claude
Desktop / Cursor / etc.) is tracked in `murmur-ai-6t8`. The cleanest
shape is one MCP server per `AgentServer` with each registered agent
exposed as a tool whose JSON Schema derives from `agent.input_type` and
`agent.output_type`.
