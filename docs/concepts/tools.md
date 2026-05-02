# Tools

Tools execute **inside the runtime, not inside the agent.** The agent
requests a tool call; the runtime enforces policy, executes, logs, and
returns the result. This is the chokepoint that makes trust enforcement,
rate limiting, and observability uniform.

## Tool flow

```
Agent → tool_call(name, args)
            ↓
        Runtime intercepts
            ↓
        ToolExecutor.execute(name, args, agent=…, external_call=…)
            ↓
        Resolve from registry
            ↓
        Trust gate (allowed for this trust_level?)
            ↓
        Lifecycle event: TOOL_CALL_STARTED
            ↓
        Execute (with logging)
            ↓
        Lifecycle event: TOOL_CALL_COMPLETED or TOOL_CALL_FAILED
            ↓
        Return result to agent
```

## Defining a tool

```python
from murmur.tools import StaticToolProvider, ToolFunc, ToolRegistry


async def web_search(query: str) -> list[dict[str, str]]:
    """Search the web. Returns a list of {title, url, snippet}."""
    ...


registry = ToolRegistry()
registry.register("web_search", web_search)
```

`ToolFunc[T]` is generic so user typing survives the call site;
`ToolRegistry.register` is generic over `T`. Storage erases to
`ToolFunc[Any]` after registration (commented why, per CLAUDE.md §20).

## Tool providers

`ToolProvider` is the Protocol that resolves an agent's allowed tools at
dispatch time. Today's concrete is `StaticToolProvider`; Phase 3 adds
`RoleBasedToolProvider` (issue `murmur-ai-5cg`) and `DenylistToolProvider`
(issue `murmur-ai-0fh`).

```python
class ToolProvider(Protocol):
    def resolve(self, agent: Agent) -> Mapping[str, ToolFunc]: ...
```

## ToolExecutor

`ToolExecutor` is the chokepoint:

```python
class ToolExecutor:
    async def execute(
        self,
        name: str,
        args: dict[str, object],
        *,
        agent: Agent,
        external_call: Callable[..., Awaitable[object]] | None = None,
    ) -> object: ...
```

When `external_call` is provided (e.g. an MCP tool), the executor still
applies the trust gate, emits the lifecycle events, and routes the call —
but delegates execution to the supplied callable. This is how
[MCP-discovered tools](mcp.md) get the same observability and policy
enforcement as native tools.

## Trust gate

| Level | Native tools | MCP tools |
|---|---|---|
| `HIGH` | All registered | All exposed unless `allow=` narrows |
| `MEDIUM` | All registered | All exposed unless `allow=` narrows |
| `LOW` | Read-only allowlist | **Requires** explicit `allow=[…]` per server |
| `SANDBOX` | None | Skipped entirely |

The matrix is enforced for MCP today. Native-tool enforcement is partial;
the full matrix lands in `murmur-ai-001`.

## Built-in / provider-side tools

`agent.builtin_tools` accepts PydanticAI's `AbstractBuiltinTool`
subclasses (`WebSearchTool`, `CodeExecutionTool`, `ImageGenerationTool`,
`WebFetchTool`, `FileSearchTool`, `MemoryTool`, `MCPServerTool`,
`XSearchTool`). They execute on the LLM provider's infrastructure, not
on Murmur's runtime — and therefore **bypass** `ToolExecutor`.

```python
from murmur import Agent
from murmur.tools import WebSearchTool

agent = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=Out,
    builtin_tools=(WebSearchTool(),),
)
```

Tokens used by built-in tools still count toward `TokenBudget` — PydanticAI's
`usage()` includes provider-side spend, and the cost middleware reads from
that. Trust gating and per-tool events do **not** apply, by design — Murmur
can't intercept what's not proxied through it. Decision D24.

## Lifecycle events

Every native + MCP-proxied tool call emits:

| Event | When |
|---|---|
| `TOOL_CALL_STARTED` | Before dispatch. |
| `TOOL_CALL_COMPLETED` | After successful return. |
| `TOOL_CALL_FAILED` | After exception. Routed to `aerror` in the default emitter. |

See [Events](events.md) for emitter wiring.
