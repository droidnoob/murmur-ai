# Tools

Runtime-proxied callables exposed to agents under policy.

```python
from murmur.tools import (
    StaticToolProvider,
    ToolExecutor,
    ToolFunc,
    ToolRegistry,
    mcp_http,
    mcp_sse,
    mcp_stdio,
)
from murmur.tools import (
    AbstractBuiltinTool,
    CodeExecutionTool,
    FileSearchTool,
    ImageGenerationTool,
    MCPServerTool,
    MemoryTool,
    WebFetchTool,
    WebSearchTool,
    XSearchTool,
)
```

## Registry

### `ToolRegistry`

::: murmur.tools.ToolRegistry
    options:
      heading_level: 4
      show_bases: false

### `StaticToolProvider`

::: murmur.tools.StaticToolProvider
    options:
      heading_level: 4
      show_bases: false

### `ToolFunc`

::: murmur.tools.ToolFunc
    options:
      heading_level: 4

## Executor

### `ToolExecutor`

::: murmur.tools.ToolExecutor
    options:
      heading_level: 4
      show_bases: false

## MCP factories

Construct `ToolsetProvider` instances backed by the three MCP
transports. See the [MCP concept page](../concepts/mcp.md) for trust
matrix, prefixing, and lifecycle modes.

### `mcp_stdio`

::: murmur.tools.mcp_stdio
    options:
      heading_level: 4

### `mcp_http`

::: murmur.tools.mcp_http
    options:
      heading_level: 4

### `mcp_sse`

::: murmur.tools.mcp_sse
    options:
      heading_level: 4

## Built-in / provider-side tools

These are PydanticAI's `AbstractBuiltinTool` subclasses, re-exported
from `murmur.tools` so users never `import pydantic_ai`. They execute
on the LLM provider's infrastructure — Anthropic web search, OpenAI
code execution, Gemini file search, etc. — and **bypass** the
Murmur `ToolExecutor` by design. See
[Tools — built-in / provider-side](../concepts/tools.md#built-in-provider-side-tools)
for the executor-bypass caveat (decision D24).

| Class | Provider | Notes |
|---|---|---|
| `WebSearchTool` | Anthropic, OpenAI, Gemini, Groq | Native web search; takes `max_uses` and `allowed_domains`. |
| `WebFetchTool` | Anthropic | Fetch a URL and add to the conversation; takes `max_uses`. |
| `CodeExecutionTool` | Anthropic, OpenAI, Gemini, Groq | Provider-side sandboxed Python execution. |
| `FileSearchTool` | OpenAI, Gemini | Search uploaded files / vector store. |
| `ImageGenerationTool` | OpenAI, Gemini, Groq | Generate images inline. |
| `MemoryTool` | Anthropic | Persistent memory across conversation turns. |
| `MCPServerTool` | OpenAI | Provider-managed MCP servers (distinct from Murmur's MCP consume side). |
| `XSearchTool` | Grok / xAI | X (Twitter) search. |
| `AbstractBuiltinTool` | – | Base class. Use the concrete subclasses; this is the common type for `Agent.builtin_tools`. |

```python
from murmur import Agent
from murmur.tools import WebSearchTool, CodeExecutionTool

agent = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=Out,
    builtin_tools=(
        WebSearchTool(max_uses=10),
        CodeExecutionTool(),
    ),
)
```

For the per-class kwargs, see PydanticAI's
[built-in tools docs](https://ai.pydantic.dev/builtin-tools/).
