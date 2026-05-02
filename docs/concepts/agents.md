# Agents

`murmur.Agent` is the **single unified class** that wraps PydanticAI
internally. It combines LLM config (model, instructions, output_type,
tools) with Murmur orchestration config (trust_level, context_passer,
mcp_servers) on the same object. There is no separate `AgentSpec` +
PydanticAI agent — the Agent *is* the spec.

```python
from murmur import Agent
from murmur.context import NullContextPasser
from murmur.types import TrustLevel
from pydantic import BaseModel


class MinionFinding(BaseModel):
    question: str
    answer: str
    confidence: float
    sources: list[str]


minion = Agent(
    name="research-minion",
    model="anthropic:claude-sonnet-4-6",
    instructions="You are a research minion ...",
    output_type=MinionFinding,
    tools=("web_search",),
    trust_level=TrustLevel.MEDIUM,
    context_passer=NullContextPasser(),
)
```

`Agent` is a frozen Pydantic value object. Update via `model_copy`:

```python
hardened = minion.model_copy(update={"trust_level": TrustLevel.LOW})
```

## Fields

| Field | Type | Notes |
|---|---|---|
| `name` | `str` | Identifier in the registry; used in logs/events. |
| `model` | `str` | PydanticAI model string, e.g. `"anthropic:claude-sonnet-4-6"`. |
| `instructions` | `str` | System prompt. |
| `output_type` | `type[BaseModel]` | Validated Pydantic schema for the agent's output. |
| `input_type` | `type[BaseModel] \| None` | Optional input validation; `TaskSpec.input` validated against this. |
| `tools` | `tuple[str, ...]` | Native tool names registered in the runtime's `ToolRegistry`. |
| `mcp_servers` | `tuple[ToolsetProvider, ...]` | MCP servers exposing tools. See [MCP](mcp.md). |
| `builtin_tools` | `tuple[AbstractBuiltinTool, ...]` | Provider-side tools (web search, code exec, etc.) — execute on the LLM provider's infra. |
| `fallback_models` | `tuple[str, ...]` | Ordered fallbacks; wrapped as `FallbackModel` at dispatch. |
| `model_settings` | `Mapping[str, object] \| None` | Provider knobs (temperature, max_tokens, …). Copied to `dict` at the boundary. |
| `trust_level` | `TrustLevel` | `HIGH` / `MEDIUM` / `LOW` / `SANDBOX`. |
| `context_passer` | `ContextPasser` | Policy for what context flows into a spawn. |
| `pre_process` | `tuple[Callable, ...]` | Sync, pure hooks running inside the agent run boundary. |
| `post_process` | `tuple[Callable, ...]` | Sync, pure hooks; same shape as `pre_process`. |

## YAML

The Python and YAML forms are two representations of the same canonical
spec. Bidirectional. Runtime doesn't care which the user picked.

```yaml
version: 1
name: research-minion
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"

instructions: |
  You are a research minion ...

output_type: my_pkg.outputs.MinionFinding   # importable class path
tools:
  - web_search
```

`output_type` and `input_type` are **importable class paths** — the YAML
loader runs `importlib.import_module` + `getattr` and validates that the
target is a `BaseModel` subclass. Decision D4.

## Tools

Two tool surfaces, with different policy implications:

- **Native tools** (`tools=(…)`): registered in the runtime's
  `ToolRegistry`, executed inside the runtime, gated by `ToolExecutor`,
  emit `TOOL_CALL_*` events. See [Tools](tools.md).
- **Built-in / provider-side tools** (`builtin_tools=(…)`): execute on
  the LLM provider's infrastructure (Anthropic web search, OpenAI code
  exec, etc.). They **bypass** `ToolExecutor` by design — Murmur can't
  intercept what's not proxied through it. Tokens still count toward
  `TokenBudget` because `usage()` includes provider-side spend. Decision D24.

## Fallback models

```python
agent = Agent(
    name="resilient",
    model="anthropic:claude-sonnet-4-6",
    fallback_models=(
        "anthropic:claude-haiku-4-5",
        "openai:gpt-4o-mini",
    ),
    instructions="...",
    output_type=Out,
)
```

When `fallback_models` is non-empty, dispatch wraps the primary in
`pydantic_ai.models.fallback.FallbackModel` with the default
`(ModelAPIError,)` trigger. v1 = ordered model strings.
`agent.model_settings` is shared across primary + every fallback for now.
Decision D26.

## Trust levels

| Level | Tools | When to use |
|---|---|---|
| `HIGH` | Full tool access | Code you wrote, executing in your trust boundary. |
| `MEDIUM` | Curated set | Default for production agents. |
| `LOW` | Read-only allowlist | Agents processing untrusted input. MCP requires explicit `allow=[...]`. |
| `SANDBOX` | None | Pure reasoning; no I/O. Phase 4 also enforces ContainerBackend. |

The full enforcement matrix lands in Phase 4. Today: MCP gating and tool
allow-listing are enforced; backend choice and cascading-spawn cycle
detection are queued for `murmur-ai-001` / `murmur-ai-jip`.
