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
| `max_concurrent_requests` | `int \| None` | Per-agent cap on concurrent provider HTTP requests. |
| `model_concurrency_limiter` | `AbstractConcurrencyLimiter \| None` | Shared limiter across agents. Mutually exclusive with `max_concurrent_requests`. |
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
target is a `BaseModel` subclass.

## Tools

Two tool surfaces, with different policy implications:

- **Native tools** (`tools=(…)`): registered in the runtime's
  `ToolRegistry`, executed inside the runtime, gated by `ToolExecutor`,
  emit `TOOL_CALL_*` events. See [Tools](tools.md).
- **Built-in / provider-side tools** (`builtin_tools=(…)`): execute on
  the LLM provider's infrastructure (Anthropic web search, OpenAI code
  exec, etc.). They **bypass** `ToolExecutor` by design — Murmur can't
  intercept what's not proxied through it. Tokens still count toward
  `TokenBudget` because `usage()` includes provider-side spend.

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

## Capping provider HTTP concurrency

`AgentRuntime.gather(max_concurrency=…)` caps how many Murmur tasks fan
out at once. That's an orchestration concern. A separate concern: many
agents sharing one API key can blow past the provider's RPM cap even
when each individual `gather` is well-behaved. For that, cap **at the
model level**.

Per-agent cap (one limiter per agent, not shared):

```python
agent = Agent(
    name="researcher",
    model="openai:gpt-5.2",
    max_concurrent_requests=5,   # ≤5 in-flight HTTP requests for this agent
    instructions="...",
    output_type=Out,
)
```

Shared cap across agents (one limiter object, threaded into each agent):

```python
from murmur.models import ConcurrencyLimiter

pool = ConcurrencyLimiter(max_running=10, name="openai-pool")

head   = Agent(name="head",   model="openai:gpt-5.2", model_concurrency_limiter=pool, …)
minion = Agent(name="minion", model="openai:gpt-5.2", model_concurrency_limiter=pool, …)
```

`max_concurrent_requests` and `model_concurrency_limiter` are mutually
exclusive — pick one. At dispatch the runtime wraps the resolved model
in `pydantic_ai.models.concurrency.ConcurrencyLimitedModel` *outside*
any `FallbackModel`, so one limiter slot covers the whole run regardless
of which fallback ultimately served the request.

Single-process by default. For cross-process limiting (e.g. one shared
cap across a worker fleet), pass a custom
`AbstractConcurrencyLimiter` subclass — e.g. a Redis-backed one — to
`model_concurrency_limiter`. PydanticAI emits OpenTelemetry spans
showing queue depth and configured limits while waiting for a slot,
so observability is automatic.

## Templates — shared config across a fleet

When several agents share a base prompt, model, or trust level, lift
the shared bits into an `AgentTemplate` and materialise concrete agents
from it. The template is a frozen builder — pure data, no dispatch
impact.

```python
from murmur import Agent, AgentTemplate
from murmur.types import TrustLevel

swarm = AgentTemplate(
    pre_instruction="You are part of an automated pipeline. JSON only. Never apologise.",
    model="anthropic:claude-sonnet-4-6",
    trust_level=TrustLevel.MEDIUM,
    tools=frozenset({"web_search"}),
)

researcher = swarm.agent(
    name="researcher",
    instructions="Find verifiable facts about the topic.",
    output_type=Findings,
)
checker = swarm.agent(
    name="checker",
    instructions="Verify each claim.",
    output_type=Verdict,
)
```

`pre_instruction` prepends every materialised agent's `instructions`
with a blank line between (`pre_instruction + "\n\n" + instructions`).
Per-call kwargs override template defaults; `None` means "inherit from
template". Collection fields (`tools`, `mcp_servers`, `builtin_tools`,
`fallback_models`) **replace** rather than extend — build a union
explicitly when you want both:

```python
specialist = swarm.agent(
    name="specialist",
    instructions="...",
    output_type=Out,
    tools=swarm.tools | frozenset({"calculator"}),
)
```

The template also constrains LLM-driven dynamic spawning via the
[`spawn_agents` tool](#llm-driven-fan-out-with-spawn_agents) — see below.

## LLM-driven fan-out with `spawn_agents`

`make_spawn_agents_tool` returns a tool callable that the LLM invokes
mid-run to delegate work to child agents in parallel. The factory binds
a runtime, a template (the safety envelope), and a shared
`output_type`; the LLM picks `name` / `instructions` / `input` per
child and nothing else. Trust level, model, and tool surface come from
the template, so the LLM cannot escalate.

```python
from murmur import AgentRuntime, AgentTemplate, TrustLevel
from murmur.tools import make_spawn_agents_tool

runtime = AgentRuntime()

swarm = AgentTemplate(
    pre_instruction="You are part of an automated research pipeline. JSON only.",
    model="anthropic:claude-sonnet-4-6",
    trust_level=TrustLevel.MEDIUM,
    tools=frozenset({"web_search"}),
)

spawn = make_spawn_agents_tool(
    runtime=runtime,
    template=swarm,
    output_type=Finding,        # all children share this output shape
    max_concurrency=5,
)
runtime.tools.register("spawn_agents", spawn)

orchestrator = swarm.agent(
    name="orchestrator",
    instructions="Decompose the task; call spawn_agents to delegate; aggregate the findings.",
    output_type=FinalReport,
    tools=frozenset({"spawn_agents"}),     # only the orchestrator gets the tool
)
runtime.register(orchestrator)
```

When the orchestrator runs, the LLM calls `spawn_agents([{name, instructions, input}, …])`;
each child is materialised through the template, dispatched via
`runtime.run`, and the per-child outcomes come back as a
`list[SpawnResult]` for the orchestrator to aggregate. Per-child
failures are captured into `SpawnResult(success=False, error=…)` rather
than raised — partial fan-outs always return.

Don't add `spawn_agents` to the template's tool surface — register it
explicitly only on the orchestrator's per-agent `tools=` set. A child
that also has the tool can in principle recurse, and cascading-depth
enforcement isn't shipped yet.

Events fire normally: `TOOL_CALL_STARTED` / `_COMPLETED` on the
orchestrator's spawn call, and `AGENT_SPAWNED` + `AGENT_COMPLETED` (or
`_FAILED`) per child. There's no `parent_trace_id` linkage from child
events back to the orchestrator's run yet — children appear as
independent top-level runs in the event stream.

## Trust levels

| Level | Tools | When to use |
|---|---|---|
| `HIGH` | Full tool access | Code you wrote, executing in your trust boundary. |
| `MEDIUM` | Curated set | Default for production agents. |
| `LOW` | Read-only allowlist | Agents processing untrusted input. MCP requires explicit `allow=[...]`. |
| `SANDBOX` | None | Pure reasoning; no I/O. A future release will pin SANDBOX agents to `ContainerBackend`. |

The full enforcement matrix is queued. Today: MCP gating and tool
allow-listing are enforced; backend choice and cascading-spawn cycle
detection are not yet shipped.
