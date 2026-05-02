# Architecture

Murmur is a **typed pipeline with pluggable stages and composable
middleware**. Each stage has a clear responsibility, a typed input/output
contract, and can be swapped without touching the others. Middleware
wraps the pipeline (or specific stages) for cross-cutting concerns —
retry, timeout, depth limit, cost tracking, observability.

The mental model: **a hypervisor for LLM agents.** Spawn it, give it
context, get a structured result back, kill it if needed.

## The pipeline

```
Task → Router → Context → Tool resolve → Execute → Tool proxy → Validate → Result
                                                       │
                          middleware: cost · timeout · retry · depth limit · observability
```

Every stage receives the pipeline context and a reference to the next
stage. It can mutate context before forwarding, transform the result on
the way back, short-circuit, or wrap in `try/except` for stage-local
error handling.

```python
from collections.abc import Awaitable, Callable
from typing import Protocol


class Stage(Protocol):
    async def __call__(
        self,
        context: PipelineContext,
        next_stage: Callable[[PipelineContext], Awaitable[AgentResult]],
    ) -> AgentResult: ...
```

## Public API rule

> Users **never** import from `pydantic_ai` or `faststream` directly.
> Everything is `from murmur import ...`.

`murmur.Agent` wraps PydanticAI internally — you get one unified class
that combines model config (model, instructions, output_type, tools)
with orchestration config (trust_level, context_passer, mcp_servers).

`murmur.AgentRuntime` accepts a broker URL string (`kafka://…`,
`nats://…`, `amqp://…`, `redis://…`) and constructs FastStream brokers
internally. You never see `KafkaBroker`.

PydanticAI and FastStream are dependencies, not public API. Migration
adapters live in [`murmur.interop`](../guides/migration-pydantic-ai.md) — that's the
only place allowed to import them.

## Protocols-first

Every pluggable component is a `typing.Protocol` first, concrete second.
The Protocol is written before any implementation. Core never imports
concrete implementations.

| Protocol | Concretes |
|---|---|
| `Backend` | `AsyncBackend`, `JobBackend` |
| `ContextPasser` | `NullContextPasser`, `FullContextPasser` |
| `ToolProvider` | `StaticToolProvider` |
| `ToolsetProvider` | `MCPToolsetProvider` |
| `EventEmitter` | `LogEventEmitter`, `SSEEventEmitter`, `MultiEventEmitter`, `BrokerEventBridge` |
| `Registry` | `InMemoryRegistry`, `YamlRegistry` |
| `RunStore` | `InMemoryRunStore`, `SQLiteRunStore`, `RocksDBRunStore`, `RedisRunStore` |
| `Worker` | `Worker` |

Tests are written against the Protocol; every concrete is run through
the **same shared contract suite** (e.g. `BackendContract`,
`RunStoreContract`, `EventEmitterContract`).

## Execution backends

```
AsyncBackend     ← asyncio.create_task — lightweight, default, zero-config
JobBackend        ← FastStream subscriber/publisher (Kafka / NATS / RabbitMQ / Redis)
ContainerBackend  ← Docker SDK — full isolation for untrusted context  (queued)
```

`AsyncBackend` and `JobBackend` are both first-class. `JobBackend`
activates when you pass a broker URL.

## Tool execution flow

```
Agent → tool_call(name, args)
            ↓
        Runtime intercepts (never agent-side)
            ↓
        Enforce policy (allowed? rate limited? budgeted?)
            ↓
        Execute (in runtime, with logging)
            ↓
        Return result to agent
```

Tools execute **inside the runtime, not inside the agent.** This means
trust enforcement, rate limiting, and observability are uniform — there's
one chokepoint, not N. Same pattern applies to MCP-discovered tools (see
[MCP](mcp.md)) — they flow through `ToolExecutor.execute` with
`external_call=…` and emit the identical lifecycle events.

## Trust levels

```python
class TrustLevel(StrEnum):
    HIGH    = "high"     # full tool access
    MEDIUM  = "medium"   # curated tool set
    LOW     = "low"      # read-only tools
    SANDBOX = "sandbox"  # no tools, pure reasoning
```

Today the gate is enforced for native tools and MCP toolsets. The full
enforcement matrix (`SANDBOX` agents always run via `ContainerBackend`
regardless of caller's request, etc.) and cascading-spawn controls are
queued for a future release.
