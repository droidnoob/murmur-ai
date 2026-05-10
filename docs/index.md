---
hide:
  - navigation
---

<p align="center">
  <img src="assets/logo.png" alt="Murmur" width="128" height="128">
</p>

<p align="center"><em>Agents that move as one.</em></p>

<p align="center">
<a href="https://pypi.org/project/murmur-ai/"><img src="https://img.shields.io/pypi/v/murmur-ai.svg" alt="PyPI"></a>
<a href="https://pypi.org/project/murmur-ai/"><img src="https://img.shields.io/pypi/pyversions/murmur-ai.svg" alt="Python versions"></a>
<a href="https://github.com/murmur-ai/murmur/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

---

**Documentation:** [https://murmur-ai.github.io/murmur/](https://murmur-ai.github.io/murmur/)

**Source:** [https://github.com/murmur-ai/murmur](https://github.com/murmur-ai/murmur)

---

**Murmur** is a Python multi-agent orchestration runtime — *infrastructure*
for spawning, distributing, and coordinating LLM-based agents reliably at
scale. Think of it as a **hypervisor for LLM agents**: spawn it, give it
context, get a structured result back, kill it if needed.

[PydanticAI](https://ai.pydantic.dev/) handles single-agent execution.
[FastStream](https://faststream.airt.ai/) handles broker-backed distribution.
Murmur owns the orchestration layer between them — and hides both behind
its own public API.

The same code runs locally on `asyncio` or distributed across a worker
fleet on Kafka / NATS / RabbitMQ / Redis. The agent doesn't change. The
workflow doesn't change. Only the runtime constructor changes.

## Key features

- **One unified `Agent` class.** Single Pydantic-frozen spec combines LLM
  config (model, instructions, output schema, tools, builtin tools) with
  orchestration config (trust level, context passer, hooks). Wraps
  PydanticAI internally — users never `import pydantic_ai`. Bidirectional
  YAML ↔ Python representation. [Learn more →](concepts/agents.md)

- **Strict typed I/O.** Every agent input and output is a Pydantic model.
  No free text crosses agent boundaries. Output validation retries on
  schema failures; parsed results are typed all the way to your call site
  via `AgentResult[T]`.

- **Same code, local or distributed.** `AgentRuntime()` runs on
  `asyncio` (the `AsyncBackend`); `AgentRuntime(broker="kafka://…")`
  publishes onto a broker for a worker fleet (the `JobBackend`). Both
  first-class from the MVP, both pass the same Protocol contract suite.
  [Learn more →](concepts/backends.md)

- **Distributed worker fleet.** First-class `Worker` class with
  competing-consumer semantics across Kafka / NATS / RabbitMQ / Redis
  Streams. Stable consumer ids, `XAUTOCLAIM`-driven reclaim of abandoned
  pending entries, lifecycle hooks (`on_task_start` / `_complete` /
  `_error`), heartbeat events on a configurable timer.
  [Learn more →](guides/distributed.md)

- **Multi-agent coordination.** Build typed `AgentGroup` DAGs with
  `Edge`s, run them with `runtime.run_group()`. Fan-out via
  `runtime.gather()` with bounded concurrency. LLM-driven dynamic fan-out
  via the built-in `spawn_agents` tool. Cascading-spawn detection,
  configurable depth + spawn cap. [Learn more →](concepts/coordination.md)

- **Tools execute in the runtime, not the agent.** Trust-level
  enforcement (`HIGH` / `MEDIUM` / `LOW` / `SANDBOX`), allow-list gating,
  and per-call lifecycle events are uniform regardless of provider.
  Native Python tools, MCP-discovered tools, and PydanticAI builtin
  tools (`WebSearchTool`, `CodeExecutionTool`, etc.) all flow through
  the same gate. [Learn more →](concepts/tools.md)

- **MCP — both sides.** **Consume** any MCP server's tools through the
  same `tools=` knob (stdio / HTTP / SSE transports). **Expose** an
  `AgentServer` to MCP clients (Claude Desktop, Cursor, …) so your
  agents become callable tools. Opt-in per-agent — never auto-on.
  [Learn more →](concepts/mcp.md)

- **Observable by default.** Every spawn, completion, tool call, group
  start/end, and budget hit emits a typed `RuntimeEvent`. Composable
  emitters: `LogEventEmitter` (structlog), `SSEEventEmitter` (HTTP
  streaming), `MultiEventEmitter` (fan-out), `BrokerEventBridge`
  (worker → publisher relay). Every event carries `agent_name`,
  `task_id`, `trace_id`, `parent_trace_id`, `timestamp`.
  [Learn more →](concepts/events.md)

- **OpenTelemetry metrics export.** Drop-in `OTelMetricsEmitter`
  records `gen_ai.client.token.usage` and
  `gen_ai.client.operation.duration` histograms per the OTel GenAI
  semantic conventions, plus Murmur's own tool-call and rejection
  counters. Cardinality-safe attributes. Murmur stays out of exporter
  config — your `MeterProvider` decides where the data lands (Datadog,
  Grafana, Logfire, Phoenix, …). Opt-in via `murmur-ai[otel]`.
  [Learn more →](concepts/events.md#otelmetricsemitter)

- **Cost-aware orchestration.** `TokenBudget` enforces per-task and
  per-runtime token ceilings with pre-check + post-charge semantics.
  Budgets propagate through cascading spawns; over-budget runs raise
  the typed `BudgetExceededError`. Best-effort USD costs computed from
  per-model rate cards. [Learn more →](concepts/cost.md)

- **HTTP server with REST + SSE.** `murmur serve` exposes the runtime
  over HTTP: typed `/runs/{id}/result`, `/events/stream` (SSE for live
  events), composite `/runtime/stats`, plus rollups `/usage` (group by
  agent / trace / model / none) and `/tools` (per-tool latency
  percentiles). Mount as a FastAPI router or run standalone.

- **Read-only dashboard.** A React dashboard ships pre-built; mount it
  at `/dashboard/` off the same server for fleet health, run history,
  cost-by-model bars, tool-latency tables, and the live event stream.
  Talks only to the documented HTTP API — no privileged access.

- **Persistent run + event stores.** Optional `RunStore` /
  `EventStore` Protocols with in-memory, SQLite, RocksDB, and Redis
  concretes. Survives restarts; powers `/runs/{id}/tree` for the run
  inspector.

- **Pluggable everywhere.** Backends, context passers, tool providers,
  routers, event emitters, registries — every pluggable is a
  `typing.Protocol` first, concrete second. Tests reuse one
  Protocol-keyed contract suite per Protocol. Bring your own concrete
  with structural typing; no inheritance required.
  [Learn more →](concepts/architecture.md)

- **Fully typed, no exceptions.** Every public function annotated.
  `ty` (Astral's Rust-based type checker) runs in CI. `Any` requires a
  comment. `# type: ignore` is banned in favour of rule-named
  `# ty: ignore[<rule>]`. PEP 561 marker shipped.

- **PydanticAI / FastStream / asyncio migration.** Adopt Murmur
  incrementally — wrap an existing PydanticAI agent with
  `from_pydantic_ai()`, expose any Murmur agent as a FastStream
  subscriber via `as_faststream_handler()`. Migration guides for
  [PydanticAI](guides/migration-pydantic-ai.md),
  [FastStream](guides/migration-faststream.md), and
  [raw asyncio](guides/migration-asyncio.md).

## Requirements

* Python **3.11** or higher.
* No broker required for local mode (`AsyncBackend`). Add a broker
  extra when you go distributed.
* For LLM calls: a provider API key (Anthropic / OpenAI / Gemini /
  Bedrock / Mistral / OpenRouter / your own OpenAI-compatible
  endpoint) — whatever PydanticAI supports, Murmur supports.

## Installation

<!-- termynal -->

```bash
pip install murmur-ai
```

The base install ships `AsyncBackend` (asyncio), the typed runtime, the
event system, and the cost-tracking middleware — no broker required.
Add extras as you grow:

| Extra | Pulls in | When |
|---|---|---|
| `murmur-ai[redis]` | `faststream[redis]` | Redis Streams broker |
| `murmur-ai[kafka]` | `faststream[kafka]` | Kafka broker |
| `murmur-ai[nats]` | `faststream[nats]` | NATS broker |
| `murmur-ai[rabbitmq]` | `faststream[rabbit]` | RabbitMQ broker |
| `murmur-ai[all-brokers]` | All four brokers | Multi-broker fleet |
| `murmur-ai[server]` | `fastapi`, `uvicorn`, `sse-starlette` | `murmur serve` HTTP API |
| `murmur-ai[otel]` | `opentelemetry-api`, `opentelemetry-sdk` | OTel metrics export |
| `murmur-ai[mcp-server]` | `mcp` | Expose as an MCP server |
| `murmur-ai[sqlite]` | `aiosqlite` | Persistent `RunStore` / `EventStore` |
| `murmur-ai[uvloop]` | `uvloop` | Faster async event loop (POSIX only) |
| `murmur-ai[reload]` | `watchfiles` | `--reload` for serve / worker |
| `murmur-ai[all]` | Every optional extra | Kitchen-sink install |

See [Installation](getting-started/installation.md) for the full table.

## Example

### Create it

Define an agent with a typed output schema, then run it:

```python
from murmur import Agent, AgentRuntime, TaskSpec
from pydantic import BaseModel


class ResearchFinding(BaseModel):
    question: str
    answer: str
    confidence: float
    sources: list[str]


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Research the question. Cite sources. Be honest about uncertainty.",
    output_type=ResearchFinding,
)

runtime = AgentRuntime()
result = await runtime.run(researcher, TaskSpec(input="What is NATS JetStream?"))

if result.is_ok():
    finding: ResearchFinding = result.output  # typed
    print(finding.answer, finding.sources)
else:
    print(result.error)
```

`result` is `AgentResult[ResearchFinding]` — the output is parsed,
validated, and typed. Failures land as typed errors (`SpawnError`,
`BudgetExceededError`, `ToolExecutionError`, …), never raw `Exception`.

### Fan out

The same agent across many tasks with bounded concurrency:

```python
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=20,
)
```

`gather` returns `list[AgentResult[ResearchFinding]]`, one per task,
each independently checkable with `result.is_ok()`.

### Coordinate

Build a typed multi-agent DAG and run it:

```python
from murmur import AgentGroup, Edge

crew = AgentGroup(
    name="research-crew",
    agents={
        "researcher": researcher,
        "fact_checker": fact_checker,
        "summariser": summariser,
    },
    edges=[
        Edge("researcher", "fact_checker"),
        Edge("fact_checker", "summariser"),
    ],
)

group_result = await runtime.run_group(
    crew,
    TaskSpec(input="What is NATS JetStream?"),
)
```

### Distribute

Same agent. Same `gather()`. Different runtime constructor:

```python
runtime = AgentRuntime(broker="redis://localhost:6379")

results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)
```

The publisher's `runtime.gather()` publishes tasks onto Redis Streams.
A separate `Worker` process — possibly a fleet of them — consumes and
processes:

```bash
murmur worker start --agents researcher --broker redis://localhost:6379 --concurrency 20
```

The worker's lifecycle, heartbeat, and abandoned-pending-entry recovery
are handled by Murmur. See
[Distributed deployments](guides/distributed.md).

### Observe

Every action emits a typed `RuntimeEvent`. Compose emitters:

```python
from murmur import AgentRuntime
from murmur.events import (
    LogEventEmitter,
    MultiEventEmitter,
    OTelMetricsEmitter,
    SSEEventEmitter,
)

sse = SSEEventEmitter()
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([
        LogEventEmitter(),     # structlog INFO/ERROR
        sse,                   # /events/stream HTTP feed
        OTelMetricsEmitter(),  # gen_ai.* histograms to your OTel backend
    ]),
)
```

Run `murmur serve --port 8420` and the dashboard, the SSE stream, the
`/usage`, `/tools`, and `/runtime/stats` endpoints all light up against
the same event source.

## Recap

In summary, you declare an agent **once** — its model, instructions,
typed output schema, tool allow-list, and trust level — and Murmur
gives you:

* A typed `AgentResult[T]` from `runtime.run()`.
* Bounded fan-out via `runtime.gather()`.
* Multi-stage DAGs via `AgentGroup` + `Edge` + `runtime.run_group()`.
* Distributed execution by changing **one constructor argument**.
* A worker fleet that handles heartbeats, abandoned-PEL recovery, and
  graceful drain.
* A typed event stream feeding your logs, dashboards, and OTel
  backend simultaneously.
* Cost ceilings, depth limits, and trust-level tool gates enforced at
  the runtime — not relied on per-agent.

Everything else (broker concretes, context passers, tool providers,
event emitters, run stores) is a `typing.Protocol` you can swap.

## Where next

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Quickstart**

    ---

    Five minutes from empty directory to a structured agent answer.

    [:octicons-arrow-right-24: Quickstart](getting-started/quickstart.md)

-   :material-book-open-page-variant:{ .lg .middle } **Concepts**

    ---

    How agents, runtimes, backends, tools, coordination, observability,
    cost, and MCP fit together.

    [:octicons-arrow-right-24: Concepts](concepts/architecture.md)

-   :material-server:{ .lg .middle } **Distributed**

    ---

    Worker fleet, broker URLs, signed envelopes, abandoned-PEL recovery.

    [:octicons-arrow-right-24: Distributed deployments](guides/distributed.md)

-   :material-api:{ .lg .middle } **API reference**

    ---

    Every public symbol, auto-generated from the docstrings.

    [:octicons-arrow-right-24: API reference](api/index.md)

</div>
