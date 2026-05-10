<p align="center">
  <img src="docs/assets/logo.png" alt="Murmur" width="128" height="128">
</p>

# Murmur

> **Agents that move as one.**

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/downloads/)
[![pydantic-ai](https://img.shields.io/badge/pydantic--ai-1.87-purple)](https://ai.pydantic.dev/)
[![FastStream](https://img.shields.io/badge/faststream-0.6-orange)](https://faststream.ag2.ai/latest/)
[![PyPI](https://img.shields.io/badge/pypi-murmur--ai-blueviolet)](https://pypi.org/project/murmur-ai/)
[![Status](https://img.shields.io/badge/status-pre--alpha-lightgrey)](#status)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Python multi-agent orchestration runtime — *infrastructure* for spawning, distributing, and coordinating LLM-based agents reliably at scale. Strictly typed, broker-agnostic, zero-config to start, distributed when you need it.

📖 **[Documentation →](https://murmur-ai.github.io/murmur/)** · 🧭 **[Concepts →](https://murmur-ai.github.io/murmur/concepts/architecture/)** · 📚 **[API reference →](https://murmur-ai.github.io/murmur/api/)** · 🛠️ **[Contributing →](CONTRIBUTING.md)** · 🔒 **[Security →](SECURITY.md)**

---

## What it is

Murmur is **infrastructure**, not an agent framework. It does not define how an agent thinks — it defines how agents are spawned, distributed, and coordinated. The mental model is a **hypervisor for LLM agents**: spawn it, give it context, get a structured result back, kill it if needed.

Internally, Murmur builds on [PydanticAI](https://ai.pydantic.dev/) for single-agent execution and [FastStream](https://faststream.airt.ai/) for broker-backed distribution — but those are **dependencies, not your imports**. Everything you write is `from murmur import ...`.

The same code runs locally on `asyncio` or distributed across a worker fleet on Kafka / NATS / RabbitMQ / Redis. The agent doesn't change. The workflow doesn't change. Only the runtime constructor changes.

## Key features

- **One unified `Agent` class.** Single Pydantic-frozen spec combines LLM config (model, instructions, output schema, tools, builtin tools) with orchestration config (trust level, context passer, hooks). Wraps PydanticAI internally — users never `import pydantic_ai`. Bidirectional YAML ↔ Python representation.

- **Strict typed I/O.** Every agent input and output is a Pydantic model. No free text crosses agent boundaries. Output validation retries on schema failures; results are typed all the way to your call site via `AgentResult[T]`.

- **Same code, local or distributed.** `AgentRuntime()` runs on `asyncio` (the `AsyncBackend`); `AgentRuntime(broker="kafka://…")` publishes onto a broker for a worker fleet (the `JobBackend`). Both first-class from MVP, both pass the same Protocol contract suite.

- **Distributed worker fleet.** Competing-consumer semantics across Kafka / NATS / RabbitMQ / Redis Streams. Stable consumer ids, `XAUTOCLAIM`-driven reclaim of abandoned pending entries, lifecycle hooks (`on_task_start` / `_complete` / `_error`), heartbeat events on a configurable timer, signed task envelopes for hostile-broker deployments.

- **Multi-agent coordination.** Build typed `AgentGroup` DAGs with `Edge`s, run them with `runtime.run_group()`. Fan-out via `runtime.gather()` with bounded concurrency. LLM-driven dynamic fan-out via the built-in `spawn_agents` tool. Cascading-spawn detection, configurable depth + spawn cap.

- **Tools execute in the runtime, not the agent.** Trust-level enforcement (`HIGH` / `MEDIUM` / `LOW` / `SANDBOX`), allow-list gating, and per-call lifecycle events are uniform regardless of provider. Native Python tools, MCP-discovered tools, and PydanticAI builtin tools (`WebSearchTool`, `CodeExecutionTool`, …) all flow through the same gate.

- **MCP — both sides.** **Consume** any MCP server's tools through the same `tools=` knob (stdio / HTTP / SSE transports). **Expose** an `AgentServer` to MCP clients (Claude Desktop, Cursor, …) so your agents become callable tools. Opt-in per-agent — never auto-on.

- **Observable by default.** Every spawn, completion, tool call, group start/end, worker lifecycle, and budget hit emits a typed `RuntimeEvent`. Composable emitters: `LogEventEmitter` (structlog), `SSEEventEmitter` (HTTP streaming), `MultiEventEmitter` (fan-out), `BrokerEventBridge` (worker → publisher relay). Every event carries `agent_name`, `task_id`, `trace_id`, `parent_trace_id`, `timestamp`.

- **OpenTelemetry metrics export.** Drop-in `OTelMetricsEmitter` records `gen_ai.client.token.usage` and `gen_ai.client.operation.duration` histograms per the OTel GenAI semantic conventions, plus Murmur's own tool-call and rejection counters. Cardinality-safe attributes; your `MeterProvider` decides where the data lands (Datadog, Grafana, Logfire, Phoenix). Opt-in via `murmur-ai[otel]`.

- **Cost-aware orchestration.** `TokenBudget` enforces per-task and per-runtime token ceilings with pre-check + post-charge semantics. Budgets propagate through cascading spawns; over-budget runs raise the typed `BudgetExceededError`. Best-effort USD costs computed from per-model rate cards.

- **HTTP server with REST + SSE.** `murmur serve` exposes the runtime over HTTP: typed `/runs/{id}/result`, `/events/stream` (SSE for live events), composite `/runtime/stats`, plus rollups `/usage` (group by agent / trace / model / none) and `/tools` (per-tool latency percentiles). Mount as a FastAPI router or run standalone.

- **Read-only dashboard.** A React dashboard ships pre-built; mount it at `/dashboard/` off the same server for fleet health, run history, cost-by-model bars, tool-latency tables, and the live event stream. Talks only to the documented HTTP API — no privileged access.

- **Persistent run + event stores.** Optional `RunStore` / `EventStore` Protocols with in-memory, SQLite, RocksDB, and Redis concretes. Survives restarts; powers `/runs/{id}/tree` for the run inspector.

- **Pluggable everywhere.** Backends, context passers, tool providers, routers, event emitters, registries — every pluggable is a `typing.Protocol` first, concrete second. Tests reuse one Protocol-keyed contract suite per Protocol. Bring your own concrete with structural typing; no inheritance required.

- **Fully typed, no exceptions.** Every public function annotated. `ty` (Astral's Rust-based type checker) runs in CI. PEP 561 marker shipped.

- **Migration paths.** Adopt Murmur incrementally — wrap an existing PydanticAI agent with `from_pydantic_ai()`, expose any Murmur agent as a FastStream subscriber via `as_faststream_handler()`. Migration guides for [PydanticAI](https://murmur-ai.github.io/murmur/guides/migration-pydantic-ai/), [FastStream](https://murmur-ai.github.io/murmur/guides/migration-faststream/), and [raw asyncio](https://murmur-ai.github.io/murmur/guides/migration-asyncio/).

## Architecture

A typed pipeline with pluggable stages and composable middleware. Closer to ASGI / Tower than to hexagonal — the work is mostly orchestration of I/O, so the shape that fits is a middleware pipeline, not a domain core with adapters.

```
Task → Router → Context → Tool resolve → Execute → Tool proxy → Validate → Result
                                                       │
                          middleware: cost · timeout · retry · depth limit · observability
```

Backends shipped:

```
AsyncBackend      asyncio  · default · zero-config
JobBackend        FastStream — Kafka / NATS / RabbitMQ / Redis Streams
```

Trust levels gate tool access: `HIGH` (full) · `MEDIUM` (curated) · `LOW` (read-only) · `SANDBOX` (none).

Context passers form a cost/quality ladder:

```
Null → Full → Summary → Selective   (Summary / Selective planned)
```

## Requirements

* Python **3.11** or higher.
* No broker required for local mode (`AsyncBackend`).
* For LLM calls: a provider API key (Anthropic / OpenAI / Gemini / Bedrock / Mistral / OpenRouter / your own OpenAI-compatible endpoint) — whatever PydanticAI supports.

## Install

```bash
pip install murmur-ai            # AsyncBackend, no broker — works immediately
```

Or with [`uv`](https://github.com/astral-sh/uv):

```bash
uv add murmur-ai
```

Optional extras:

| Extra | Pulls in | When |
|---|---|---|
| `murmur-ai[redis]` | `faststream[redis]` | Redis Streams broker |
| `murmur-ai[kafka]` | `faststream[kafka]` | Kafka broker |
| `murmur-ai[nats]` | `faststream[nats]` | NATS broker |
| `murmur-ai[rabbitmq]` | `faststream[rabbit]` | RabbitMQ broker |
| `murmur-ai[all]` | All four brokers | Multi-broker fleet |
| `murmur-ai[server]` | `fastapi`, `uvicorn`, `sse-starlette` | `murmur serve` HTTP API |
| `murmur-ai[otel]` | `opentelemetry-api`, `opentelemetry-sdk` | OTel metrics export |
| `murmur-ai[mcp-server]` | `mcp` | Expose as an MCP server |
| `murmur-ai[sqlite]` | `aiosqlite` | Persistent `RunStore` / `EventStore` |
| `murmur-ai[redis-runstore]` | `redis` | Cluster-wide `RunStore` |
| `murmur-ai[rocksdb]` | `rocksdict` | High-throughput single-host store |
| `murmur-ai[uvloop]` | `uvloop` | Faster async event loop (POSIX only) |
| `murmur-ai[reload]` | `watchfiles` | `--reload` for serve / worker |

## Example

### Create it

```python
import asyncio
from pydantic import BaseModel
from murmur import Agent, AgentRuntime, TaskSpec


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


async def main() -> None:
    runtime = AgentRuntime()
    result = await runtime.run(researcher, TaskSpec(input="What is NATS JetStream?"))
    if result.is_ok():
        finding: ResearchFinding = result.output  # typed
        print(finding.answer, finding.sources)
    else:
        print(result.error)


asyncio.run(main())
```

`result` is `AgentResult[ResearchFinding]` — output is parsed, validated, and typed. Failures land as typed errors (`SpawnError`, `BudgetExceededError`, `ToolExecutionError`, …), never raw `Exception`.

### Fan out

```python
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=20,
)
```

### Coordinate

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
group_result = await runtime.run_group(crew, TaskSpec(input="…"))
```

### Distribute

Same agent. Same `gather()`. Different runtime constructor:

```python
runtime = AgentRuntime(broker="redis://localhost:6379")
results = await runtime.gather(researcher, tasks, max_concurrency=100)
```

A separate `Worker` process consumes the broker:

```bash
murmur worker start --agents researcher --broker redis://localhost:6379 --concurrency 20
```

```python
from murmur.worker import Worker

worker = Worker(broker=broker, agents={"researcher": researcher}, concurrency=20)


@worker.on_task_complete
async def on_complete(task_id: str, agent_name: str, duration_ms: int) -> None:
    print(f"[{task_id}] {agent_name} done in {duration_ms}ms")


await worker.start()
```

The worker's lifecycle, heartbeat, and abandoned-pending-entry recovery are handled by Murmur. See [Distributed deployments](https://murmur-ai.github.io/murmur/guides/distributed/).

### Observe

```python
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

Run `murmur serve --port 8420` and the dashboard, the SSE stream, and the `/usage`, `/tools`, `/runtime/stats` endpoints all light up against the same event source.

## YAML specs

```yaml
version: 1
name: researcher
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"

instructions: |
  Research the question. Cite sources. Be honest about uncertainty.

output_type: my_pkg.outputs.ResearchFinding   # importable class path

tools:
  - web_search
```

```python
runtime.run("researcher", TaskSpec(input="..."))   # resolves from registry
```

YAML and the SDK are bidirectional — every Python `Agent` round-trips through YAML and back.

## CLI

```bash
murmur run script.py                  # run a Python script
murmur validate specs/                # validate YAML specs
murmur worker start --agents X        # start a distributed worker
murmur serve --port 8420              # standalone HTTP server + SSE stream + /dashboard/
murmur status http://host:8420        # tail the live event stream in your terminal
```

## Stack

| Layer        | Choice                                |
| ------------ | ------------------------------------- |
| Language     | Python 3.11 / 3.12 / 3.13             |
| Validation   | Pydantic 2.13                         |
| Agents       | PydanticAI 1.87  *(internal)*         |
| Distribution | FastStream 0.6   *(internal)*         |
| Metrics      | OpenTelemetry 1.39  *(opt-in extra)*  |
| Logging      | structlog 25.5                        |
| Packaging    | uv                                    |
| Lint/Format  | ruff                                  |
| Type check   | ty (Astral)                           |
| Testing      | pytest · pytest-asyncio · hypothesis  |
| Docs         | mkdocs-material + mkdocstrings        |

Versions pinned exactly in `pyproject.toml`. See [`CLAUDE.md`](./CLAUDE.md) §9 for rationale.

## Project layout

```
src/murmur/
    __init__.py        # public API: Agent, AgentRuntime, TaskSpec, TrustLevel,
                       #             AgentResult, AgentGroup, Edge, FanOut, ...
    agent.py           # Agent (wraps PydanticAI internally)
    runtime.py         # AgentRuntime + RuntimeOptions + broker-URL parsing
    types.py           # frozen value types

    core/              # protocols, pipeline, errors  (zero sibling imports)
    context/           # context passers (full · null)
    tools/             # tool registry + executor + MCP factories + builtin re-exports
    backends/          # async (asyncio) · job (broker-backed)
    groups/            # AgentGroup, Edge, runner
    middleware/        # retry · timeout · depth_limit · cost_tracking
    runs/              # RunStore + 4 concretes (in-memory · sqlite · rocksdb · redis)
    events/            # RuntimeEvent + emitters (log · sse · multi · broker-bridge · otel)
    server/            # AgentServer + AgentRouter + REST + SSE
    worker/            # distributed worker with lifecycle hooks
    registry/          # YAML + in-memory spec loaders
    interop/           # from_pydantic_ai · as_faststream_handler
    cli/               # run · validate · worker · serve · status

packages/dashboard/             # React dashboard (separate package)
packages/murmur-client/         # HTTP + LocalClient (separate wheel)

docs/                  # mkdocs site (concepts, guides, API ref)
tests/
```

PyPI distribution: `murmur-ai`. Import: `murmur` (`pip install murmur-ai` → `import murmur`).

The dependency arrow points inward to `core/` and `types.py`. Only `murmur.interop` may import `pydantic_ai` or `faststream`.

## Development

```bash
uv sync --group dev          # install dev tools
uv sync --group docs         # docs build deps (when editing docs/)

uv run ruff check src tests
uv run ruff format src tests
uv run ty check
uv run pytest                # all tests
uv run pytest -m "not integration"   # unit only
uv run pre-commit run --all-files

uv run mkdocs serve          # local docs preview at :8000
```

CI fails on any of: ruff lint, ruff format drift, `ty` errors, failing tests, dead docs links.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for setup and quality gates, and read [`CLAUDE.md`](./CLAUDE.md) — the source of truth for project conventions, architecture, and "do not build" boundaries. Mirrored to [`AGENTS.md`](./AGENTS.md) for non-Claude tooling.

## License

[MIT](LICENSE). Compatible with our MIT (`pydantic-ai`, `pydantic`, `uvloop`) and Apache-2.0 (`faststream`, `mcp`, `httpx`, `opentelemetry-*`) dependencies.
