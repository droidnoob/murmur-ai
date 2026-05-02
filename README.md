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

A Python multi-agent orchestration runtime. Strictly typed, broker-agnostic, zero-config to start, distributed when you need it.

📖 **[Documentation →](https://murmur-ai.github.io/murmur/)** · 🧭 **[Concepts →](https://murmur-ai.github.io/murmur/concepts/architecture/)** · 📚 **[API reference →](https://murmur-ai.github.io/murmur/api/)** · 🛠️ **[Contributing →](CONTRIBUTING.md)** · 🔒 **[Security →](SECURITY.md)**

---

## What it is

Murmur is **infrastructure**, not an agent framework. It does not define how an agent thinks — it defines how agents are spawned, distributed, and coordinated. The mental model is a **hypervisor** for LLM agents: spawn it, give it context, get a structured result back, kill it if needed.

Internally, Murmur builds on [PydanticAI](https://ai.pydantic.dev/) for single-agent execution and [FastStream](https://faststream.airt.ai/) for broker-backed distribution — but those are **dependencies, not your imports**. Everything you write is `from murmur import ...`.

## Why

Most agent runtimes either lock you into one execution model (in-process only, or one specific broker) or let agents call tools directly with no policy layer. Murmur splits the concerns: PydanticAI owns single-agent execution, Murmur owns the orchestration — fan-out, routing, context passing, tool policy, trust levels, cost controls, observability — so the **same agent runs on a laptop or a Kafka cluster without changes.** You never construct a broker. You hand the runtime a URL.

## Design principles

- **Pluggable everything, sensible defaults.** Override only what you care about.
- **Strict I/O contracts.** Every input and output is a Pydantic schema. No free text between agents.
- **Tools execute in the runtime, not the agent.** The agent requests a call; the runtime enforces policy, executes, logs, returns the result.
- **User-controlled context passing.** The runtime never decides what the next agent sees — you do, via a policy.
- **Single-agent and multi-agent share one interface.** A router decides which path runs.
- **Backends are interchangeable.** Thread for dev, FastStream for prod, container for untrusted contexts (Phase 4).
- **Observable by default.** Every spawn, tool call, completion flows through a typed `RuntimeEvent` to swappable emitters.
- **One public API.** `from murmur import ...` — no PydanticAI or FastStream types leak outward.

## Architecture

A typed pipeline with pluggable stages and composable middleware. Closer to ASGI / Tower than to hexagonal — the work is mostly orchestration of I/O, so the shape that fits is a middleware pipeline, not a domain core with adapters.

```
Task → Router → Context → Tool resolve → Execute → Tool proxy → Validate → Result
                                                       │
                          middleware: cost · timeout · retry · depth limit · observability
```

Backends shipped:

```
ThreadBackend     asyncio  · default · zero-config
JobBackend        FastStream — Kafka / NATS / RabbitMQ / Redis Streams
ContainerBackend  Docker — full isolation for untrusted context  (Phase 4)
```

Trust levels gate tool access: `HIGH` (full) · `MEDIUM` (curated) · `LOW` (read-only) · `SANDBOX` (none).

Context passers form a cost/quality ladder:

```
Null → Full → Summary → Selective   (Summary / Selective ship in Phase 3)
```

## Install

```bash
uv add murmur-ai                   # ThreadBackend, no broker — works immediately
uv add 'murmur-ai[kafka]'          # add Kafka support
uv add 'murmur-ai[server]'         # FastAPI HTTP server
uv add 'murmur-ai[all]'            # all four brokers
```

> Murmur is built and managed with [`uv`](https://github.com/astral-sh/uv). Install with `curl -LsSf https://astral.sh/uv/install.sh | sh`.

Persistence extras (production deployments):

```bash
uv add 'murmur-ai[sqlite]'         # single-host file-backed RunStore
uv add 'murmur-ai[redis-runstore]' # cluster-wide RunStore
uv add 'murmur-ai[rocksdb]'        # high-throughput single-host
```

## Quickstart

```python
import asyncio

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec


class ResearchOutput(BaseModel):
    summary: str
    sources: list[str]
    confidence: float


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Given a topic, return a structured summary with sources.",
    output_type=ResearchOutput,
)


async def main() -> None:
    runtime = AgentRuntime()                                 # ThreadBackend, zero config
    result = await runtime.run(researcher, TaskSpec(input="Mechanistic interpretability"))
    if result.is_ok():
        print(result.output.summary)
    else:
        print("error:", result.error)


asyncio.run(main())
```

## Same agent, distributed

```python
runtime = AgentRuntime(broker="kafka://localhost:9092")     # JobBackend via FastStream

# fan-out
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)
```

Same `Agent`. Different runtime constructor. The agent does not know it moved.

## YAML specs

```yaml
version: 1
name: researcher
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"

instructions: |
  You are a research agent. Given a topic, return a structured
  summary with sources.

output_type: my_pkg.outputs.ResearchOutput   # importable class path

tools:
  - web_search
```

```python
runtime.run("researcher", TaskSpec(input="..."))   # resolves from registry
```

YAML and the SDK are bidirectional — every Python `Agent` round-trips through YAML and back.

## Distributed workers

```bash
murmur worker start \
    --agents researcher \
    --broker kafka://localhost:9092 \
    --concurrency 20
```

```python
from murmur.worker import Worker

worker = Worker(runtime=runtime, agents=["researcher"], concurrency=20)


@worker.on_task_complete
async def on_complete(task_id: str, agent_name: str, duration_ms: int) -> None:
    print(f"[{task_id}] {agent_name} done in {duration_ms}ms")


await worker.start()
```

## Observability — out of the box

```python
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

sse = SSEEventEmitter(heartbeat_interval=15.0)
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)

# Hand sse.subscribe() to a FastAPI EventSourceResponse for a live dashboard.
```

`murmur serve` exposes the same SSE stream as a standalone HTTP server:

```bash
murmur serve --broker kafka://localhost:9092 --publish-events --port 8420
# GET /events/stream — live RuntimeEvent frames for the entire worker fleet
```

## CLI

```bash
murmur run script.py                  # run a Python script
murmur validate specs/                # validate YAML specs
murmur worker start --agents X        # start a distributed worker
murmur serve --port 8420              # standalone HTTP server + SSE stream
```

`murmur workflow run` and `murmur status` are later phases.

## Stack

| Layer        | Choice                                |
| ------------ | ------------------------------------- |
| Language     | Python 3.11 / 3.12 / 3.13             |
| Validation   | Pydantic 2.13                         |
| Agents       | PydanticAI 1.87  *(internal)*         |
| Distribution | FastStream 0.6   *(internal)*         |
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
    __init__.py        # public API: Agent, AgentRuntime, TaskSpec, TrustLevel, AgentResult, AgentGroup, Edge, FanOut, ...
    agent.py           # Agent (wraps PydanticAI internally)
    runtime.py         # AgentRuntime + RuntimeOptions + broker-URL parsing
    types.py           # frozen value types

    core/              # protocols, pipeline, errors  (zero sibling imports)
    context/           # context passers (full · null)
    tools/             # tool registry + executor + MCP factories + builtin re-exports
    backends/          # thread · job (FastStream wrapper)
    groups/            # AgentGroup, Edge, runner
    middleware/        # retry · timeout · depth_limit · cost_tracking
    runs/              # RunStore + 4 concretes (in-memory · sqlite · rocksdb · redis)
    events/            # RuntimeEvent + 4 emitter concretes
    server/            # AgentServer + AgentRouter
    worker/            # distributed worker with lifecycle hooks
    registry/          # YAML + in-memory spec loaders
    interop/           # from_pydantic_ai · as_faststream_handler
    cli/               # run · validate · worker · serve

packages/murmur-client/        # separate wheel — HTTP + LocalClient

docs/                  # mkdocs site (concepts, guides, API ref)
tests/
```

PyPI distribution: `murmur-ai`. Import: `murmur` (`pip install murmur-ai` → `import murmur`).

The dependency arrow points inward to `core/` and `types.py`. Only `murmur.interop` may import `pydantic_ai` or `faststream`.

## Status

Pre-alpha. The runtime is feature-complete on its public surface — Phase 1 + 1.5 + 1.6 + 2 all shipped (events, cost tracking, distributed event bridge, MCP consume side, persistent run stores, standalone server, embedded mode). Phase 3 (smart context passers, group coordination tools, YAML workflow engine) and Phase 4 (Container isolation, full trust matrix, cascading-spawn controls) are scoped but not started.

Public API is stable on the surface that's shipped. Additive changes only until v0.1.

## Development

```bash
uv sync --group dev          # install dev tools
uv sync --group docs         # docs build deps (when editing docs/)

uv run ruff check src tests
uv run ruff format src tests
uv run ty check
uv run pytest                # all tests
uv run pytest -m "not integration"   # unit only — 556 passing
uv run pre-commit run --all-files

uv run mkdocs serve          # local docs preview at :8000
```

CI fails on any of: ruff lint, ruff format drift, `ty` errors, failing tests, dead docs links.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for setup and quality gates,
and read [`CLAUDE.md`](./CLAUDE.md) — the source of truth for project
conventions, architecture, and "do not build" boundaries. Mirrored to
[`AGENTS.md`](./AGENTS.md) for non-Claude tooling.

## License

[MIT](LICENSE). Compatible with our MIT (`pydantic-ai`, `pydantic`,
`uvloop`) and Apache-2.0 (`faststream`, `mcp`, `httpx`) dependencies.
