# Murmur

> **Agents that move as one.**

A Python multi-agent orchestration runtime. Strictly typed, broker-agnostic, zero-config to start, distributed when you need it.

> **Status:** pre-alpha. Interfaces will change.

---

## What it is

Murmur is **infrastructure**, not an agent framework. It does not define how an agent thinks — it defines how agents are spawned, distributed, and coordinated. The mental model is a **hypervisor** for LLM agents: spawn it, give it context, get a structured result back, kill it if needed.

Internally, Murmur builds on PydanticAI for single-agent execution and FastStream for broker-backed distribution — but those are **dependencies, not your imports**. Everything you write is `from murmur import ...`.

## Why

Most agent runtimes either lock you into one execution model (in-process only, or one specific broker) or let agents call tools directly with no policy layer. Murmur splits the concerns: PydanticAI owns single-agent execution, Murmur owns the orchestration — fan-out, routing, context passing, tool policy, trust levels, cost controls, observability — so the **same agent runs on a laptop or a Kafka cluster without changes.** You never construct a broker. You hand the runtime a URL.

## Design principles

- **Pluggable everything, sensible defaults.** Override what you care about.
- **Strict I/O contracts.** Every input and output is a Pydantic schema. No free text between agents.
- **Tools execute in the runtime, not the agent.** The agent requests a call; the runtime enforces policy, executes, logs, returns the result.
- **User-controlled context passing.** The runtime never decides what the next agent sees — you do, via a policy.
- **Single-agent and multi-agent share one interface.** A router decides which path runs.
- **Backends are interchangeable.** Thread for dev, FastStream for prod, container for untrusted contexts.
- **One public API.** `from murmur import ...` — no PydanticAI or FastStream types leak outward.

## Architecture

A typed pipeline with pluggable stages and composable middleware. Closer to ASGI / Tower than to hexagonal — the work is mostly orchestration of I/O, so the shape that fits is a middleware pipeline, not a domain core with adapters.

```
Task → Router → Context → Tool resolve → Execute → Tool proxy → Validate → Result
                                                       │
                          middleware: cost · timeout · retry · depth limit · observability
```

Backends:

```
ThreadBackend     asyncio  · default · zero-config
ProcessBackend    CPU isolation
JobBackend        FastStream — Kafka / NATS / RabbitMQ / Redis Streams
ContainerBackend  Docker — full isolation for untrusted context  (phase 4)
```

Trust levels gate tool access: `HIGH` (full) · `MEDIUM` (curated) · `LOW` (read-only) · `SANDBOX` (none).

Context passers form a cost/quality ladder:

```
Null → Full → Summary → Selective
```

## Quick start

### Install

```bash
uv add murmur-ai                   # ThreadBackend, no broker — works immediately
uv add 'murmur-ai[kafka]'          # add Kafka support
uv add 'murmur-ai[all]'            # all brokers
```

> Murmur is built and managed with [`uv`](https://github.com/astral-sh/uv). Install with `curl -LsSf https://astral.sh/uv/install.sh | sh`.

### Hello, agent

```python
import asyncio

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec
from murmur.context import NullContextPasser
from murmur.types import TrustLevel


class ResearchOutput(BaseModel):
    summary: str
    sources: list[str]
    confidence: float


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Given a topic, return a structured summary with sources.",
    output_type=ResearchOutput,
    tools=["web_search"],
    trust_level=TrustLevel.MEDIUM,
    context_passer=NullContextPasser(),
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

### Same agent, distributed

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

### YAML specs

```yaml
name: researcher
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"
backend: auto

instructions: |
  You are a research agent. Given a topic, return a structured
  summary with sources.

output_schema:
  type: object
  required: [summary, sources, confidence]
  properties:
    summary:    { type: string }
    sources:    { type: array, items: { type: string } }
    confidence: { type: number, minimum: 0.0, maximum: 1.0 }

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

## CLI (Phase 1)

```bash
murmur run script.py                  # run a Python script
murmur validate specs/                # validate all specs
murmur worker start --agents X        # start a distributed worker
```

`murmur serve`, `murmur workflow run`, and `murmur status` are later phases.

## Stack

| Layer        | Choice                                |
| ------------ | ------------------------------------- |
| Language     | Python 3.11+                          |
| Validation   | Pydantic 2.13                         |
| Agents       | PydanticAI 1.87  (internal)           |
| Distribution | FastStream 0.6   (internal)           |
| Logging      | structlog 25.5                        |
| Packaging    | uv                                    |
| Lint/Format  | ruff                                  |
| Type check   | ty (Astral)                           |
| Testing      | pytest · pytest-asyncio · hypothesis  |

Versions are pinned exactly in `pyproject.toml`. See `CLAUDE.md` for rationale.

## Project layout

```
src/murmur/
    __init__.py        # public API: Agent, AgentRuntime, TaskSpec, TrustLevel, AgentResult
    agent.py           # Agent (wraps PydanticAI internally)
    runtime.py         # AgentRuntime + broker-URL parsing
    types.py           # TrustLevel, TaskSpec, AgentResult, AgentHandle

    core/              # pipeline, router, errors  (zero sibling imports)
    context/           # context passers (full · null)
    tools/             # tool registry + executor + builtins
    backends/          # thread · job (FastStream wrapper)
    worker/            # distributed worker with lifecycle hooks
    registry/          # YAML + in-memory spec loaders
    middleware/        # retry · timeout · depth_limit
    interop/           # from_pydantic_ai · as_faststream_handler
    cli/               # run · validate · worker
tests/
```

PyPI distribution: `murmur-ai`. Import: `murmur` (`pip install murmur-ai` → `import murmur`).

The dependency arrow points inward to `core/` and `types.py`. Only `murmur.interop` may import `pydantic_ai` or `faststream`.

## Development

```bash
uv sync --group dev          # install dev tools

uv run ruff check .          # lint
uv run ruff format .         # format
uv run ty check              # type check
uv run pytest                # all tests
uv run pytest -m "not integration"   # unit only
uv run pre-commit run --all-files
```

CI fails on any of: ruff lint, ruff format drift, `ty` errors, failing tests.

## Contributing

Read [`CLAUDE.md`](./CLAUDE.md) first — it is the source of truth for project conventions, architecture, and "do not build" boundaries. Mirrored to [`AGENTS.md`](./AGENTS.md) for non-Claude tooling.

## License

TBD.
