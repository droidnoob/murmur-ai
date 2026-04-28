# Murmur — User Guide

The README has the pitch and a quickstart. This file is the user manual: every piece of the public API you'll touch, plus the architectural nuances that aren't obvious from the docstrings.

> **Status: pre-alpha.** Pinned to Python 3.11+, Pydantic 2.13, PydanticAI 1.87, FastStream 0.6.7. Public API in `from murmur import …` is intentionally narrow.

---

## Contents

1. [Install](#install)
2. [The public API surface](#the-public-api-surface)
3. [Concepts](#concepts)
4. [Quickstart — single agent, in-process](#quickstart--single-agent-in-process)
5. [Pre/post hooks](#prepost-hooks)
6. [Tools and trust levels](#tools-and-trust-levels)
7. [Multi-agent groups (`AgentGroup` + `Edge` + `FanOut`)](#multi-agent-groups)
8. [Distributed mode (broker URLs)](#distributed-mode-broker-urls)
9. [YAML specs](#yaml-specs)
10. [CLI](#cli)
11. [Server / client (`AgentServer` + `MurmurClient`)](#server--client)
12. [Errors and `request_id`](#errors-and-request_id)
13. [Configuration / extras](#configuration--extras)
14. [Common gotchas](#common-gotchas)
15. [Status and what's deferred](#status-and-whats-deferred)

---

## Install

```bash
# Base install — ThreadBackend works immediately, no broker required.
uv add murmur-ai

# Optional broker support. Pick what you actually deploy against.
uv add 'murmur-ai[kafka]'
uv add 'murmur-ai[nats]'
uv add 'murmur-ai[rabbitmq]'
uv add 'murmur-ai[redis]'
uv add 'murmur-ai[all]'         # all four

# Server (FastAPI host for your agents).
uv add 'murmur-ai[server]'      # adds fastapi, uvicorn, sse-starlette

# Lightweight HTTP client (no PydanticAI, no FastStream — talks to a server).
# Currently shipped from the same wheel; will split into its own package later.
from murmur_client import MurmurClient
```

---

## The public API surface

Anything reachable from `from murmur import ...` is the contract you can rely on. Everything else (modules with leading underscore, anything under `core/protocols/`, anything in `backends/`, `groups/runner.py`, etc.) is internal — it can move without notice.

```python
from murmur import (
    Agent,            # frozen Pydantic value object — your agent definition
    AgentContext,     # frozen — what gets threaded between stages
    AgentGroup,       # frozen DAG of agents
    AgentHandle,      # opaque handle returned by Backend.spawn
    AgentResult,      # typed envelope: output | error + metadata
    AgentRuntime,     # the front door — run, gather, run_group
    Edge,             # one connection in an AgentGroup topology
    FanOut,           # type annotation marking a list field as the fan-out target
    ResultMetadata,   # duration, tokens, backend, trace_id
    TaskSpec,         # one unit of work — input + request_id + metadata
    TrustLevel,       # HIGH / MEDIUM / LOW / SANDBOX
)

# Lazy-imported (require optional extras):
from murmur.server import AgentServer, ErrorResponse   # needs murmur-ai[server]
from murmur.worker import Worker                       # always available
from murmur_client import MurmurClient, Run            # lightweight HTTP client
```

---

## Concepts

| Type | What it is |
|---|---|
| `Agent` | A frozen value object holding `name`, `model`, `instructions`, `output_type`, `tools`, `trust_level`, `context_passer`, `backend` hint, optional `input_type`, optional `pre_process` / `post_process` hooks. **Pure data.** Dispatch lives in `AgentRuntime`. |
| `TaskSpec` | One unit of work. `id`, `request_id` (auto-uuid4), `input` (string), arbitrary `metadata`. Frozen. |
| `AgentResult[T]` | What every dispatch returns. Either `output: T` is set (success) or `error` is set (failure) — never both. `is_ok()` discriminates. Frozen. |
| `AgentRuntime` | The front door. `run`, `gather`, `run_group`. Picks `ThreadBackend` for local mode (no broker URL) and `JobBackend` for distributed (any URL). |
| `AgentGroup` + `Edge` | Declarative DAG of agents. Each `Edge` connects an upstream to one or more downstreams, optionally via a `mapper`. |
| `FanOut[list[T]]` | Type annotation that marks a Pydantic field as the auto-fan-out target. The runner spawns one downstream agent per item when the edge has no explicit mapper. |
| `Worker` | Broker-side consumer. Subscribes to per-agent task topics, dispatches via an inner ThreadBackend runtime, publishes results back. |
| `AgentServer` | FastAPI app that registers agents and groups and exposes them over HTTP. |
| `MurmurClient` | `httpx`-backed client for an `AgentServer`. Same exception types as the server. |

The architecture is a typed pipeline of pluggable stages, with backends, context passers, tool providers, etc. all defined as `typing.Protocol` first (in `murmur.core.protocols`). Concretes match structurally — they never inherit from the Protocols.

---

## Quickstart — single agent, in-process

```python
import asyncio
from pydantic import BaseModel
from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.context.null import NullContextPasser


class ResearchFinding(BaseModel):
    summary: str
    confidence: float


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Return a one-paragraph summary with a confidence score.",
    output_type=ResearchFinding,
    trust_level=TrustLevel.MEDIUM,
    context_passer=NullContextPasser(),
)


async def main() -> None:
    runtime = AgentRuntime()                              # ThreadBackend, zero config
    result = await runtime.run(
        researcher, TaskSpec(input="What is mechanistic interpretability?")
    )
    if result.is_ok():
        assert isinstance(result.output, ResearchFinding)
        print(result.output.summary, result.output.confidence)


asyncio.run(main())
```

Fan-out is one call:

```python
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)
# Per-task failures land in their slot's AgentResult.error — never raises on partial.
ok = [r.output for r in results if r.is_ok()]
```

---

## Pre/post hooks

Same-type, sync, pure transformations that run inside the agent's run boundary. Use them for input cleaning, output validation, normalisation. They compose left-to-right.

```python
def strip_whitespace(input: str) -> str:
    return input.strip()

def cap_confidence(out: ResearchFinding) -> ResearchFinding:
    return out.model_copy(update={"confidence": min(out.confidence, 1.0)})


agent = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="...",
    output_type=ResearchFinding,
    pre_process=(strip_whitespace,),
    post_process=(cap_confidence,),
)
```

Rules:
- Each hook is `(T) -> T` — no type changes (use a mapper on an `Edge` for that).
- Pure / sync. No I/O, no `async`.
- Empty tuple = identity.
- If `Agent.input_type` is set, the raw `task.input` string is parsed into that type before the first hook runs; the post-hook output is re-serialised to JSON for PydanticAI's run.

---

## Tools and trust levels

Tools execute in the runtime, not the agent. The agent emits a tool call; the runtime intercepts, applies trust-level policy, executes, logs, returns the result. Trust levels:

| Level | Tool access |
|---|---|
| `HIGH` | Full — every registered tool. |
| `MEDIUM` | Curated — explicit allow-list per agent. |
| `LOW` | Read-only — `read_file`, `web_search`, etc. |
| `SANDBOX` | None. Pure reasoning only. |

Register tools on the runtime, not the agent:

```python
from murmur import AgentRuntime
from murmur.tools import ToolRegistry, ToolExecutor

registry = ToolRegistry()

async def web_search(query: str) -> str:
    """Search the web for ``query`` and return the top result."""
    ...

registry.register("web_search", web_search)
runtime = AgentRuntime(tool_registry=registry)
```

`@agent.tool` decorator — explicitly **dropped** in Phase 1 (per dispatch decision D1). Agents stay pure data; tools live on the runtime.

---

## Multi-agent groups

A `crew = AgentGroup(name=..., topology={agent: Edge(...), ...})` is a declarative DAG. Each `Edge` says "the output of this upstream flows to these downstream agent(s), optionally transformed by a mapper".

The runner resolves what flows across an edge by a four-way ladder:

1. **Mapper returns `list[TaskSpec]`** → fan-out. Use `runtime.gather`.
2. **Mapper returns `TaskSpec`** → single dispatch. Use `runtime.run`.
3. **No mapper, upstream `output_type` has a `FanOut[list[T]]` field** → auto fan-out, one downstream per item.
4. **No mapper, no `FanOut`** → JSON-serialise the upstream output as the downstream's `TaskSpec.input`.

`AllAgentsFailedError` is raised when an entire fan-out tier fails — the downstream mapper never sees error envelopes.

### Auto fan-out via `FanOut`

```python
from pydantic import BaseModel, Field
from murmur import Agent, AgentGroup, Edge, FanOut, TaskSpec


class SubQuestion(BaseModel):
    question: str
    search_terms: list[str] = Field(default_factory=list)


class DecompositionResult(BaseModel):
    sub_questions: FanOut[list[SubQuestion]]   # marker — auto fan-out target
    reasoning: str = ""


class MinionFinding(BaseModel):
    answer: str
    confidence: float


class FinalReport(BaseModel):
    title: str
    findings_count: int


# (define head, minion, summary Agents...)

def minions_to_summary(findings: list[MinionFinding]) -> TaskSpec:
    return TaskSpec(input=f"synthesise {len(findings)} findings")


crew = AgentGroup(
    name="research",
    topology={
        head:    Edge(to=(minion,)),                              # auto fan-out via FanOut
        minion:  Edge(to=(summary,), mapper=minions_to_summary),  # aggregating mapper
        summary: Edge.terminal(),                                 # terminal node
    },
)

runtime = AgentRuntime()
report = await runtime.run_group(crew, TaskSpec(input="Failure modes of LLM agents"))
assert isinstance(report.output, FinalReport)
```

### Topology rules

- Exactly **one terminal node** (`Edge.to=()`). Multi-output is Phase 3.
- Each downstream has exactly **one incoming edge** (Phase 1 simplification).
- No cycles, no dangling references — caught at `AgentGroup` construction.
- Multiple terminals → `TopologyError` raised at `run_group` time.

### Multiple groups, shared agents

```python
research_crew = AgentGroup(name="research", topology={head: Edge(to=(minion,)), ...})
audit_crew    = AgentGroup(name="audit",    topology={head: Edge(to=(minion,)), ...})

# Same `head` and `minion` Agent objects are reused.
```

When two groups run concurrently against the same broker, they're isolated by `batch_id`. Each `gather` call generates a fresh UUID; each `spawn` uses the handle's UUID. The `ResultCollector` routes messages on the runtime's reply topic by `batch_id` into the right `BatchState`. Workers consume from per-agent topics blindly — they never know which group triggered the task.

---

## Distributed mode (broker URLs)

Same agent, different runtime constructor:

```python
runtime = AgentRuntime()                                  # ThreadBackend
runtime = AgentRuntime(broker="memory://")                # JobBackend, in-process pub/sub
runtime = AgentRuntime(broker="kafka://host:9092")        # JobBackend over Kafka
runtime = AgentRuntime(broker="nats://host:4222")
runtime = AgentRuntime(broker="amqp://user:pass@host")    # RabbitMQ
runtime = AgentRuntime(broker="redis://host:6379")
```

The agent never sees the difference. Pick one based on deployment shape:

| Scheme | When |
|---|---|
| (none) | Local dev, single-agent scripts, single-host. |
| `memory://` | Want distributed-mode semantics in one process. Tests, debugging, demos. |
| `kafka://` | High-throughput, durable, partitioned. |
| `nats://` | Low-latency, lightweight, JetStream optional. |
| `amqp://` | RabbitMQ — flexible routing, mature ops story. |
| `redis://` | Redis Streams — tiny footprint, easy to host. |

### Topology

```
runtime --publish--> murmur.{agent}.tasks ---> Worker
                                                  │
                                                  ▼
                                     ThreadBackend dispatch
                                                  │
        ResultCollector  <----publish---- murmur.results.{runtime_id}
```

`JobBackend` is a transport for `ThreadBackend` invocations across machines. The worker's *internal* runtime is always thread-mode — passing it a broker URL would re-publish tasks in an infinite loop.

### Workers

```bash
murmur worker start \
    --agents researcher \
    --broker kafka://localhost:9092 \
    --specs ./specs \
    --concurrency 20 \
    --prefetch 5
```

Or programmatically:

```python
from murmur import AgentRuntime
from murmur.worker import Worker

worker_runtime = AgentRuntime()                       # MUST be thread-mode
publisher = AgentRuntime(broker="kafka://...")        # publisher uses broker
broker = publisher.backend._broker                    # reuse the constructed Broker

worker = Worker(
    broker=broker,
    agents={"researcher": researcher},                # name → Agent
    runtime=worker_runtime,
    concurrency=20,
)

@worker.on_task_start
async def on_start(task_id: str, agent_name: str) -> None: ...

@worker.on_task_complete
async def on_complete(task_id: str, agent_name: str, duration_ms: int) -> None: ...

@worker.on_task_error
async def on_error(task_id: str, agent_name: str, error: Exception) -> None: ...

await worker.start()
```

---

## YAML specs

Agents (and Phase 3 groups / workflows) round-trip through YAML. Convention:

```
specs/
    agents/
        researcher.yaml          # name field MUST equal filename stem
        synthesizer.yaml
    groups/                      # Phase 3
    workflows/                   # Phase 3
```

### Schema

```yaml
version: 1                                            # required, validator rejects others
name: researcher                                      # must match filename
model: anthropic:claude-sonnet-4-6
trust_level: medium                                   # high | medium | low | sandbox
context_passer: "null"                                # null | full
backend: auto                                         # auto | thread | job

instructions: |
  ...

input_type:  my_pkg.types.ResearchQuestion            # optional — class path
output_type: my_pkg.types.ResearchFinding             # required — class path

tools:
  - web_search
```

`input_type` / `output_type` are importable Python class paths (per dispatch decision D4). The loader runs `importlib.import_module` + `getattr`, then verifies the class is a `BaseModel` subclass. Replace with your own package; the bundled `murmur.examples.types` exists only as a working reference.

### Validation rules

`murmur validate <dir>` and `YamlRegistry.validate()` collect all errors per file (not fail-fast):

- Invalid YAML → `[file]: invalid YAML — <pyyaml message>`
- Top-level not a mapping → `[file]: top-level must be a mapping, got list`
- Missing / unsupported `version` → Pydantic error
- Unknown field → Pydantic error (we use `extra="forbid"`)
- Filename ≠ name → `[file]: filename 'foo' does not match spec name 'bar'`
- Unresolvable class path → `[file]: could not import module …` / `class 'X' not found in module 'Y'`
- Duplicate agent name → `[file]: duplicate agent name 'X' (already loaded)`

### Round-trip

```python
from murmur.registry.yaml import agent_to_spec, spec_to_agent

spec = agent_to_spec(researcher)          # AgentSpecYaml
restored = spec_to_agent(spec)            # Agent — structurally equal
yaml_text = _yaml.safe_dump(spec.model_dump(mode="json"))
```

Phase 1 supports `NullContextPasser` and `FullContextPasser` in YAML. Custom context passers raise `SpecValidationError` from `agent_to_spec` until Phase 3.

---

## CLI

```bash
murmur --log-level INFO <command>

# Validate every spec under a directory.
murmur validate ./specs

# Run a Python script with `murmur` already importable.
murmur run myscript.py
murmur run myscript.py -- --my-flag value      # forwarded args after `--`

# Start a broker-side worker. Loads agents from --specs.
murmur worker start \
    --agents researcher,synthesizer \
    --broker kafka://localhost:9092 \
    --specs ./specs \
    --concurrency 10 \
    --prefetch 5
```

Exit codes (consistent across commands):

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Validation / runtime errors |
| `2` | Setup / configuration errors (missing dir, bad URL, unknown agent name) |
| `130` | CTRL-C (POSIX `128 + SIGINT`) |

`murmur worker start` handles `SIGTERM` / `SIGINT` for clean drain — in-flight tasks complete before the broker disconnects.

---

## Server / client

Two separate concerns:

- **`AgentServer`** is the host. You register agents and groups, call `serve(port)`, and the FastAPI app exposes them over HTTP.
- **`MurmurClient`** is the lightweight client. `httpx` + `pydantic` only — no PydanticAI, no FastStream, no agent code. Talks to any `AgentServer`.

### Server

```python
from murmur import AgentRuntime
from murmur.server import AgentServer

# For broker-mode dispatch, give the server a runtime with a broker URL:
server = AgentServer(runtime=AgentRuntime(broker="kafka://localhost:9092"))

# Or accept the default (thread-mode, in-process) for simple deployments:
server = AgentServer()

server.register(researcher)
server.register_group(research_crew)        # auto-registers the group's agents too

await server.serve(port=8421)
```

The DAG walker for `register_group(...)` runs as a background task on `/submit` and reuses the same `runtime.run` / `runtime.gather` as everything else. Workers (the broker-side fleet) don't know groups exist — they consume per-agent topics blindly.

### HTTP routes

```
# Discovery
GET    /agents                        → list[str]
GET    /agents/{name}/schema          → input/output JSON schemas
GET    /groups                        → list[str]
GET    /groups/{name}/topology        → { agents: [...], edges: [...] }
GET    /health                        → { "status": "ok" }

# Synchronous dispatch (short-lived)
POST   /agents/{name}/run             → AgentResult-shaped body
POST   /agents/{name}/gather          → list[AgentResult-shaped body]
POST   /groups/{name}/run             → AgentResult-shaped body

# Asynchronous dispatch (long-lived)
POST   /submit                        → { run_id }
GET    /runs/{run_id}/status          → RunStatus
GET    /runs/{run_id}/result          → AgentResult-shaped body  (409 if not done)
GET    /runs/{run_id}/stream          → SSE stream of RunEvent
POST   /runs/{run_id}/cancel          → { state }
```

### Client

```python
import httpx
from murmur import TaskSpec
from murmur_client import MurmurClient

async with MurmurClient("http://server:8421") as client:
    # Synchronous
    result = await client.run("researcher", TaskSpec(input="..."))
    findings = await client.gather("researcher", [TaskSpec(input=q) for q in qs])
    report = await client.run_group("research-crew", TaskSpec(input="..."))

    # Asynchronous: submit + poll/stream
    run = await client.submit("research-crew", TaskSpec(input="..."), is_group=True)
    status = await run.status()
    if status.state.value == "completed":
        result = await run.result()
    async for event in run.stream():
        print(event.type, event.agent)
    await run.cancel()

    # Discovery
    agents = await client.list_agents()
    schema = await client.get_agent_schema("researcher")
    topology = await client.get_group_topology("research-crew")
```

---

## Errors and `request_id`

Every dispatch carries a `request_id` end-to-end:

- The HTTP middleware reads `X-Request-Id` (or generates a UUID) and binds it to `structlog.contextvars`.
- The handler stamps it onto every `TaskSpec`.
- `JobBackend` includes it in `TaskMessage`. Workers re-bind it to `structlog` for their dispatch.
- Logs anywhere in the stack carry `request_id` automatically.

```bash
grep "req-abc-123" logs/*.log    # full picture across all agents in that run
```

### Error shape

The server emits `ErrorResponse` for every non-2xx:

```json
{
  "error": "BudgetExceededError",
  "message": "Token budget exceeded",
  "agent": "research-minion",
  "task_id": "batch-xyz-42",
  "request_id": "req-abc-123"
}
```

| Domain error | HTTP status |
|---|---|
| `SpecValidationError` / `TopologyError` | 400 |
| `TrustViolationError` | 403 |
| `RegistryError` | 404 |
| `BudgetExceededError` / `DepthLimitError` | 429 |
| `SpawnError` / `ToolExecutionError` / `ContextError` / `AllAgentsFailedError` | 500 |
| `TimeoutError` | 504 |
| Unknown | 500 |

`MurmurClient` parses the response body and raises the matching exception class. User code catches the same type whether the agent ran locally or behind an HTTP server:

```python
from murmur.core.errors import BudgetExceededError, RegistryError

try:
    result = await client.run("researcher", task)
except BudgetExceededError as exc:
    ...
except RegistryError:
    ...
```

### Graceful shutdown

`AgentServer` handles `SIGTERM` / `SIGINT`:

1. Middleware short-circuits incoming requests with `503 + Retry-After: 5`.
2. `_drain` waits for `active_runs` to finish, capped by `drain_timeout` (default 30s).
3. Broker connections close; the process exits.

`Worker.stop()` mirrors this: drain in-flight tasks, then disconnect the broker. Tasks that didn't make the drain timeout fall through `ResultCollector`'s timeout path on the publisher side and surface as `AgentResult.error = SpawnError("…did not complete (broker timeout)")`.

---

## Configuration / extras

```toml
# pyproject.toml — what the user installs
[project.optional-dependencies]
kafka     = ["faststream[kafka]==0.6.7"]
nats      = ["faststream[nats]==0.6.7"]
rabbitmq  = ["faststream[rabbit]==0.6.7"]
redis     = ["faststream[redis]==0.6.7"]
all       = [...]                          # all four brokers
server    = ["fastapi", "uvicorn", "sse-starlette"]
container = ["docker==7.1.0"]              # Phase 4 — DO NOT install in Phase 1
```

The base install ships only PydanticAI + FastStream's core + structlog + pyyaml. ThreadBackend works without any extras. Pick broker extras to unlock the matching `FastStreamBroker`.

---

## Common gotchas

1. **`AgentServer()` defaults to thread-mode.** No broker URL means no `JobBackend`, no topics. For distributed deployments pass `runtime=AgentRuntime(broker="kafka://…")` explicitly. The default is fine for single-host servers.
2. **The worker's runtime must be thread-mode.** A broker-mode runtime would re-publish each consumed task in an infinite loop. Default `AgentRuntime()` is correct.
3. **YAML class paths are real Python imports.** The loader does `importlib.import_module(module_path); getattr(...)`. Whatever you reference must be importable in the worker's environment too.
4. **`@agent.tool` does not exist.** Tools register on the runtime's `ToolRegistry`. Decision D1 from `phase-1-mvp-dispatch.md`.
5. **`output_type` is a class path, not a JSON schema** (decision D4). The README's quickstart will be tightened to match — until then, follow this guide for YAML.
6. **`Worker(agents=…)` takes a dict** (`Mapping[str, Agent]`), not a list of names. The CLI loads names from a `YamlRegistry` and constructs the dict for you.
7. **Two concurrent groups sharing an agent are batch-isolated, not topic-isolated.** They publish to the same `murmur.{agent}.tasks` topic. The `ResultCollector` routes replies by `batch_id` into the right awaiter — that's where isolation lives.
8. **`murmur worker start --broker memory://`** is valid and useful — single-process distributed-mode for debugging.

---

## Status and what's deferred

Done in Phase 1 (everything in `from murmur import …` works end-to-end against `memory://` and FastStream's `TestBroker` for all four schemes):

- Single-agent dispatch (`runtime.run`, `runtime.gather`)
- Multi-agent groups (`AgentGroup`, `Edge`, `FanOut`, `runtime.run_group`)
- ThreadBackend, JobBackend, FastStream wrappers (kafka/nats/amqp/redis), InMemoryBroker
- Worker with lifecycle hooks
- Pre/post hooks, `request_id` propagation
- YAML registry with version field, validation, round-trip
- CLI: `validate`, `run`, `worker start`
- AgentServer (FastAPI) with all routes from the addendum
- MurmurClient with typed error round-trip
- Graceful shutdown on server and worker

Carved off as follow-ups (tracked in `.planning/phase-1-mvp.md`):

- **#3b real-broker integration tests** — testcontainers running Kafka/NATS/Rabbit/Redis containers in CI. The wrappers are correct; this is empirical proof.
- **`murmur-client` packaging split** — `src/murmur_client/` is ring-fenced, but it ships from the same wheel today. Splitting to `pip install murmur-client` is project-layout work.
- **#7 interop adapters** — `from_pydantic_ai`, `as_faststream_handler`.
- **#8 pipeline middleware composition** — wiring `RetryMiddleware` / `TimeoutMiddleware` / `DepthLimitMiddleware` into the runtime via `RuntimeOptions`.
- **#10 runtime hardening** — `gather(fail_fast=...)` flag, bounded `state` mapping.
- **#11 docs** — `examples/quickstart.py`, `examples/distributed.py`.
- **#12 CI** — GitHub Actions matrix.

Phase 2 / 3 / 4 features (observability events, smart context passers, group coordination tools, ContainerBackend, full trust-level enforcement) are explicitly out of Phase 1.
