# CLAUDE.md

Authoritative project context for Claude Code (and any other coding agent — this file is mirrored to `AGENTS.md`). Read this in full before making changes.

> **Tagline:** *Agents that move as one.*

---

## 1. What Murmur is

Murmur is a **Python multi-agent orchestration runtime**.

It is **not** a framework for defining agent behavior. It is **infrastructure** for spawning, distributing, and coordinating LLM-based agents reliably at scale. The mental model is a **hypervisor** for LLM agents: spawn it, give it context, get a structured result back, kill it if needed.

PydanticAI handles single-agent execution (LLM calls, tools, structured output, validation). FastStream handles broker-backed distribution. Murmur owns the orchestration layer between them — and **hides both behind its own public API.**

Repository: `murmur-ai` · PyPI distribution: `murmur-ai` · Import package: `murmur`.

---

## 2. PUBLIC API RULE — CRITICAL

> **Users NEVER import from `pydantic_ai` or `faststream` directly. Everything is `from murmur import ...`.**

- `murmur.Agent` is a **single unified class** that wraps PydanticAI internally. It combines LLM config (model, instructions, output_type, tools) with Murmur orchestration config (trust_level, context_passer, backend) on the same object. There is **no separate `AgentSpec` + PydanticAI agent**.
- `murmur.AgentRuntime` accepts a **broker URL string** (`"kafka://..."`, `"nats://..."`, `"amqp://..."`, `"redis://..."`) and constructs FastStream brokers internally. Users never import `KafkaBroker` etc.
- PydanticAI and FastStream are **dependencies, not public API**.
- Migration adapters live in `murmur.interop` (and only there):
  - `from_pydantic_ai(...)` — wrap an existing PydanticAI agent into a `murmur.Agent`
  - `as_faststream_handler(...)` — expose a `murmur.Agent` as a FastStream subscriber

Anything in `murmur/__init__.py` is the public API. Anything else is internal — internal modules may reorganize without notice.

---

## 2a. INTERFACES RULE — CRITICAL

> **Every pluggable component is a `typing.Protocol` first, concrete second. The Protocol is written before any implementation. Core never imports concrete implementations.**

All Protocol definitions live in `src/murmur/core/protocols/` — **one file per Protocol** — and are re-exported from `core.protocols.__init__`. Tests are written against the Protocol; every concrete implementation is run through the **same shared test suite** (e.g. `ThreadBackend` and `JobBackend` both pass `BackendContract` tests).

### Required Protocols (must exist in `core/protocols/`)

| Protocol         | Required surface                                                       |
| ---------------- | ---------------------------------------------------------------------- |
| `Backend`        | `spawn`, `kill`, `status` (and `result` for retrieval)                 |
| `ContextPasser`  | `prepare`                                                              |
| `ToolProvider`   | `resolve`                                                              |
| `ToolExecutor`   | `execute`                                                              |
| `Router`         | `classify`                                                             |
| `EventEmitter`   | `emit`                                                                 |
| `Registry`       | `get`, `list`, `validate`                                              |
| `Worker`         | `start`, `stop`, `on_task_start`, `on_task_complete`, `on_task_error`  |
| `Pipeline`       | `run`                                                                  |
| `Stage`          | `__call__(context, next_stage)`                                        |
| `Middleware`     | `__call__(context, next_stage)`                                        |

### How concretes relate to Protocols

- **Structural typing only.** A concrete class never `class X(Backend):` — it simply implements the methods. Protocols match by shape.
- **No imports from concretes into Protocols.** `core/protocols/backend.py` knows nothing about `ThreadBackend`.
- **Type-only imports.** When a concrete needs to reference its Protocol (e.g. for an internal helper), import it under `if TYPE_CHECKING:` — runtime should not depend on the Protocol module.

### Layout

```
src/murmur/core/protocols/
    __init__.py          # re-exports every Protocol
    backend.py           # Backend
    context.py           # ContextPasser
    tools.py             # ToolProvider, ToolExecutor
    router.py            # Router
    events.py            # EventEmitter
    registry.py          # Registry
    worker.py            # Worker
    pipeline.py          # Pipeline, Stage, Middleware
```

### Workflow when adding a pluggable

1. Write or update the Protocol in `core/protocols/<name>.py`.
2. Write or update the shared contract test suite keyed on that Protocol.
3. Implement the concrete in its sibling package (`backends/`, `context/`, `tools/`, …).
4. Run the shared suite against the new concrete via `pytest -k <Protocol>Contract`.

---

## 3. Core design principles

- Execution unit is flexible — thread, process, container, or distributed job. The caller never picks blindly; the runtime decides based on task classification and configured backend.
- Everything is pluggable with sensible defaults. Users override only what they care about.
- **Strict I/O contracts.** Every agent input and output is schema-validated via Pydantic. No free text passes between agents.
- Context passing is a **user-controlled policy**, not a runtime decision.
- **Tools execute inside the runtime, not inside the agent.** The agent requests a tool call; the runtime enforces policy, executes, logs, and returns the result.
- Single-agent and multi-agent are the **same interface**. A router decides which path runs transparently.
- **ThreadBackend is the onboarding.** **JobBackend (FastStream) is the value prop.** Both are first-class from MVP.

---

## 4. Architecture — Pipeline + Middleware

Murmur is a **typed pipeline with pluggable stages and composable middleware**. Each stage has a clear responsibility, a typed input/output contract, and can be swapped without touching others. Middleware wraps the pipeline (or specific stages) for cross-cutting concerns.

This is **not** strict hexagonal. The "domain" is thin — Murmur is mostly orchestration of I/O — so a middleware pipeline (similar to ASGI / Starlette / Tower) is the right shape. We keep the principles of hexagonal that matter (Protocol-based ports, dependency inversion, frozen value objects) and drop the rigid `core/` vs `adapters/` split.

```
User Query
     │
     ▼
┌─────────────┐
│   Router    │  ← classifies complexity (rule-based first, LLM later)
└──────┬──────┘
       │
  ┌────┴────┐
  ▼         ▼
Single    Orchestrator
Agent     → Fan-out → Aggregator
  │                        │
  └──────────┬─────────────┘
             ▼
         Response
```

### Pipeline stages

```
Task → Router → Context → Tool resolve → Execute → Tool proxy → Validate → Result
                                                       │
                          middleware: cost · timeout · retry · depth limit · observability
```

### Stage protocol

```python
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeVar

T = TypeVar("T")


class Stage(Protocol[T]):
    async def __call__(
        self,
        context: PipelineContext,
        next_stage: Callable[[PipelineContext], Awaitable[T]],
    ) -> T: ...
```

Each stage receives the pipeline context and a reference to the next stage. It can mutate context before forwarding, transform the result on the way back, short-circuit, or wrap in try/except for stage-local error handling.

### Execution backends

```
ThreadBackend     ← asyncio.create_task — lightweight, default, zero-config
ProcessBackend    ← ProcessPoolExecutor — CPU isolation
JobBackend        ← FastStream subscriber/publisher (Kafka / NATS / RabbitMQ / Redis)
ContainerBackend  ← Docker SDK — full isolation for untrusted context  (Phase 4)
```

`ThreadBackend` and `JobBackend` must work from MVP. `JobBackend` activates when the user passes a broker URL.

### Context passers (cost/quality ladder)

```
NullContextPasser       — fresh spawn, no context
FullContextPasser       — pass everything
SummaryContextPasser    — AI summarizes  (Phase 3)
SelectiveContextPasser  — AI picks what is relevant; sanitizes returns from untrusted sub-agents  (Phase 3)
```

### Tool providers

```
StaticToolProvider     — fixed set                                  (Phase 1)
RoleBasedToolProvider  — role → tool map                            (Phase 3)
SelectiveToolProvider  — model picks                                (Phase 3)
DenylistToolProvider   — base set minus denied (untrusted contexts) (Phase 3)
```

### Trust levels

```python
class TrustLevel(StrEnum):
    HIGH    = "high"     # full tool access
    MEDIUM  = "medium"   # curated tool set
    LOW     = "low"      # read-only tools
    SANDBOX = "sandbox"  # no tools, pure reasoning
```

### Tool execution flow

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

---

## 5. Agent — the single unified class

```python
from murmur import Agent
from murmur.context import NullContextPasser
from murmur.types import TrustLevel


minion = Agent(
    name="research-minion",
    model="anthropic:claude-sonnet-4-6",
    instructions="You are a research minion...",
    output_type=MinionFinding,           # Pydantic model
    tools=["web_search"],
    trust_level=TrustLevel.MEDIUM,
    context_passer=NullContextPasser(),
)


@minion.tool
async def fetch_database(query: str) -> str:
    """Fetch data from the internal database."""
    return f"Results for: {query}"
```

### YAML equivalent

```yaml
name: research-minion
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"
backend: auto

instructions: |
  You are a research minion...

output_schema:
  type: object
  required: [question, answer, confidence, sources, key_facts]
  properties:
    question: { type: string }
    answer: { type: string }
    confidence: { type: number, minimum: 0.0, maximum: 1.0 }
    sources: { type: array, items: { type: string } }
    key_facts: { type: array, items: { type: string } }

tools:
  - web_search
```

Python and YAML are **two representations of the same canonical spec** — bidirectional. Runtime does not care which the user picked.

### Spec registry

```
specs/
    agents/
        researcher.yaml
        fact_checker.yaml
    groups/
        research_crew.yaml
    workflows/        # Phase 3
        research-swarm.yaml
```

Spawn by name or by object:

```python
runtime.run("researcher", task)   # resolves from registry
runtime.run(researcher, task)     # pass the Agent directly
```

---

## 6. Runtime API

```python
from murmur import Agent, AgentRuntime, TaskSpec

runtime = AgentRuntime()                                  # local — ThreadBackend
runtime = AgentRuntime(broker="kafka://localhost:9092")   # distributed — JobBackend

# single
result = await runtime.run(agent, TaskSpec(input="..."))

# fan-out
results = await runtime.gather(
    agent,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)

# typed Result
for r in results:
    if r.is_ok():
        print(r.output)   # Pydantic model
    else:
        print(r.error)    # SpawnError, ToolExecutionError, ...
```

Same code, local or distributed. **Agents do not change. Workflows do not change. Only the runtime constructor changes.**

### Supported broker URLs

```
kafka://host:port
nats://host:port
amqp://host:port      # RabbitMQ
redis://host:port
```

URL parsing happens in `murmur.runtime`. Users never see FastStream broker classes.

---

## 7. Worker — distributed consumer

First-class `Worker` class with lifecycle hooks. CLI and programmatic.

```bash
murmur worker start \
    --agents research-minion \
    --broker kafka://localhost:9092 \
    --concurrency 20
```

```python
from murmur.worker import Worker

worker = Worker(
    runtime=runtime,
    agents=["research-minion"],
    concurrency=20,
    prefetch=5,
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

## 8. Phased build order — Phase 1 only for now

> **START WITH PHASE 1. Do NOT scaffold phases 2–4.** Build phase 1 fully working with tests before moving on.

### Phase 1 — MVP (current scope)

- `murmur.Agent` (wraps PydanticAI internally)
- `TaskSpec`, `AgentResult`, `AgentHandle`, `TrustLevel`
- `AgentRuntime.run()` and `AgentRuntime.gather()`
- Router (single vs multi — rule-based first; LLM later)
- `ThreadBackend` (asyncio, default, zero-config)
- `JobBackend` (FastStream, broker URL parsing)
- `FullContextPasser` + `NullContextPasser`
- `StaticToolProvider`
- Tool execution proxy with trust-level enforcement
- Schema validation on output (delegates to PydanticAI internally)
- Domain errors
- `structlog` throughout
- YAML spec loading + registry
- `Worker` class with lifecycle hooks
- CLI: `murmur run`, `murmur validate`, `murmur worker start`
- Tests for core + both backends
- Interop adapters: `from_pydantic_ai()`, `as_faststream_handler()`

### Phase 2 — Observability (later)

- `RuntimeEvent` model + `EventEmitter` protocol
- `LogEventEmitter`, `SSEEventEmitter`
- Cost-tracking middleware, token-budget enforcement
- Trace IDs flowing through the pipeline
- `murmur serve` CLI (event endpoint)
- Minimal read-only React dashboard

### Phase 3 — Smart context + groups + workflows (later)

- `SummaryContextPasser`, `SelectiveContextPasser`
- `AgentGroup`, `GroupSpec`
- Coordination: `Sequential`, `Parallel`, `DAG`
- `SharedMemoryTool`, `BarrierTool`
- `RoleBasedToolProvider`, `DenylistToolProvider`
- YAML workflow engine with Jinja templating

### Phase 4 — Isolation + trust (later)

- `ContainerBackend` (Docker SDK)
- Full trust-level enforcement matrix
- Untrusted-context sanitization
- Depth limit + cascading-spawn controls
- `WebSocketEventEmitter`, `FastStreamEventEmitter`

---

## 9. Toolchain & pinned dependencies

The full Astral toolchain. One ecosystem, single `pyproject.toml`.

```
uv        ← package manager, project mgmt, virtualenvs
ruff      ← linter + formatter
ty        ← type checker (Astral, Rust-based)
pytest    ← testing
structlog ← logging
```

### Pinned versions (verified live as of 2026-04-25)

| Package         | Version  |
| --------------- | -------- |
| Python          | >=3.11   |
| uv              | 0.11.7   |
| pydantic        | 2.13.3   |
| pydantic-ai     | 1.87.0   |
| faststream      | 0.6.7    |
| structlog       | 25.5.0   |
| pyyaml          | >=6.0    |
| ruff            | 0.15.12  |
| ty              | 0.0.32   |
| pytest          | 9.0.3    |
| pytest-asyncio  | 1.3.0    |
| pytest-cov      | 7.1.0    |
| hypothesis      | 6.152.2  |
| docker (SDK)    | 7.1.0    |  ← phase 4 only
| pre-commit      | 4.6.0    |

> `ty` is pre-1.0 (`0.0.x`). Pin exactly; bump deliberately.

### Broker extras

```
murmur-ai[kafka]      → faststream[kafka]
murmur-ai[nats]       → faststream[nats]
murmur-ai[rabbitmq]   → faststream[rabbit]
murmur-ai[redis]      → faststream[redis]
murmur-ai[all]        → all brokers
```

ThreadBackend has no broker dep — `pip install murmur-ai` works out of the box.

---

## 10. Python language constraints — `>=3.11`

The project targets Python 3.11+. Not 3.12+ or 3.13+.

**Allowed (3.11 features) — use freely:**
- `asyncio.TaskGroup`
- `asyncio.timeout()`
- `enum.StrEnum`
- `ExceptionGroup` and `except*`
- `typing.Self`
- `tomllib`
- `X | Y` union syntax (already 3.10+, fully supported)

**Forbidden (3.12+ syntax):**
- `type X = ...` statement (PEP 695). Use `TypeAlias` from `typing`.
- PEP 695 generic class/function syntax (`class Foo[T]:`, `def f[T](...)`). Use `TypeVar` and `Generic[T]`.

```python
# wrong — 3.12+ only
type AgentName = str
class AgentResult[T](BaseModel): ...

# right — 3.11 compatible
from typing import Generic, TypeAlias, TypeVar
AgentName: TypeAlias = str
T = TypeVar("T", bound=BaseModel)
class AgentResult(BaseModel, Generic[T]): ...
```

`from __future__ import annotations` is **optional** — 3.11 supports `X | Y`, `list[int]`, `dict[str, int]` natively at runtime.

---

## 11. Project structure (Phase 1)

The project uses **src layout** (`src/murmur/...`). Imports remain `from murmur ...`.

```
src/murmur/
    __init__.py              # PUBLIC API: Agent, AgentRuntime, TaskSpec, TrustLevel, AgentResult
    py.typed                 # PEP 561 marker
    agent.py                 # murmur.Agent (wraps PydanticAI internally)
    runtime.py               # AgentRuntime + broker-URL parsing + run() / gather()
    types.py                 # TrustLevel, TaskSpec, AgentResult, AgentHandle, AgentContext

    core/
        protocols/           # ALL Protocols live here — written before concretes
            __init__.py
            backend.py       # Backend
            context.py       # ContextPasser
            tools.py         # ToolProvider, ToolExecutor
            router.py        # Router
            events.py        # EventEmitter
            registry.py      # Registry
            worker.py        # Worker
            pipeline.py      # Pipeline, Stage, Middleware
        pipeline.py          # concrete Pipeline composer + PipelineContext
        errors.py            # MurmurError hierarchy

    context/
        __init__.py          # re-exports passers
        full.py              # Phase 1 — concrete, satisfies core.protocols.ContextPasser
        null.py              # Phase 1 — concrete
        # summary.py / selective.py — Phase 3

    routing/
        __init__.py
        always_single.py     # Phase 1 default — satisfies core.protocols.Router

    tools/
        __init__.py
        registry.py
        executor.py          # policy enforcement, rate limiting, logging
        builtin/
            web_search.py

    backends/
        __init__.py          # ThreadBackend, JobBackend — concretes only
        thread.py            # asyncio — DEFAULT, satisfies core.protocols.Backend
        job.py               # FastStream-driven, satisfies core.protocols.Backend
        # process.py — later
        # container.py — Phase 4

    worker/
        __init__.py
        worker.py            # Worker class with lifecycle hooks

    registry/
        __init__.py
        yaml.py              # YAML loader
        memory.py            # in-memory registry (tests)

    middleware/
        retry.py
        timeout.py
        depth_limit.py
        # cost_tracking.py / observability.py — Phase 2

    interop/
        __init__.py
        pydantic_ai.py       # from_pydantic_ai()
        faststream.py        # as_faststream_handler()

    cli/
        __init__.py          # main() entry point
        run.py               # murmur run
        validate.py          # murmur validate
        worker.py            # murmur worker start

tests/
    core/
    context/
    tools/
    backends/
    worker/
    interop/
    conftest.py
```

### Dependency rules

```
core/        → only stdlib + pydantic + typing. ZERO sibling-package imports.
backends/    → may import external libs; depend on core + types
context/     → may import external libs; depend on core + types
tools/       → may import external libs; depend on core + types
worker/      → uses runtime + core
registry/    → uses types + core
middleware/  → wraps stages; depends on core
interop/     → may import pydantic_ai / faststream — only place allowed to
cli/         → wires everything; depends on the public API
```

**Hard rule: nothing in `core/` imports from any sibling package.** The arrow always points inward toward `core/` and `types.py`.

```
backends/context/tools/middleware/registry → core, types     ✓
worker → runtime, core, types                                ✓
runtime → backends, context, tools, core, types              ✓
agent → core, types, (internally: pydantic_ai)               ✓
cli → public API + runtime + registry                        ✓
core → anything else                                         ✗ NEVER
```

---

## 12. Type system — fully typed, no exceptions

Every function, method, variable annotation, and return type is explicitly typed. `ty` enforces this in CI. **No `Any` unless unavoidable, and explicitly commented.**

```python
# wrong
def spawn(self, spec, task, backend=None): ...

# right
async def run(
    self,
    agent: Agent | str,
    task: TaskSpec,
    *,
    backend: Backend | None = None,
) -> AgentResult[BaseModel]: ...
```

### Protocols are fully typed

```python
from typing import Protocol

class Backend(Protocol):
    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> AgentHandle: ...

    async def kill(self, handle: AgentHandle) -> None: ...
```

### Generics

```python
from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

class AgentResult(BaseModel, Generic[T]):
    output: T | None
    error: Exception | None
    metadata: ResultMetadata
    agent_name: str
    task_id: str

    def is_ok(self) -> bool: ...
```

### Suppressions

- `# ty: ignore[<rule-name>]` only — never bare `# type: ignore`. Always include rule name and reason.
- `Any` requires an inline comment explaining why.

### `typing.cast` — avoid where possible

`cast` is a runtime no-op that asserts to the type checker without proving anything. Each `cast` is a place the type system stops helping. Default to **not using it**.

When tempted, the right move is almost always one of:

- **Narrow at a parser boundary** — write `parse_X(s: str) -> Literal[...] | None` returning `None` for invalid inputs, instead of `cast(Literal[...], s)` after a runtime `if s in {...}` check.
- **Return a precise type** — if the call site needs `cast(SomeShape, result)`, the function's return type is too loose. Make it a Pydantic `BaseModel`, `TypedDict`, or named dataclass. (Pydantic models also work as FastAPI response models out of the box; `TypedDict` trips on `from __future__ import annotations`.)
- **Use runtime constructors as narrowing** — `str(x)`, `int(x)`, `json.loads(x)` double as type narrowing for DB rows or external payloads.
- **Type the variable correctly upfront** — don't build a `list[object]` and cast at the boundary; type it `list[Foo]` from the start.

Reserve `cast` only for genuine untyped boundaries — and add a comment naming the boundary:
- DB drivers returning `Any` (sqlite3 row tuples, redis-py replies)
- Framework callback types loosely typed by the framework (e.g. Starlette `call_next`)
- Stdlib quirks like `MappingProxyType` not advertising `Mapping`
- Sanctioned access to a `_private` attribute already typed `Any`

If a cast doesn't fit one of those categories, find the real fix.

---

## 13. SOLID, DRY, YAGNI

- **SRP** — one responsibility per class. If you write "and" in the description, split.
- **OCP** — adding a new backend / context passer / tool provider is a new file. Zero changes to `core/`.
- **LSP** — never `isinstance(backend, ContainerBackend)` checks. All backends substitutable.
- **ISP** — narrow protocols. Split `Backend` and `Observable` rather than one fat interface.
- **DIP** — `AgentRuntime` receives a `Backend` protocol, never a concrete `ThreadBackend`.
- **DRY** — extract on the **third** occurrence, not the second.
- **YAGNI** — no abstract base classes until two concretes exist *now*. No flags for unimplemented behavior. No "extensibility hooks" with no users. No premature optimization.

### Explicit over implicit

- No metaclasses
- No `__init_subclass__` magic
- No dynamic attribute generation, no `__getattr__` indirection
- No monkey patching
- If you cannot trace execution top-to-bottom by reading, rewrite it

### Protocols over ABCs

```python
# wrong — forces inheritance
class MyBackend(BaseBackend): ...

# right — structural
class Backend(Protocol):
    async def spawn(...) -> AgentHandle: ...

class MyBackend:                  # satisfies Backend without importing murmur
    async def spawn(...) -> AgentHandle: ...
```

---

## 14. Async rules

- All I/O is async. No `time.sleep`, no `requests`, no blocking calls in async context.
- Use `asyncio.gather` (or `asyncio.TaskGroup` for structured concurrency) for fan-out. Never sequential `await` in a loop.
- Every async function that can fail has a timeout (`asyncio.timeout()`).

```python
# wrong
results = []
for task in tasks:
    results.append(await runtime.run(agent, task))

# right
results = await asyncio.gather(*[runtime.run(agent, t) for t in tasks])
# or
async with asyncio.TaskGroup() as tg:
    handles = [tg.create_task(runtime.run(agent, t)) for t in tasks]
results = [h.result() for h in handles]
```

```python
async with asyncio.timeout(30):
    result = await backend.spawn(agent, task, context)
```

---

## 15. Immutability

All spec / value objects are **frozen Pydantic models**. Never mutate — `model_copy(update=...)`.

```python
class TaskSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    input: str
    metadata: dict[str, str]

# wrong
task.input = "new"               # raises

# right
new_task = task.model_copy(update={"input": "new"})
```

Prefer `frozenset` over `set`, `tuple` over `list`, for fields that should not change.

---

## 16. Errors

Define domain errors. **Never** raise raw `Exception` or `ValueError` from core code.

```python
# src/murmur/core/errors.py

class MurmurError(Exception): ...
class SpawnError(MurmurError): ...
class ToolExecutionError(MurmurError): ...
class ContextError(MurmurError): ...
class BudgetExceededError(MurmurError): ...
class DepthLimitError(MurmurError): ...
class SpecValidationError(MurmurError): ...
class RegistryError(MurmurError): ...
```

Catch narrow, raise specific:

```python
try:
    result = await backend.spawn(agent, task, context)
except TimeoutError as e:
    raise SpawnError(f"agent '{agent.name}' timed out after {timeout}s") from e
except ConnectionError as e:
    raise SpawnError(f"agent '{agent.name}' failed to connect to backend") from e
```

---

## 17. Logging

`structlog` only. **No `print()`.** Every log entry from the runtime carries `agent_name`, `task_id`, `backend`, `trust_level`. Minimum four events per agent lifecycle: spawn, tool call, result, error.

```python
import structlog

log: structlog.stdlib.BoundLogger = structlog.get_logger()

await log.ainfo(
    "agent_spawned",
    agent_name=agent.name,
    task_id=task.id,
    backend=backend.__class__.__name__,
    trust_level=agent.trust_level.value,
)
```

---

## 18. Testing standards

- Unit tests: no external deps, mock backends only.
- Integration tests: real backends, real broker, marked `@pytest.mark.integration`.
- Every public method has a unit test.
- Test file mirrors source: `src/murmur/runtime.py` → `tests/test_runtime.py`.
- `pytest-asyncio` for async; `hypothesis` for property-based on specs and schemas.
- Coverage: ≥ 80% on `core/`, **100% on every protocol method in `core/`**.

```python
@pytest.mark.asyncio
async def test_run_unknown_agent_raises_registry_error() -> None:
    runtime = AgentRuntime(_backend=MockBackend(), _registry=MockRegistry([]))
    with pytest.raises(RegistryError, match="not found"):
        await runtime.run("nonexistent", task)
```

---

## 19. Naming conventions

```
Protocols          → noun describing capability:    Backend, ContextPasser, ToolProvider
Concrete types     → adjective + noun:              ThreadBackend, NullContextPasser
Errors             → noun + Error:                  SpawnError, BudgetExceededError
Async functions    → verb:                          run, gather, prepare, resolve, execute
Sync functions     → verb:                          validate, parse, load, build
Value objects      → noun:                          TaskSpec, AgentResult
Enums              → PascalCase class:              TrustLevel.HIGH
Modules            → lowercase, singular:           runtime.py, registry.py, executor.py
Test files         → test_ prefix
Constants          → UPPER_SNAKE_CASE:              MAX_SPAWN_DEPTH, DEFAULT_TIMEOUT_SECONDS
Private methods    → single underscore prefix
```

---

## 20. Hard rules — never do these

- No global state. No module-level singletons.
- No mutable default arguments.
- No `print()` — `structlog` only.
- No hardcoded model names outside spec definitions.
- No `time.sleep` in async code.
- No bare `except:`.
- No `# ty: ignore` without rule name + comment.
- No `Any` without comment.
- No `**kwargs` in public APIs — spell out every parameter.
- No circular imports.
- No relative imports outside `__init__.py`.
- No wildcard imports.
- No string concatenation for log messages.
- No inheritance for code reuse — composition only.
- No `core/` imports from sibling packages.
- **No user-facing imports from `pydantic_ai` or `faststream`. Only `murmur.interop` may import them.**

---

## 21. CLI surface (Phase 1)

```bash
murmur run script.py                # run a Python script (or `murmur run agent_name --input ...`)
murmur validate specs/              # validate all specs
murmur worker start --agents X      # start a distributed worker
```

Anything beyond these is out of Phase 1. (Phase 2 adds `murmur serve`. Phase 3 adds `murmur workflow run`.)

---

## 21a. Documentation — keep in sync

The docs site lives in `docs/` and is built with `mkdocs-material` (see `mkdocs.yml`). It ships alongside the code, not separately. When a change touches the public API, observable behaviour, or a documented decision, update the relevant page in the same PR — don't defer to "docs sweep later". The pages most likely to drift:

- `docs/api/*.md` — auto-rendered from docstrings via `mkdocstrings`. Adding a new public symbol means adding a `:::` directive on the matching page (see `docs/api/index.md` for the package map). Renaming or removing one means updating the directive.
- `docs/concepts/*.md` — long-form prose about architecture, runtime, agents, tools, events, cost, MCP. Update when behaviour or wiring changes.
- `docs/guides/*.md` — distributed / embedded / migration recipes. Update when the surface those recipes use changes.
- `docs/index.md` — landing page snippets must compile against the current API.

Verification: `uv sync --group docs && uv run mkdocs build --strict`. `--strict` catches dead links, missing pages in nav, and unresolved `mkdocstrings` references. The CI workflow (`.github/workflows/docs.yml`) runs the same on every PR.

If a change is *purely* internal (refactor, test-only, package layout) and doesn't move the public surface, docs probably don't need touching — but err toward updating when in doubt.

---

## 22. What NOT to build

Coding agents over-build. These are explicit non-goals.

**Already in PydanticAI — do not reimplement (and never expose):**
- LLM provider abstraction
- Structured output validation / retry on invalid output
- Tool execution **inside** the agent (Murmur proxies tools — different concern)
- MCP server integration

**Out of scope entirely:**
- Web UI / dashboard (Phase 2 ships a minimal read-only one)
- Authentication / user management
- Agent memory / RAG / vector store integration
- Custom logging framework (use `structlog` directly)
- Custom serialization (use Pydantic `model_dump` / `model_validate`)
- Plugin system / dynamic loading (Python imports are sufficient)

**Phased — not in MVP:**
- ContainerBackend / Docker SDK integration → **Phase 4**
- Group coordination tools (SharedMemoryTool, MessageBusTool, BarrierTool, VotingTool) → **Phase 3**
- YAML workflow engine with Jinja templating → **Phase 3**
- Cost-tracking / observability middleware, event emitters → **Phase 2**

---

## 23. Commit conventions

```
feat(core):     add spawn depth limit enforcement
fix(backends):  handle timeout in thread backend
chore(ci):      add ty check to pipeline
docs:           update CLAUDE.md
test(tools):    add property-based tests for tool resolution
refactor(ctx):  extract empty-context guard
```

Prefix matches directory: `core`, `backends`, `context`, `tools`, `worker`, `runtime`, `agent`, `registry`, `middleware`, `interop`, `cli`, `ci`, `docs`.

---

## 24. Common commands

```bash
# environment
uv sync --group dev          # install all deps + dev tools

# day-to-day
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
uv run pytest -m "not integration"
uv run pytest -m integration
uv run pre-commit run --all-files

# running murmur
uv run murmur run script.py
uv run murmur validate specs/
uv run murmur worker start --agents researcher --broker kafka://localhost:9092
```

---

## 25. Key design decisions to preserve

- Fan-out is handled by FastStream / broker, **not** a custom scheduler.
- No Go runtime. Fully Python.
- Agents never execute tools directly — always proxied through the runtime.
- Sub-agents processing external data must use `ContainerBackend` + `DenylistToolProvider` (Phase 4).
- Output schemas are Pydantic models — never free text between agents.
- Context passing is bidirectional — prepare before spawn, sanitize on result return.
- Cascading spawns enforce depth limit and budget cap at the **runtime** level, not the agent level.
- ThreadBackend and JobBackend both first-class from MVP. Same `Agent`, different runtime constructor.
- **Public API = `from murmur import ...`.** No leaks of `pydantic_ai` / `faststream` types outward.

---

## 26. When you (the agent) are unsure

1. Re-read the relevant section of this file.
2. Prefer the existing pattern over inventing a new one.
3. If a new abstraction seems necessary, check YAGNI — is there a second concrete use today?
4. If it touches `core/`, double-check the import direction.
5. If a new package needs to be created, confirm with the user — only Phase 1 packages should exist now.
6. Ask the user before adding a new top-level dependency.


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
