# Coordination

Murmur ships **two primitives plus one tool factory** for multi-agent
coordination. Pick the one whose mental model matches your problem; don't reach
for the others.

| Need | Pick |
|---|---|
| Single agent calling tools | plain `Agent` |
| One agent, many similar tasks (fan-out) | `runtime.gather` |
| Typed pipeline with explicit stages, deterministic flow | `AgentGroup` |
| Named coordinator + closed menu of pre-built workers (CrewAI-style) | `AgentTeam` |
| LLM should *invent* workers per request | `Agent` + `make_spawn_agents_tool` |
| Consensus, voting, debate, LLM-as-judge | build from `AgentGroup` |
| Iterative refinement / loop-back | `AgentTeam` with `max_rounds > 1` |

Two dispatch surfaces, no overlap:

- `runtime.run(agent, task)` — single agent, possibly with delegation tools
  registered.
- `runtime.run_group(spec, task)` — declarative coordination. Polymorphic on
  whether `spec` is an `AgentGroup` or an `AgentTeam`.

## When to coordinate at all

```
single agent?
   yes → Agent + runtime.run
   no  ↓

same task, many inputs?
   yes → runtime.gather(agent, [TaskSpec(input=q) for q in questions])
   no  ↓

deterministic stages with typed contracts between them?
   yes → AgentGroup
   no  ↓

named coordinator picking from a closed menu of workers?
   yes → AgentTeam
   no  ↓

LLM invents its own workers per request?
   yes → Agent + make_spawn_agents_tool
```

If you can't answer a hard "yes" to any of these, the coordination shape
hasn't crystallised yet — build the simplest single-agent version first and
let the seams emerge.

## `AgentGroup` — typed DAG

A frozen DAG of agents connected by `Edge` arrows. Construction validates
topology (no cycles, every edge target is in the graph, every node has a
declared output_type); the runner walks it tier by tier and dispatches each
node through `runtime.run` or `runtime.gather`.

```python
from murmur import Agent, AgentGroup, AgentRuntime, Edge, FanOut, TaskSpec
from pydantic import BaseModel

class SubQuestion(BaseModel):
    question: str

class Decomposition(BaseModel):
    sub_questions: FanOut[list[SubQuestion]]

class Finding(BaseModel):
    answer: str

class FinalReport(BaseModel):
    summary: str
    findings_count: int

researcher = Agent(name="researcher", model="anthropic:claude-sonnet-4-6",
                   instructions="Decompose into sub-questions.",
                   output_type=Decomposition)
analyst = Agent(name="analyst", model="anthropic:claude-sonnet-4-6",
                instructions="Answer one sub-question.", output_type=Finding)
synthesizer = Agent(name="synthesizer", model="anthropic:claude-sonnet-4-6",
                    instructions="Synthesise the findings into a final report.",
                    output_type=FinalReport)

def findings_to_summary(findings: list[Finding]) -> TaskSpec:
    return TaskSpec(input=f"Summarise {len(findings)} findings")

crew = AgentGroup(
    name="research-crew",
    topology={
        researcher:  Edge(to=(analyst,)),                      # auto fan-out via FanOut field
        analyst:     Edge(to=(synthesizer,), mapper=findings_to_summary),
        synthesizer: Edge.terminal(),
    },
)

runtime = AgentRuntime()
result = await runtime.run_group(crew, TaskSpec(input="Why do LLM agents fail?"))
print(result.output.summary)
```

### Tier-parallel sibling dispatch

Sibling nodes within one topology tier dispatch concurrently via
`asyncio.gather` (the linear single-node fast path stays sequential). The
contract is "results stored, terminal returned", never "node X ran before
node Y". A failure inside any sibling cancels the rest of the tier and
propagates the original exception with its type intact.

### Heterogeneous fan-out

Declare a fan-out source whose item type is a union — each item routes to
the downstream agent whose `Agent.input_type` matches:

```python
from typing import Union

class Question(BaseModel):
    text: str

class Statement(BaseModel):
    claim: str

class Mixed(BaseModel):
    items: FanOut[list[Question | Statement]]

source       = Agent(..., output_type=Mixed)
q_handler    = Agent(..., input_type=Question,  output_type=Resolution)
s_handler    = Agent(..., input_type=Statement, output_type=Resolution)
synthesizer  = Agent(..., output_type=FinalReport)

crew = AgentGroup(
    name="hetero",
    topology={
        source:      Edge(to=(q_handler, s_handler)),
        q_handler:   Edge(to=(synthesizer,), mapper=...),
        s_handler:   Edge(to=(synthesizer,)),
        synthesizer: Edge.terminal(),
    },
)
```

The construction-time validator rejects ambiguous unions (subclass
relationships between members), missing handlers, orphan handlers, and
conditional edges from heterogeneous sources — items that fail routing
surface as `SpecValidationError` rather than silently dropping.

### Multi-terminal — `GroupResult`

When a topology fires N>=2 leaves at runtime (moderator-and-specialists,
parallel branches whose conditions both fire), `run_group` returns a
[`GroupResult`](../api/agent.md#groupresult) keyed by `Agent.name` with
aggregate metadata. Single-leaf topologies — including branch-routing where
exactly one predicate fires — still return `AgentResult`.

```python
result = await runtime.run_group(crew, TaskSpec(input="..."))
if isinstance(result, GroupResult):
    for leaf_name, leaf in result.outputs.items():
        print(leaf_name, leaf.output)
else:
    print(result.output)
```

The runtime decides which shape based on how many terminals fired, not on
the topology shape itself — a two-leaf topology where one branch is gated
by a `False` condition still returns `AgentResult`.

## `AgentTeam` — coordinator + closed menu

CrewAI-style hierarchical: one coordinator agent picks targets from a
closed `Mapping[str, Agent]` of delegates. The runtime auto-registers a
typed `delegate(target, input)` tool on the coordinator's surface; the LLM
picks a name from a `Literal` enum, supplies typed input, gets typed
output back, and synthesises against `team.output_type`.

```python
from murmur import Agent, AgentRuntime, AgentTeam, TaskSpec
from pydantic import BaseModel

class BillingInput(BaseModel):
    invoice_id: str

class TechnicalInput(BaseModel):
    error_code: str

class Resolution(BaseModel):
    summary: str

triage    = Agent(name="triage",    model="anthropic:claude-sonnet-4-6",
                  instructions="Route the customer's issue to the right delegate.",
                  output_type=Resolution)
billing   = Agent(name="billing",   input_type=BillingInput,
                  output_type=Resolution, model="anthropic:claude-sonnet-4-6",
                  instructions="Resolve billing issues.")
technical = Agent(name="technical", input_type=TechnicalInput,
                  output_type=Resolution, model="anthropic:claude-sonnet-4-6",
                  instructions="Resolve technical issues.")

team = AgentTeam(
    name="customer-support",
    coordinator=triage,
    delegates={"billing": billing, "technical": technical},
    output_type=Resolution,
    max_rounds=5,
)

runtime = AgentRuntime()
result = await runtime.run_group(team, TaskSpec(input="my invoice INV-42 is wrong"))
print(result.output.summary)
```

### Per-delegate session memory

By default each delegate retains conversation history *within one*
`runtime.run_group(team, ...)` call. When `triage` calls `delegate("billing",
X)` twice, the billing agent sees the prior exchange on the second call via
`AgentContext.messages`. Set `retain_delegate_history=False` to make every
delegate dispatch independent.

History is strictly per-run — distinct `run_group(team, ...)` calls never
share state. Cross-run persistence (RAG, vector stores, "remember the
user") stays explicitly out of scope; build it as a tool against your own
store.

### Validators that fire at construction

`AgentTeam` rejects four configurations eagerly:

- empty `delegates` mapping
- the coordinator listed as one of its own delegates
- any delegate without `Agent.input_type` declared (typed routing requires it)
- two delegates claiming the same `input_type` (ambiguous routing)

The auto-generated tool also enforces a runtime guard: if the LLM picks
`delegate("billing", TechnicalInput(...))` (schema-valid under the raw
`Literal+Union`, but semantically wrong), the call is rejected as
`ToolExecutionError` before dispatch.

### `max_rounds` budget

`max_rounds` (default 10) caps total `delegate()` calls per team run. A
runaway coordinator can't burn through delegates indefinitely. Independent
of `RuntimeOptions.max_spawn_depth`, which still bounds total cascade
depth across the runtime.

### Distributed semantics

`AgentTeam` dispatch currently requires `AsyncBackend` (in-process). The
modified coordinator and the per-run delegate tool are publisher-side
constructs that don't survive the broker hop; `JobBackend` ships only
`Agent.name` over the wire, so the worker never sees the team's wiring.
`run_group(team, ...)` raises `NotImplementedError` with a clear message
when the runtime is broker-backed.

For distributed multi-agent workflows, use `AgentGroup` — every
`runtime.run` and `runtime.gather` call inside the runner crosses the
broker uniformly.

### Migrating from CrewAI

| CrewAI | Murmur (post-coordination v2) |
|---|---|
| `Process.sequential` + `Crew(tasks=[...])` | `AgentGroup` linear topology |
| `Process.hierarchical` + `manager_llm` / `manager_agent` | `AgentTeam` |
| `Agent.allowed_agents=[...]` (string roles, prompt-augmented) | `AgentTeam.delegates={"name": agent}` (structural `Literal`) |
| Manager LLM picks via prompt | Coordinator LLM picks via `Literal`-typed tool argument |
| Sequential delegation | Parallel — LLM emits multiple `delegate()` calls in one turn → `asyncio.gather` |
| Single-process | Single-process for `AgentTeam`; `AgentGroup` works distributed |
| Optional `output_pydantic=` per Task | `Agent.output_type` mandatory |
| `allow_delegation=False` (effectively depth=1) | `RuntimeOptions.max_spawn_depth`, `max_total_spawns`, `cycle_policy` |
| `Process.consensual` (planned, unimplemented) | Build from `AgentGroup` parallel proposers + synthesiser terminal |
| `memory=True` (vector store + chat history fused) | Per-team session memory in `AgentTeam` (default); cross-run persistence is your tool |

## `make_spawn_agents_tool` — open-ended dispatch

When the LLM should *invent* the menu per request rather than picking from
a closed set, register the spawn tool on a parent agent. Children are
materialised from an `AgentTemplate` so trust level, model, and tool
surface come from your config — not the LLM's call.

```python
from murmur import Agent, AgentRuntime, AgentTemplate, TaskSpec
from murmur.tools import make_spawn_agents_tool
from pydantic import BaseModel

class Finding(BaseModel):
    answer: str

class FinalReport(BaseModel):
    summary: str

worker_template = AgentTemplate(
    model="anthropic:claude-sonnet-4-6",
    pre_instruction="You are a research worker. Be terse.",
)

runtime = AgentRuntime()
spawn = make_spawn_agents_tool(
    runtime=runtime,
    template=worker_template,
    output_type=Finding,
    max_concurrency=10,
)
runtime.tool_registry.register("spawn_agents", spawn)

moderator = Agent(
    name="moderator",
    model="anthropic:claude-sonnet-4-6",
    instructions="Decompose the question; call spawn_agents for each subtopic.",
    output_type=FinalReport,
    tools=frozenset({"spawn_agents"}),
)

result = await runtime.run(moderator, TaskSpec(input="Why are LLM agents brittle?"))
print(result.output.summary)
```

The factory bounds what the LLM can spawn: it picks `name`, `instructions`,
and `input` per child — nothing else. Per-child failures roll up into
`SpawnResult(success=False, error=...)`; the moderator decides whether to
retry, re-route, or surface them in the synthesis.

## Building common patterns from primitives

### Voting / consensus

Parallel proposers feed a synthesiser terminal:

```python
crew = AgentGroup(
    name="vote",
    topology={
        question: Edge(to=(proposer_a, proposer_b, proposer_c),
                       mapper=lambda q: TaskSpec(input=q.text)),
        proposer_a: Edge(to=(synth,), mapper=tally_votes),
        proposer_b: Edge(to=(synth,)),
        proposer_c: Edge(to=(synth,)),
        synth:      Edge.terminal(),
    },
)
```

The synthesiser receives all proposers' outputs and applies whatever
voting rule fits — majority, weighted, ranked-choice. CrewAI's planned
`Process.consensual` is the same shape; build it once, share the topology.

### LLM-as-judge

A single judge as terminal:

```python
crew = AgentGroup(
    name="judge",
    topology={
        candidate: Edge(to=(judge,), mapper=judge_input),
        judge:     Edge.terminal(),
    },
)
```

The judge's `output_type` carries the verdict; cycle through
`AgentGroup`s if you want multi-round refinement.

### Iterative refinement

`AgentTeam` with `max_rounds > 1` and a coordinator that decides whether
to dispatch again or finalise:

```python
team = AgentTeam(
    name="refine",
    coordinator=editor,                          # decides "again or done"
    delegates={"writer": writer, "fact_checker": checker},
    output_type=FinalDraft,
    max_rounds=5,
)
```

The editor's instructions tell it to call `delegate("writer", ...)` then
`delegate("fact_checker", ...)`, inspect outputs, and either iterate or
emit the final draft.

### Sequential pipeline

`AgentGroup` with linear topology — same as the single-pass research
example above. No special construct.

### Cross-run memory

Persistent memory (vector stores, RAG, "remember the user across
sessions") is **not** a Murmur primitive. Build it as a tool against
your own store; the agent dispatches `recall_memory(query)` /
`store_memory(key, value)` like any other tool, and the runtime stays
out of the persistence path.

```python
runtime.tool_registry.register("recall_memory", recall_memory)
runtime.tool_registry.register("store_memory", store_memory)

librarian = Agent(
    name="librarian",
    instructions="Always recall before answering; persist new facts.",
    tools=frozenset({"recall_memory", "store_memory"}),
    output_type=FinalAnswer,
)

# Two independent runs. The store persists across them via the tool's
# closure — Murmur's AgentContext.messages is per-run only.
await runtime.run(librarian, TaskSpec(input="Remember teal."))
await runtime.run(librarian, TaskSpec(input="What's my colour?"))
```

See [`examples/memory_via_tool.py`](https://github.com/anthropics/murmur-runtime/blob/main/examples/memory_via_tool.py)
for a complete runnable example with a stub store you swap for Chroma,
sqlite-vec, pgvector, or Redis at the closure boundary. The runtime sees
two independent dispatches; the persistence is yours.

This boundary is intentional. CrewAI's ``memory=True`` collapses
short-term context, long-term embeddings, and entity memory under one
flag; Murmur splits the layers so the trade-offs stay visible:

- `AgentContext.messages` — per-run conversation context (current).
- `AgentTeam.retain_delegate_history` — per-team-run delegate session
  memory (added with `AgentTeam`).
- Cross-run memory — your tool, your store.

## What every coordination primitive shares

The runtime applies the same guarantees to every dispatch — single agent,
group node, team delegate, spawn-tool child:

- **Cascading-spawn graph.** Each `runtime.run` reads the parent frame
  from a contextvar and derives `AgentContext.depth`,
  `AgentContext.parent_agent`, `AgentContext.parent_trace_id`, and
  `AgentContext.ancestors`. Every event carries the parent trace id so
  observability backends can stitch a cascading run into a single tree.
  See [runtime](runtime.md).
- **Cycle detection.** Name-based — if the target agent already appears
  on the parent chain, the runtime rejects with `SpawnCycleError` before
  any backend work. Opt into `cycle_policy="permissive"` if you have a
  legitimate bounded-reuse pattern; depth and cap remain enforced.
- **Depth limit.** `RuntimeOptions.max_spawn_depth` caps cascade depth.
  `DepthLimitMiddleware` rejects past the limit.
- **Spawn cap.** `RuntimeOptions.max_total_spawns` is the runtime-wide
  kill switch — once exhausted, `SpawnCapError`. Defaults to `None`
  (unbounded) so long-lived workers don't self-brick.
- **Token budget.** `RuntimeOptions.token_budget` enforces a runtime-wide
  ceiling. Pre-check + post-charge keeps the meter accurate; exhausted
  budgets fire `BudgetExceededError` and a `BUDGET_EXCEEDED` event.
- **Signed envelopes.** `RuntimeOptions.broker_signing_key` enables
  HMAC-signed `TaskMessage`s — opt-in, off by default. See
  [security](../security.md).
- **Per-batch timeout.** `RuntimeOptions.timeout_seconds` wraps every
  `runtime.run` and `runtime.gather` batch.

These hold across all three coordination shapes — you don't lose them
by picking `AgentTeam` over `AgentGroup` over the spawn tool.
