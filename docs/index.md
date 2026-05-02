---
hide:
  - navigation
  - toc
---

# Murmur

> **Agents that move as one.**

Murmur is a Python multi-agent orchestration runtime. Not a framework for defining
agent behaviour — **infrastructure** for spawning, distributing, and coordinating
LLM-based agents reliably at scale. Think of it as a **hypervisor for LLM agents**:
spawn it, give it context, get a structured result back, kill it if needed.

PydanticAI handles single-agent execution. FastStream handles broker-backed
distribution. Murmur owns the orchestration layer between them — and hides both
behind its own public API.

---

## Install

```bash
pip install murmur-ai
```

ThreadBackend works out of the box with no broker. Add a broker extra when you're
ready to distribute:

```bash
pip install "murmur-ai[kafka]"      # or [nats], [rabbitmq], [redis], [all]
```

## A first agent

```python
from murmur import Agent, AgentRuntime, TaskSpec
from pydantic import BaseModel


class ResearchFinding(BaseModel):
    question: str
    answer: str
    confidence: float


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="Research the question. Cite sources. Be honest about uncertainty.",
    output_type=ResearchFinding,
)

runtime = AgentRuntime()
result = await runtime.run(researcher, TaskSpec(input="What is NATS JetStream?"))

if result.is_ok():
    print(result.output)        # ResearchFinding
```

## Same code, distributed

```python
runtime = AgentRuntime(broker="kafka://localhost:9092")
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in questions],
    max_concurrency=100,
)
```

The agent doesn't change. The workflow doesn't change. Only the runtime
constructor changes.

## Why Murmur

- **One unified `Agent` class.** Wraps PydanticAI internally; you never import
  from `pydantic_ai`.
- **Strict I/O contracts.** Every agent input and output is Pydantic-validated.
  No free text passes between agents.
- **Tools execute in the runtime, not the agent.** Trust enforcement, rate
  limiting, and observability are uniform.
- **Same code, local or distributed.** ThreadBackend (`asyncio`) and JobBackend
  (FastStream + Kafka / NATS / RabbitMQ / Redis) are both first-class.
- **Observable by default.** Every spawn, tool call, and completion flows
  through a typed `RuntimeEvent` to swappable emitters (`Log` / `SSE` / `Multi`
  / broker bridge).
- **Cost-aware.** `TokenBudget` enforces per-runtime ceilings with pre-check +
  post-charge semantics.

[:material-rocket-launch: API reference](api/index.md){ .md-button .md-button--primary }
[:material-github: Source on GitHub](https://github.com/murmur-ai/murmur){ .md-button }
