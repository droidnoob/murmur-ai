# Quickstart

In five minutes you'll define an agent, run it locally, and run it
distributed. Same code in both cases.

## 1. Define the agent

```python
from murmur import Agent
from pydantic import BaseModel


class ResearchFinding(BaseModel):
    question: str
    answer: str
    confidence: float
    sources: list[str]


researcher = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions=(
        "Research the question. Cite sources. "
        "Be honest about uncertainty in the confidence field."
    ),
    output_type=ResearchFinding,
)
```

The agent is a frozen value object — you can serialise it, ship it across
the wire, and reconstruct it identically. There's no separate "spec" vs
"instance"; the [`Agent`](../concepts/agents.md) class is the unified
unit.

## 2. Run it locally

```python
import asyncio

from murmur import AgentRuntime, TaskSpec


async def main() -> None:
    runtime = AgentRuntime()                    # ThreadBackend (asyncio)

    result = await runtime.run(
        researcher,
        TaskSpec(input="What at-least-once guarantees does NATS JetStream provide?"),
    )

    if result.is_ok():
        print(result.output.answer)
        print(f"confidence: {result.output.confidence:.2f}")
    else:
        print(f"error: {result.error}")


asyncio.run(main())
```

`AgentRuntime()` with no arguments uses [`ThreadBackend`](../concepts/backends.md) —
no broker, no external services, runs entirely in `asyncio`.

## 3. Fan out

```python
results = await runtime.gather(
    researcher,
    tasks=[TaskSpec(input=q) for q in QUESTIONS],
    max_concurrency=20,
)

ok = [r.output for r in results if r.is_ok()]
failed = [r.error for r in results if not r.is_ok()]
```

`gather` runs the same agent over many tasks with bounded concurrency.
Partial failures don't take the batch down — each `AgentResult` is
independently OK-or-error.

## 4. Run it distributed

```python
runtime = AgentRuntime(broker="kafka://localhost:9092")
results = await runtime.gather(researcher, tasks=tasks, max_concurrency=100)
```

Only the constructor changed. The agent is the same; the workflow is the
same. Murmur's [`JobBackend`](../concepts/backends.md#jobbackend) parses
the broker URL, publishes one `TaskMessage` per task, and aggregates
results. A worker process subscribes:

```bash
murmur worker start \
    --agents researcher \
    --broker kafka://localhost:9092 \
    --concurrency 20
```

The worker uses a thread-mode runtime internally — `JobBackend` is a
transport for `ThreadBackend` invocations across machines. See the
[Distributed deployments guide](../guides/distributed.md) for production
patterns.

## 5. Observe what happened

Every spawn, tool call, and completion flows through a typed
[`RuntimeEvent`](../concepts/events.md):

```python
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

sse = SSEEventEmitter(heartbeat_interval=15.0)
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)
```

Hand `sse.subscribe()` to a FastAPI `EventSourceResponse` and you have a
live event stream into a browser. Or use `murmur serve` to get one out
of the box — see the [Events concept page](../concepts/events.md).

## What's next

- [Architecture](../concepts/architecture.md) — pipeline + middleware mental model.
- [Agents](../concepts/agents.md) — every field on the unified `Agent` class.
- [Cost tracking](../concepts/cost.md) — `TokenBudget` and per-runtime limits.
- [MCP](../concepts/mcp.md) — using Model Context Protocol servers as tool sources.
