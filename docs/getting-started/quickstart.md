# Quickstart

Five minutes from empty directory to a structured agent answer. By the
end you'll have run a single agent locally, then fanned the same agent
across many tasks. Distributed mode is one constructor change away — see
[Distributed deployments](../guides/distributed.md) for that.

## 1. Bootstrap a project

```bash
uv init my-murmur-app
cd my-murmur-app
uv add murmur-ai
export ANTHROPIC_API_KEY=...
```

`murmur-ai` pulls in PydanticAI as a transitive — you don't import from
it directly. AsyncBackend works out of the box; broker extras
(`murmur-ai[kafka]` etc.) come later when you go distributed.

## 2. Define the agent

Create `quickstart.py` (this matches [`examples/quickstart.py`](https://github.com/murmur-ai/murmur/blob/main/examples/quickstart.py)
verbatim):

```python
import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel


class CapitalLookup(BaseModel):
    """Structured output schema. The LLM call retries until it produces
    a value that validates against this model."""

    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)
    fun_fact: str


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY is not set.", file=sys.stderr)
        return 2

    geographer = Agent(
        name="geographer",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions=(
            "You answer geography questions. Return the capital city, your "
            "confidence (0.0-1.0), and one short, surprising fact."
        ),
        output_type=CapitalLookup,
        trust_level=TrustLevel.LOW,
    )

    runtime = AgentRuntime()  # AsyncBackend — no broker, in-process.

    result = await runtime.run(
        geographer,
        TaskSpec(input="What is the capital of Iceland?"),
    )

    if not result.is_ok():
        print(f"agent failed: {result.error}", file=sys.stderr)
        return 1

    answer = result.output
    assert isinstance(answer, CapitalLookup)
    print(f"{answer.country}: {answer.capital}  (confidence {answer.confidence:.2f})")
    print(f"  fun fact: {answer.fun_fact}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
```

Run it:

```bash
uv run python quickstart.py
```

Output (yours will differ on the fun fact):

```
Iceland: Reykjavík  (confidence 1.00)
  fun fact: Reykjavík was the world's first capital lit by geothermal energy.
```

## 3. What just happened

- `Agent` is a frozen Pydantic value object. It carries the model
  string, instructions, output schema, and trust level — all as data,
  no callables. You can serialise it, ship it across the wire, and
  reconstruct it identically.
- `AgentRuntime()` with no arguments uses
  [`AsyncBackend`](../concepts/backends.md#asyncbackend) — no broker,
  no external services, runs entirely in `asyncio`.
- `runtime.run` returns `AgentResult[CapitalLookup]`. Either `output`
  is set (success) or `error` is set (failure) — never both. Use
  `is_ok()` to discriminate.
- `output_type=CapitalLookup` drives PydanticAI's structured-output
  retry loop. The LLM is re-prompted until the response validates.

## 4. Fan out across many tasks

Add this below `main()`:

```python
QUESTIONS = [
    "What is the capital of Iceland?",
    "What is the capital of Japan?",
    "What is the capital of Argentina?",
    "What is the capital of Botswana?",
    "What is the capital of Bhutan?",
]

results = await runtime.gather(
    geographer,
    tasks=[TaskSpec(input=q) for q in QUESTIONS],
    max_concurrency=3,
)

for r in results:
    if r.is_ok():
        assert isinstance(r.output, CapitalLookup)
        print(f"{r.output.country}: {r.output.capital}")
    else:
        print(f"failure: {r.error}")
```

`gather` runs the same agent over many tasks with bounded concurrency.
Partial failures don't take the batch down — each `AgentResult` is
independently OK-or-error.

## 5. Try a tiny zero-cost first run

Don't have a key? Swap the model string for PydanticAI's `test`
pseudo-model — it returns canned responses without calling a provider:

```python
geographer = Agent(
    name="geographer",
    model="test",
    instructions="...",
    output_type=CapitalLookup,
)
```

Useful for CI smoke tests. The output won't be meaningful, but the
runtime, validation, and event flow are exercised.

## Where to next

- **Run it distributed** — same agent, broker URL: [Distributed deployments](../guides/distributed.md).
- **Mount it in an existing app** — FastAPI integration: [Embedded mode](../guides/embedded.md).
- **Decompose work via the LLM** — orchestrator + child agents: [Agents — LLM-driven fan-out](../concepts/agents.md#llm-driven-fan-out-with-spawn_agents).
- **See every spawn / tool call** — wire `SSEEventEmitter`: [Events](../concepts/events.md).
- **Cap costs** — `TokenBudget` enforcement: [Cost tracking](../concepts/cost.md).
