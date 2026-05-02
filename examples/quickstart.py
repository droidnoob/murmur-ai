"""Quickstart — Murmur in thread mode.

Run a single agent against a single structured task, locally, with no broker.
This is the simplest possible Murmur program — one ``AgentRuntime``, one
``Agent``, one ``TaskSpec``. No external services. No worker process.

See also: ``docs/concepts/runtime.md``, ``docs/concepts/agents.md``.
Pairs with the tutorial at ``docs/getting-started/quickstart.md``.

Prereqs:
    pip install murmur-ai
    export ANTHROPIC_API_KEY=...

Run:
    python examples/quickstart.py
"""

import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel


class CapitalLookup(BaseModel):
    """Structured output schema. The LLM call will be retried by PydanticAI
    until it produces a value that validates against this model."""

    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)
    fun_fact: str


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
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

    runtime = AgentRuntime()  # AsyncBackend — no broker, runs in-process.

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
    print(
        f"  — {result.metadata.duration_ms} ms, "
        f"{result.metadata.tokens_used or 0} tokens"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
