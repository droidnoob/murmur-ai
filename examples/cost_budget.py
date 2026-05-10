"""Cost budget — token caps and the BUDGET_EXCEEDED path.

Demonstrates :class:`TokenBudget` enforcement: set a tight ceiling, run an
agent until it saturates, and watch ``BudgetExceededError`` raise on the
next call. The runtime emits a ``BUDGET_EXCEEDED`` event before raising,
so any wired :class:`EventEmitter` (default ``LogEventEmitter``) sees the
saturation point.

Murmur's policy is **post-charge, not preempt** — the middleware can't
cancel mid-run, so one over-spend per saturation event is the documented
semantic. The next call's pre-check then hard-stops.

See also: ``docs/concepts/cost.md``.

Prereqs:
    pip install murmur-runtime
    export ANTHROPIC_API_KEY=...

Run:
    python examples/cost_budget.py
"""

import asyncio
import os
import sys

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.core.errors import BudgetExceededError
from murmur.middleware.cost_tracking import TokenBudget
from murmur.runtime import RuntimeOptions


class HaikuOut(BaseModel):
    haiku: str


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    poet = Agent(
        name="haiku-poet",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions="Write a single short haiku about the topic.",
        output_type=HaikuOut,
        trust_level=TrustLevel.LOW,
    )

    budget = TokenBudget(limit=200)
    runtime = AgentRuntime(options=RuntimeOptions(token_budget=budget))

    topics = ["winter rain", "city lights", "old library", "morning coffee"]

    for topic in topics:
        print(f"--- topic: {topic} (remaining={budget.remaining}) ---")
        try:
            result = await runtime.run(poet, TaskSpec(input=topic))
        except BudgetExceededError as exc:
            print(f"  budget exceeded: {exc}")
            break
        if result.is_ok():
            assert isinstance(result.output, HaikuOut)
            print(f"  {result.output.haiku}")
            print(f"  charged {result.metadata.tokens_used} tokens")
        else:
            print(f"  failure: {result.error}")
            break

    print(f"final used={budget.used} of limit={budget.limit}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
