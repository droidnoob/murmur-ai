"""LLM-driven fan-out — orchestrator agent delegates via spawn_agents.

Wires :func:`make_spawn_agents_tool` so an orchestrator's LLM can decompose
a task and dispatch child agents in parallel. Trust level, model, and tool
surface come from the bound :class:`AgentTemplate` — the LLM picks
``name`` / ``instructions`` / ``input`` per child and nothing else.

Children inherit the template's ``pre_instruction`` (here, a JSON-only
preamble) and the configured ``output_type``. Per-child failures land in
the returned ``SpawnResult(success=False, error=…)`` rather than raising,
so partial fan-outs always come back to the orchestrator for aggregation.

See also: ``docs/concepts/agents.md`` (Templates + LLM-driven fan-out
sections), ``docs/api/tools.md``.

Prereqs:
    pip install murmur-ai
    export ANTHROPIC_API_KEY=...

Run:
    python examples/spawn_agents.py
"""

import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import AgentRuntime, AgentTemplate, TaskSpec, TrustLevel
from murmur.tools import make_spawn_agents_tool


class CapitalFact(BaseModel):
    """One child's structured response — the unit the orchestrator aggregates."""

    country: str
    capital: str
    fun_fact: str = Field(description="One short, surprising fact.")


class TravelBriefing(BaseModel):
    """Orchestrator's final output — a roll-up across the children."""

    summary: str
    entries: list[CapitalFact]


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    runtime = AgentRuntime()

    swarm = AgentTemplate(
        pre_instruction=(
            "You are a research minion. Be concise. Return JSON only. "
            "Do not call tools beyond what's been provided."
        ),
        model="anthropic:claude-haiku-4-5-20251001",
        trust_level=TrustLevel.LOW,
    )

    spawn = make_spawn_agents_tool(
        runtime=runtime,
        template=swarm,
        output_type=CapitalFact,
        max_concurrency=4,
    )
    runtime.tools.register("spawn_agents", spawn)

    orchestrator = swarm.agent(
        name="travel-orchestrator",
        instructions=(
            "You build a travel briefing. Decompose the user's request into "
            "one child per country: each child's `instructions` should ask "
            "for that country's capital + one fun fact, and the child's "
            "`input` should name the country. After the children return, "
            "aggregate them into a TravelBriefing with a one-sentence summary."
        ),
        output_type=TravelBriefing,
        tools=frozenset({"spawn_agents"}),
    )

    result = await runtime.run(
        orchestrator,
        TaskSpec(input="Build a briefing for Iceland, Japan, and Argentina."),
    )

    if not result.is_ok():
        print(f"orchestrator failed: {result.error}", file=sys.stderr)
        return 1

    briefing = result.output
    assert isinstance(briefing, TravelBriefing)
    print(f"summary: {briefing.summary}\n")
    for entry in briefing.entries:
        print(f"  {entry.country} — capital: {entry.capital}")
        print(f"    fun fact: {entry.fun_fact}")
    print(
        f"\n— {result.metadata.duration_ms} ms total, "
        f"{result.metadata.tokens_used or 0} tokens (orchestrator only; "
        "child token counts surface through the BUDGET_EXCEEDED path when a "
        "TokenBudget is wired)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
