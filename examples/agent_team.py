"""AgentTeam — coordinator agent delegating to typed specialists.

Builds a 1-coordinator / 2-delegate team. The coordinator receives a
free-form question and uses an auto-generated ``delegate(target=..., input=...)``
tool to route work to the right specialist. Each delegate declares its own
``input_type`` (a Pydantic model) so the coordinator's argument schema is
typed at the LLM tool-call boundary — no JSON spelunking on either side.

The team returns a single :class:`AgentResult` validated against the
team's ``output_type``. Per-delegate session memory is preserved across
the coordinator's tool calls within one ``run_group()`` invocation
(``retain_delegate_history=True`` by default).

Pairs with: ``docs/concepts/coordination.md`` (`AgentTeam` section).

Prereqs:
    pip install murmur-runtime
    export ANTHROPIC_API_KEY=...

Run:
    python examples/agent_team.py
"""

import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, AgentTeam, TaskSpec, TrustLevel


# Per-delegate input schemas — what the coordinator passes through the
# auto-generated ``delegate`` tool. Each delegate must have a unique
# input_type; that's how AgentTeam wires the typed Literal target.
class GeoQuestion(BaseModel):
    country: str = Field(description="Country to look up.")


class HistoryQuestion(BaseModel):
    topic: str = Field(description="Person, place, or event to give a fact about.")


# Delegate output schemas.
class GeoAnswer(BaseModel):
    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)


class HistoryAnswer(BaseModel):
    fact: str
    score: float = Field(ge=0.0, le=1.0)


# Final coordinator output — what run_group(team, ...) ultimately returns.
class Briefing(BaseModel):
    summary: str
    score: float = Field(ge=0.0, le=1.0)


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    model = "anthropic:claude-haiku-4-5-20251001"

    geographer = Agent(
        name="geographer",
        model=model,
        instructions=(
            "You answer geography questions. Return the country, capital city, "
            "and your confidence (0..1)."
        ),
        input_type=GeoQuestion,
        output_type=GeoAnswer,
        trust_level=TrustLevel.LOW,
    )
    historian = Agent(
        name="historian",
        model=model,
        instructions=(
            "Give one short historical fact about the topic, plus a 0..1 score "
            "for how confident you are it's correct."
        ),
        input_type=HistoryQuestion,
        output_type=HistoryAnswer,
        trust_level=TrustLevel.LOW,
    )
    coordinator = Agent(
        name="briefer",
        model=model,
        instructions=(
            "You build a one-paragraph briefing about a country. You have two "
            "delegates: 'geo' (geography facts) and 'hist' (historical facts). "
            "Call delegate(target='geo', input={'country': ...}) and "
            "delegate(target='hist', input={'topic': ...}), then synthesise the "
            "results into a single briefing with a confidence score."
        ),
        output_type=Briefing,
        # MEDIUM is the conventional level for coordinators — they invoke
        # tools (the auto-generated `delegate`) but don't touch external IO
        # directly. Delegates can stay at LOW.
        trust_level=TrustLevel.MEDIUM,
    )

    team = AgentTeam(
        name="country-briefing",
        coordinator=coordinator,
        delegates={"geo": geographer, "hist": historian},
        output_type=Briefing,
        # ``max_rounds`` caps how many delegate calls the coordinator can
        # make per ``run_group``. Independent of ``RuntimeOptions.max_spawn_depth``.
        max_rounds=4,
    )

    runtime = AgentRuntime()
    result = await runtime.run_group(
        team, TaskSpec(input="Write a briefing about Iceland.")
    )
    # ``AgentTeam`` always resolves through its coordinator, so ``run_group``
    # returns an :class:`AgentResult` here. Multi-terminal :class:`GroupResult`
    # is only possible when running an :class:`AgentGroup` with >=2 leaves.
    from murmur import AgentResult

    assert isinstance(result, AgentResult)

    if result.is_ok():
        assert isinstance(result.output, Briefing)
        print(f"summary: {result.output.summary}")
        print(f"score:   {result.output.score:.2f}")
        print(f"tokens:  {result.metadata.tokens_used}")
        return 0
    print(f"FAIL: {result.error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
