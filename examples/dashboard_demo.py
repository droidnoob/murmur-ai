"""Dashboard demo — real agents driving a real EventStore.

Boots an :class:`AgentServer` with the dashboard mounted at
``/dashboard``, an :class:`InMemoryEventStore` wired into the runtime's
emitter chain, and a small swarm of agents that actually go through
``runtime.run`` / ``runtime.gather`` against a local LM Studio endpoint.
Every panel on the dashboard reflects events those agents emit.

LM Studio default: ``http://localhost:1234/v1``. Override with
``--base-url`` and ``--model`` if you have a different model loaded.

Run:
    cd packages/dashboard && npm install && npm run build && cd ../..
    python examples/dashboard_demo.py
    # then open http://127.0.0.1:8420/dashboard/
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# When running ``python examples/dashboard_demo.py`` Python prepends the
# script's directory to ``sys.path``. That makes ``examples/mcp.py``
# shadow the real ``mcp`` package the murmur tools layer imports. Strip
# the examples dir before any murmur import to avoid the collision.
_EXAMPLES_DIR = str(Path(__file__).resolve().parent)
sys.path[:] = [p for p in sys.path if p != _EXAMPLES_DIR and p != ""]

from pydantic import BaseModel  # noqa: E402

from murmur import (  # noqa: E402
    Agent,
    AgentGroup,
    AgentRuntime,
    Edge,
    TaskSpec,
    TrustLevel,
)
from murmur.context.null import NullContextPasser  # noqa: E402
from murmur.events import (  # noqa: E402
    LogEventEmitter,
    MultiEventEmitter,
    SSEEventEmitter,
)
from murmur.events.store import InMemoryEventStore, StoreEventEmitter  # noqa: E402
from murmur.middleware.cost_tracking import TokenBudget  # noqa: E402
from murmur.models import OpenAIChatModel  # noqa: E402
from murmur.providers import OpenAIProvider  # noqa: E402
from murmur.runtime import RuntimeOptions  # noqa: E402
from murmur.server.app import AgentServer  # noqa: E402
from murmur.types import FanOut  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = REPO_ROOT / "packages" / "dashboard" / "dist"


# ---------------------------------------------------------------------------
# Agent definitions — three small specialists with distinct personalities.
# Each emits AGENT_SPAWNED / AGENT_COMPLETED through runtime.run, which the
# StoreEventEmitter persists for the dashboard.
# ---------------------------------------------------------------------------


class TriviaOut(BaseModel):
    answer: str
    confidence: float


class SummaryOut(BaseModel):
    headline: str
    bullets: list[str]


class CritiqueOut(BaseModel):
    verdict: str
    score: int


# ---- Group output models — small + JSON-friendly so gemma-4-e4b can hit them ----


class SubQuestion(BaseModel):
    question: str


class Decomposition(BaseModel):
    """The decomposer fans out to 3 sub-questions; each gets routed to a
    researcher in parallel via the ``FanOut`` marker."""

    sub_questions: FanOut[list[SubQuestion]]


class Finding(BaseModel):
    answer: str
    confidence: float


class FinalReport(BaseModel):
    headline: str
    findings_count: int


def build_research_group(*, model: OpenAIChatModel) -> AgentGroup:
    """3-stage cascade — decomposer fans out into 3 researchers, then aggregator.

    Produces a real cascading-spawn tree the dashboard renders with
    `parent_trace_id` edges: root → 3 children → terminal aggregator.
    """
    decomposer = Agent(
        name="decomposer",
        model=model,
        instructions=(
            "Break the user's prompt into exactly 3 short, specific "
            "sub-questions. Output JSON like "
            '{"sub_questions": [{"question": "..."}, ...]} with 3 entries.'
        ),
        output_type=Decomposition,
        trust_level=TrustLevel.LOW,
        context_passer=NullContextPasser(),
    )
    researcher = Agent(
        name="researcher",
        model=model,
        instructions=(
            "Answer the sub-question in one sentence. Set confidence between 0 and 1."
        ),
        output_type=Finding,
        trust_level=TrustLevel.LOW,
        context_passer=NullContextPasser(),
    )
    aggregator = Agent(
        name="aggregator",
        model=model,
        instructions=(
            "You receive a count of findings as the input string. Produce "
            "a one-line headline summarising that the research is complete "
            "and echo the count in `findings_count`."
        ),
        output_type=FinalReport,
        trust_level=TrustLevel.LOW,
        context_passer=NullContextPasser(),
    )
    return AgentGroup(
        name="research-crew",
        topology={
            decomposer: Edge(to=(researcher,)),  # auto fan-out via FanOut
            researcher: Edge(
                to=(aggregator,),
                mapper=lambda findings: TaskSpec(input=str(len(findings))),
            ),
            aggregator: Edge.terminal(),
        },
    )


def build_agents(*, model: OpenAIChatModel) -> list[Agent]:
    return [
        Agent(
            name="trivia",
            model=model,
            instructions=(
                "Answer the user's question in one short sentence. "
                "Set confidence between 0 and 1."
            ),
            output_type=TriviaOut,
            trust_level=TrustLevel.LOW,
            context_passer=NullContextPasser(),
        ),
        Agent(
            name="summarizer",
            model=model,
            instructions=(
                "Summarise the input as a punchy headline plus 3-5 bullets. "
                "Keep each bullet under 12 words."
            ),
            output_type=SummaryOut,
            trust_level=TrustLevel.LOW,
            context_passer=NullContextPasser(),
        ),
        Agent(
            name="critic",
            model=model,
            instructions=(
                "Read the input, give a one-line verdict, and a quality "
                "score from 1 to 10."
            ),
            output_type=CritiqueOut,
            trust_level=TrustLevel.LOW,
            context_passer=NullContextPasser(),
        ),
    ]


# Workload: a rotating set of inputs sized for a tiny local model.
WORKLOAD = [
    ("trivia", "What's the boiling point of water at sea level?"),
    ("trivia", "Who wrote The Hobbit?"),
    ("trivia", "What language is Murmur written in?"),
    ("summarizer", "Murmur is a multi-agent orchestration runtime in Python."),
    ("summarizer", "FastStream brokers Kafka, NATS, RabbitMQ, and Redis."),
    ("critic", "Adding HMAC envelope signing to broker dispatch."),
    ("critic", "Replacing typing.cast with parse helpers and TypedDicts."),
]


GROUP_PROMPTS = [
    "How does HMAC envelope signing protect broker dispatch?",
    "What's the difference between a Pydantic TypedDict and BaseModel?",
    "Why do we cap spawn depth in a multi-agent runtime?",
]


async def _drive(
    runtime: AgentRuntime,
    agents: dict[str, Agent],
    group: AgentGroup,
) -> None:
    """Spawn agents in a slow loop so the dashboard shows continuous motion.

    Mixes single dispatches, a parallel ``gather()``, and a cascading
    ``run_group()`` so the spawn tree shows real depth (root → 3
    researchers in parallel → aggregator).
    """
    cycle = 0
    while True:
        try:
            for offset in (0, 1):
                name, prompt = WORKLOAD[(cycle + offset) % len(WORKLOAD)]
                agent = agents[name]
                await runtime.run(agent, TaskSpec(input=prompt))
                await asyncio.sleep(1.5)
            cycle += 2

            if cycle % 4 == 0:
                # Cascading group — produces a real parent→3-children→aggregator tree.
                prompt = GROUP_PROMPTS[(cycle // 4) % len(GROUP_PROMPTS)]
                await runtime.run_group(group, TaskSpec(input=prompt))

            if cycle % 6 == 0:
                # Fan-out: 3 trivia questions in parallel (no parent edge).
                trivia = agents["trivia"]
                tasks = [
                    TaskSpec(input=q)
                    for q in (
                        "What is 2 + 2?",
                        "What colour is the sky on Mars?",
                        "How many sides does a hexagon have?",
                    )
                ]
                await runtime.gather(trivia, tasks=tasks, max_concurrency=3)
            await asyncio.sleep(2.0)
        except Exception as exc:  # noqa: BLE001 — demo loop, never give up
            print(f"[drive] cycle errored: {exc!r}", file=sys.stderr)
            await asyncio.sleep(2.0)


async def amain(args: argparse.Namespace) -> int:
    if not DASHBOARD_DIR.is_dir() or not (DASHBOARD_DIR / "index.html").is_file():
        print(
            f"[error] dashboard bundle not found at {DASHBOARD_DIR}.\n"
            "        Build it first: "
            "cd packages/dashboard && npm install && npm run build",
            file=sys.stderr,
        )
        return 2

    model = OpenAIChatModel(
        args.model,
        provider=OpenAIProvider(base_url=args.base_url, api_key="lm-studio"),
    )

    store = InMemoryEventStore()
    sse = SSEEventEmitter(heartbeat_interval=10.0)
    emitter = MultiEventEmitter([LogEventEmitter(), sse, StoreEventEmitter(store)])

    runtime = AgentRuntime(
        runtime_id="rt-demo-na-001",
        event_emitter=emitter,
        options=RuntimeOptions(
            token_budget=TokenBudget(limit=200_000),
            max_total_spawns=5_000,
        ),
    )

    agents = {a.name: a for a in build_agents(model=model)}
    research_group = build_research_group(model=model)
    server = AgentServer(
        runtime=runtime,
        sse_emitter=sse,
        dashboard_dir=DASHBOARD_DIR,
        event_store=store,
    )
    for agent in agents.values():
        server.register(agent)
    server.register_group(research_group)

    print("Murmur dashboard demo")
    print(f"  model:      {args.model} via {args.base_url}")
    print("  dashboard:  http://127.0.0.1:8420/dashboard/")
    print("  runs API:   http://127.0.0.1:8420/runs")
    print("  events SSE: http://127.0.0.1:8420/events/stream")
    print("  (Ctrl-C to stop)")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(server.serve(host="127.0.0.1", port=8420))
        tg.create_task(_drive(runtime, agents, research_group))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="http://localhost:1234/v1",
        help="OpenAI-compatible base URL (default: LM Studio at :1234).",
    )
    parser.add_argument(
        "--model",
        default="google/gemma-4-e4b",
        help="Model name as served by --base-url.",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
