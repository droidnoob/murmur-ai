"""Worker concurrency tuning — fan out N tasks across one Worker.

``distributed.py`` shows the wire shape with default knobs. This example
focuses on the two knobs you reach for when one Worker process needs to
saturate a real broker:

* ``concurrency`` — max in-flight LLM calls in this Worker. Each
  in-flight task holds an ``asyncio`` slot; the broker subscriber blocks
  on the semaphore once the cap is hit.
* ``prefetch`` — how many messages the subscriber pulls per poll before
  it must process them. Lower values give tighter fan-out fairness
  across a fleet of Workers (each Worker grabs less per round-trip);
  higher values favour throughput on a single Worker.

The example dispatches a fan-out of 32 tasks against a Worker tuned for
that burst. Swap ``InMemoryBroker()`` for ``make_broker(scheme=...,
url=...)`` to see the same shape against Kafka / NATS / RabbitMQ /
Redis — the agent and Worker code don't change.

Two competing Workers running in the same process show the
broker-level competing-consumer split: each task is delivered to
exactly one Worker (no broadcast).

Pairs with: ``docs/concepts/backends.md``, ``docs/guides/distributed.md``.

Prereqs:
    pip install murmur-ai
    export ANTHROPIC_API_KEY=...

Run:
    python examples/worker_concurrency.py
"""

import asyncio
import os
import sys
from collections import Counter

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.worker import Worker


class Capital(BaseModel):
    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)


COUNTRIES = [
    "Iceland",
    "Japan",
    "Peru",
    "Botswana",
    "Canada",
    "Egypt",
    "France",
    "Spain",
    "Italy",
    "Germany",
    "Brazil",
    "Argentina",
    "Australia",
    "New Zealand",
    "South Africa",
    "Kenya",
    "India",
    "Nepal",
    "Thailand",
    "Vietnam",
    "Korea",
    "Morocco",
    "Tunisia",
    "Greece",
    "Portugal",
    "Norway",
    "Sweden",
    "Finland",
    "Denmark",
    "Ireland",
    "Poland",
    "Hungary",
]


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
            "Answer geography questions. Return country, capital, confidence (0..1)."
        ),
        output_type=Capital,
        trust_level=TrustLevel.LOW,
    )

    # In-process broker so the example runs without external services.
    # Swap for ``make_broker(scheme="kafka", url="kafka://localhost:9092")``
    # — the rest of the code is unchanged.
    broker = InMemoryBroker()
    runtime = AgentRuntime(broker_instance=broker)

    # Two competing Workers on the same agent topic. Both subscribe under
    # the same broker ``group``; each TaskMessage is delivered to exactly
    # one Worker — no double-charge from broadcast.
    workers = [
        Worker(
            broker=broker,
            agents={geographer.name: geographer},
            concurrency=16,  # up to 16 LLM calls in flight per Worker
            prefetch=4,  # claim 4 at a time — tighter fairness
            heartbeat_seconds=0,
        )
        for _ in range(2)
    ]
    completed_per_worker: Counter[int] = Counter()
    for idx, w in enumerate(workers):

        @w.on_task_complete
        async def _on_complete(
            task_id: str,
            agent_name: str,
            duration_ms: int,
            _idx: int = idx,
        ) -> None:
            completed_per_worker[_idx] += 1

    for w in workers:
        await w.start()
    try:
        # Brief settle so both subscribers register before we publish.
        await asyncio.sleep(0.1)

        results = await runtime.gather(
            geographer,
            tasks=[TaskSpec(input=f"Capital of {c}?") for c in COUNTRIES],
            # ``max_concurrency`` here caps publisher-side dispatch
            # (back-pressure from the publisher). The Workers will still
            # only run ``concurrency`` in flight at a time.
            max_concurrency=len(COUNTRIES),
        )
    finally:
        for w in workers:
            await w.stop()
        await runtime.shutdown()

    ok = sum(r.is_ok() for r in results)
    total_tokens = sum(r.metadata.tokens_used for r in results if r.is_ok())
    print(f"completed: {ok}/{len(results)}")
    print(f"tokens:    {total_tokens}")
    print("split between competing Workers:")
    for idx in sorted(completed_per_worker):
        print(f"  worker[{idx}]: {completed_per_worker[idx]} tasks")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
