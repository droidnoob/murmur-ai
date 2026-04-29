"""Distributed mode — broker-backed fan-out, single process.

Same ``Agent`` definition as ``quickstart.py``; the only thing that changes
is the runtime. We construct a broker, point an ``AgentRuntime`` at it
(``JobBackend`` under the hood), and start a same-process ``Worker`` that
subscribes to the agent's task topic. Tasks published by the runtime travel
through the broker and are consumed by the worker, exactly as they would
across a Kafka / NATS / RabbitMQ / Redis cluster — just without the network.

The ``InMemoryBroker`` is what the user-facing ``broker="memory://"`` URL
resolves to. We import it directly here because the runtime and worker need
to share the *same* broker instance for the in-process round-trip to work.
Swap the ``InMemoryBroker()`` line for a ``FastStreamBroker`` (or just pass
``broker="kafka://localhost:9092"`` to the runtime + ``murmur worker start
--broker kafka://localhost:9092 --agents geographer`` in another terminal)
to see the distributed shape with no code change to the agent.

Prereqs:
    pip install murmur-ai
    export ANTHROPIC_API_KEY=...

Run:
    python examples/distributed.py
"""

import asyncio
import os
import sys

from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.backends._inmemory_broker import InMemoryBroker
from murmur.worker import Worker


class CapitalLookup(BaseModel):
    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)


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
            "Answer geography questions. Return the country, capital, and "
            "your confidence (0.0-1.0)."
        ),
        output_type=CapitalLookup,
        trust_level=TrustLevel.LOW,
    )

    broker = InMemoryBroker()
    runtime = AgentRuntime(broker_instance=broker)
    worker = Worker(broker=broker, agents={geographer.name: geographer})

    @worker.on_task_complete
    async def _log_complete(task_id: str, agent_name: str, duration_ms: int) -> None:
        print(f"  worker: completed {agent_name} task {task_id} in {duration_ms} ms")

    countries = ["Iceland", "Japan", "Peru", "Botswana"]

    await worker.start()
    try:
        results = await runtime.gather(
            geographer,
            tasks=[TaskSpec(input=f"Capital of {c}?") for c in countries],
            max_concurrency=4,
        )
    finally:
        await worker.stop()

    print()
    for country, result in zip(countries, results, strict=True):
        if not result.is_ok():
            print(f"  {country}: ERROR — {result.error}")
            continue
        answer = result.output
        assert isinstance(answer, CapitalLookup)
        print(
            f"  {answer.country:>10}: {answer.capital:<20}"
            f"  (confidence {answer.confidence:.2f})"
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
