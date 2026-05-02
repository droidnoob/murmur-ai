"""Events dashboard — live RuntimeEvent stream over SSE.

Wires :class:`SSEEventEmitter` into the runtime, mounts a Starlette
``GET /events/stream`` route that delivers each ``RuntimeEvent`` as a
Server-Sent Event, and runs an agent in the background so a connected
client sees the lifecycle as it unfolds.

The same pattern is what ``murmur serve --port N`` provides out of the
box — this example shows the embedded shape, useful when your own
ASGI app wants to expose a Murmur event feed alongside its own routes.

See also: ``docs/concepts/events.md``, ``docs/api/events.md``.

Prereqs:
    pip install murmur-ai[server]
    export ANTHROPIC_API_KEY=...

Run:
    python examples/events_dashboard.py
    # then: curl -N http://127.0.0.1:8422/events/stream
    # or open in a browser.
"""

import asyncio
import json
import os
import sys

from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.routing import Route

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter


class Out(BaseModel):
    answer: str


def _sse_format(event_type: str, payload: dict[str, object]) -> bytes:
    return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n".encode()


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    sse = SSEEventEmitter(heartbeat_interval=15.0)
    runtime = AgentRuntime(
        event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
    )

    agent = Agent(
        name="trivia",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions="Answer the question in one sentence.",
        output_type=Out,
        trust_level=TrustLevel.LOW,
    )

    async def events_endpoint(_request: Request) -> StreamingResponse:
        async def gen() -> "asyncio.AsyncIterator[bytes]":
            async for event in sse.subscribe():
                yield _sse_format(event.event_type.value, dict(event.payload))

        return StreamingResponse(gen(), media_type="text/event-stream")

    app = Starlette(routes=[Route("/events/stream", events_endpoint)])

    # Drive the agent in the background so connected clients see live events.
    async def driver() -> None:
        questions = [
            "What's the speed of light?",
            "Who painted the Mona Lisa?",
            "When did the Roman Empire fall?",
        ]
        for q in questions:
            await runtime.run(agent, TaskSpec(input=q))
            await asyncio.sleep(2.0)

    import uvicorn

    config = uvicorn.Config(app, host="127.0.0.1", port=8422, log_level="warning")
    server = uvicorn.Server(config)

    print("dashboard: curl -N http://127.0.0.1:8422/events/stream")
    print("(Ctrl-C to stop)")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(server.serve())
        tg.create_task(driver())
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(0)
