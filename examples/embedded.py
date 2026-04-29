"""Embedded mode — Murmur mounted into your FastAPI app.

Same agent surface as ``quickstart.py``; the difference is that the agents
are served from *your* FastAPI process via ``app.include_router(agent_router)``
instead of running as a separate ``murmur serve`` process. Two callers in
this script:

1. **HTTP-style** — an httpx call against the in-process ASGI app, exactly
   what an external service or browser would do.
2. **In-process** — :class:`murmur_client.LocalClient` running inside the
   same Python process. No httpx round-trip. Skip this for the simplest
   integration; reach for it when an HTTP handler in your app wants to
   dispatch an agent without serializing through localhost.

This script *does not* call ``uvicorn.run`` — it drives the ASGI app via
``httpx.ASGITransport`` so the example is self-contained. To run it as a
real server, replace the bottom of ``main()`` with::

    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8421)

…and curl ``http://127.0.0.1:8421/murmur/agents/geographer/run``.

Prereqs:
    pip install murmur-ai[server]
    export ANTHROPIC_API_KEY=...

Run:
    python examples/embedded.py
"""

import asyncio
import os
import sys

import httpx
from fastapi import FastAPI
from murmur_client import LocalClient
from pydantic import BaseModel, Field

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.server import AgentRouter


class CapitalLookup(BaseModel):
    country: str
    capital: str
    confidence: float = Field(ge=0.0, le=1.0)


def build_agent() -> Agent:
    return Agent(
        name="geographer",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions=(
            "Answer geography questions. Return the country, capital, and "
            "your confidence (0.0-1.0)."
        ),
        output_type=CapitalLookup,
        trust_level=TrustLevel.LOW,
    )


def build_app() -> tuple[FastAPI, AgentRouter]:
    """The shape your real FastAPI app would adopt.

    Murmur owns its own routes under the ``/murmur`` prefix; everything
    else on the app is yours.
    """
    agent_router = AgentRouter(runtime=AgentRuntime())
    agent_router.register(build_agent())

    app = FastAPI(lifespan=agent_router.lifespan_context, title="My App")
    app.include_router(agent_router, prefix="/murmur")
    AgentRouter.install_exception_handlers(app)

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"hello": "from your app"}

    return app, agent_router


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    app, agent_router = build_app()

    print("=== HTTP-style call (your-app's POST /murmur/agents/geographer/run) ===")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post(
            "/murmur/agents/geographer/run",
            json={"task": TaskSpec(input="Capital of Japan?").model_dump()},
        )
        r.raise_for_status()
        body = r.json()
        if not body["success"]:
            print(f"  agent failed: {body['error']}", file=sys.stderr)
            return 1
        out = body["output"]
        line = (
            f"  {out['country']}: {out['capital']}  "
            f"(confidence {out['confidence']:.2f})"
        )
        print(line)

    print()
    print("=== In-process call (LocalClient — no httpx round-trip) ===")
    async with LocalClient(server=agent_router.server) as client:
        result = await client.run("geographer", TaskSpec(input="Capital of Peru?"))
        if not result.is_ok():
            print(f"  agent failed: {result.error}", file=sys.stderr)
            return 1
        answer = result.output
        assert isinstance(answer, CapitalLookup)
        print(
            f"  {answer.country}: {answer.capital}  "
            f"(confidence {answer.confidence:.2f})"
        )
        print(
            f"  — {result.metadata.duration_ms} ms, "
            f"{result.metadata.tokens_used or 0} tokens"
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
