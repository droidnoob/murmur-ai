"""Stub MCP server for the stdio E2E smoke test.

Spawned as a subprocess by ``test_stdio_e2e.py``. Constructs an
``AgentServer`` with one MCP-enrolled agent backed by PydanticAI's
``test`` pseudo-model (no provider key needed), then runs
``serve_mcp(transport="stdio")`` on stdin/stdout.

The client side connects via ``mcp.client.stdio.stdio_client`` and
exercises the wire format end-to-end: initialise handshake, list_tools,
call_tool round-trip.
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TrustLevel
from murmur.server import AgentServer


class EchoOut(BaseModel):
    answer: str


def main() -> None:
    runtime = AgentRuntime()
    agent = Agent(
        name="echo",
        model="test",  # PydanticAI test pseudo-model — no provider call.
        instructions="Reply with a short answer.",
        output_type=EchoOut,
        trust_level=TrustLevel.MEDIUM,
    )
    server = AgentServer(runtime=runtime)
    server.register_mcp(
        agent,
        tool_name="echo",
        description="Echo a short answer for the given input.",
    )
    asyncio.run(server.serve_mcp(transport="stdio"))


if __name__ == "__main__":
    main()
