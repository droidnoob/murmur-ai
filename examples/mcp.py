"""MCP — agent talking to a Model Context Protocol server.

Attaches an MCP server (here, the bundled stdio stub from the test suite)
to an :class:`Agent` via :func:`mcp_stdio`, with an explicit ``allow=[…]``
narrowing the tool surface. MCP-discovered tools flow through the same
:class:`ToolExecutor` gate as native tools — same trust enforcement, same
``TOOL_CALL_*`` lifecycle events.

The stub server is bundled with the test suite so this example runs
without installing third-party MCP servers. Swap the ``mcp_stdio(...)``
line for any real server you want to consume:

    git_mcp = mcp_stdio("npx", ["@modelcontextprotocol/server-git"])

See also: ``docs/concepts/mcp.md``.

Prereqs:
    pip install murmur-ai
    export ANTHROPIC_API_KEY=...

Run:
    python examples/mcp.py
"""

import asyncio
import os
import sys
from pathlib import Path

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel
from murmur.tools import mcp_stdio


class StubAnswer(BaseModel):
    answer: str


_STUB = (
    Path(__file__).resolve().parent.parent / "tests" / "tools" / "_mcp_stub_server.py"
)


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. "
            "Export it and re-run: export ANTHROPIC_API_KEY=...",
            file=sys.stderr,
        )
        return 2

    if not _STUB.exists():
        print(
            f"bundled MCP stub not found at {_STUB}. "
            "This example expects to run from the murmur-ai source tree; "
            "swap the mcp_stdio(...) line for a real server you have installed.",
            file=sys.stderr,
        )
        return 2

    # The stub exposes a single ``echo`` tool. ``allow=["echo"]`` is required
    # at TrustLevel.LOW; for MEDIUM/HIGH the allow-list narrows what's exposed.
    stub_mcp = mcp_stdio(sys.executable, [str(_STUB)], allow=["echo"])

    agent = Agent(
        name="mcp-agent",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions=(
            "You have an `echo` tool that returns its input verbatim. "
            "Call it once with the user's text, then place the response "
            "into the `answer` field."
        ),
        output_type=StubAnswer,
        mcp_servers=(stub_mcp,),
        trust_level=TrustLevel.LOW,
    )

    runtime = AgentRuntime()

    try:
        result = await runtime.run(
            agent,
            TaskSpec(input="please echo: hello from murmur"),
        )
    finally:
        await runtime.shutdown()

    if not result.is_ok():
        print(f"agent failed: {result.error}", file=sys.stderr)
        return 1

    assert isinstance(result.output, StubAnswer)
    print(f"answer: {result.output.answer}")
    print(
        f"  — {result.metadata.duration_ms} ms, "
        f"{result.metadata.tokens_used or 0} tokens"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
