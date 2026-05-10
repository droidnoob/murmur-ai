"""Cross-run memory — recommended pattern: build it as a tool.

Murmur intentionally does **not** ship a built-in cross-run memory primitive.
Persistent memory (vector stores, RAG, "remember the user across sessions")
is application data with retention policies, key management, and storage
infra that the runtime shouldn't absorb. The recommended shape is two tools
backed by your own store:

- ``recall_memory(query: str) -> list[Memory]`` — read.
- ``store_memory(key: str, value: str) -> None`` — write.

This example uses a plain ``dict`` keyed by ``key`` (with substring search
for recall) so it's runnable with zero external dependencies. Swap the
``_MemoryStore`` for ``chromadb``, ``sqlite-vec``, ``pgvector``, or any
other vector store you already operate; the agent-facing surface is
unchanged.

Two sessions across the same store demonstrate the cross-run pattern:

1. **Session 1**: agent stores a fact via ``store_memory``.
2. **Session 2** (a fresh ``runtime.run``): agent recalls the fact via
   ``recall_memory`` and uses it.

The runtime sees two independent runs; the persistence sits behind the
tool callable. Murmur owns orchestration; the user owns memory.

See also: ``docs/concepts/coordination.md`` ("Cross-run memory" pattern),
``docs/concepts/tools.md``, ``CLAUDE.md §22`` (what NOT to build).

Prereqs:
    pip install murmur-runtime
    export ANTHROPIC_API_KEY=...

Run:
    python examples/memory_via_tool.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Awaitable, Callable

from pydantic import BaseModel

from murmur import Agent, AgentRuntime, TaskSpec, TrustLevel


class Memory(BaseModel):
    """One stored fact — what ``recall_memory`` returns per match."""

    key: str
    value: str


class _MemoryStore:
    """Trivial in-process key/value store with substring recall.

    Stand-in for whatever durable store you actually run in production:
    Chroma collection, SQLite table, Redis hash, Postgres + pgvector,
    etc. The shape the agent-facing tools expose is the contract; the
    backing store is yours to pick.
    """

    def __init__(self) -> None:
        self._items: dict[str, str] = {}

    def store(self, key: str, value: str) -> None:
        self._items[key] = value

    def recall(self, query: str, *, limit: int = 5) -> list[Memory]:
        matches = [
            Memory(key=k, value=v)
            for k, v in self._items.items()
            if query.lower() in k.lower() or query.lower() in v.lower()
        ]
        return matches[:limit]


class FinalAnswer(BaseModel):
    """Agent's structured response."""

    answer: str


_RecallTool = Callable[[str], Awaitable[list[Memory]]]
_StoreTool = Callable[[str, str], Awaitable[None]]


def _build_memory_tools(store: _MemoryStore) -> tuple[_RecallTool, _StoreTool]:
    """Return ``(recall_memory, store_memory)`` async callables bound to ``store``.

    The closure binding keeps the store private to these tools — the
    runtime's tool registry only sees the callables, not the store
    object. Multiple agents wired to the same tools share the store
    naturally; agents wired to different stores are isolated.
    """

    async def recall_memory(query: str) -> list[Memory]:
        """Recall memories matching ``query`` (substring match in this stub)."""
        return store.recall(query)

    async def store_memory(key: str, value: str) -> None:
        """Persist ``value`` under ``key`` for later recall."""
        store.store(key, value)

    return recall_memory, store_memory


async def main() -> int:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "Set ANTHROPIC_API_KEY to run this example.",
            file=sys.stderr,
        )
        return 1

    store = _MemoryStore()
    recall_memory, store_memory = _build_memory_tools(store)

    runtime = AgentRuntime()
    runtime.tool_registry.register("recall_memory", recall_memory)
    runtime.tool_registry.register("store_memory", store_memory)

    librarian = Agent(
        name="librarian",
        model="anthropic:claude-haiku-4-5-20251001",
        instructions=(
            "You answer questions about the user. Use recall_memory to look "
            "up what you already know; use store_memory to persist new "
            "facts. Always recall before answering — prior sessions may "
            "have left useful context."
        ),
        output_type=FinalAnswer,
        tools=frozenset({"recall_memory", "store_memory"}),
        trust_level=TrustLevel.MEDIUM,
    )

    # Session 1: teach the agent a fact. The agent will call store_memory.
    session_1 = await runtime.run(
        librarian,
        TaskSpec(input="Remember this for next time: my favourite colour is teal."),
    )
    print(
        "session 1:",
        session_1.output.answer if session_1.is_ok() else session_1.error,
    )

    # Session 2: a fresh runtime.run on the same agent + tools. The store
    # persists across runs because it lives in the tool's closure, not in
    # AgentContext.messages. The agent calls recall_memory and uses the
    # prior fact.
    session_2 = await runtime.run(
        librarian,
        TaskSpec(input="What's my favourite colour?"),
    )
    print(
        "session 2:",
        session_2.output.answer if session_2.is_ok() else session_2.error,
    )

    print(f"\nstore contents: {store._items}")
    await runtime.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
