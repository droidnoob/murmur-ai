"""ToolsetProvider Protocol — remote / external tool sources.

A ``ToolsetProvider`` is a runtime-managed source of tools an agent can
call alongside its native ``tools=…``. The first concrete is
:class:`murmur.tools.MCPToolsetProvider` (Model Context Protocol), but
the shape is deliberately generic so future remote toolsets — Sourcegraph
Cody, Continue, custom HTTP toolboxes — can satisfy the same Protocol
without inheritance.

Three guarantees this Protocol exists to enforce:

1. **Lifecycle is owned by the runtime, not the agent.** ``start`` and
   ``stop`` are idempotent so the runtime can call them lazily on first
   use and again on shutdown without tracking per-call state.
2. **Discovery is explicit.** ``list_tools`` returns a typed sequence of
   :class:`ToolDescriptor`, never opaque dicts — so the executor can
   reason about ``name`` / ``read_only`` for trust gating.
3. **Invocation flows through the runtime's executor.** ``call_tool`` is
   the *only* sanctioned way to invoke a remote tool — every call is
   funnelled through :class:`ToolExecutor` for trust + allow-list +
   logging, matching the proxy pattern native tools already use.

The Protocol is matched structurally — concretes never inherit.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


class ToolDescriptor(BaseModel):
    """Frozen description of a single tool exposed by a ``ToolsetProvider``.

    Mirrors the JSON Schema-shaped metadata MCP servers publish. Read-only
    is a hint, *not* a security boundary — trust gating uses an explicit
    allow-list, never this flag (see ``9mt.5``).
    """

    model_config = ConfigDict(frozen=True)

    name: str
    """Tool name as it will appear to the agent. Must be unique within the provider."""

    input_schema: Mapping[str, object] = Field(default_factory=dict)
    """JSON Schema describing the tool's argument shape."""

    description: str = ""
    """Human-readable description surfaced to the LLM."""

    read_only: bool = False
    """Provider-declared hint that the tool does not mutate external state.

    Informational only. ``TrustLevel.LOW`` does not auto-permit read-only
    tools — an explicit allow-list is required.
    """


@runtime_checkable
class ToolsetProvider(Protocol):
    """Pluggable source of tools managed by the runtime lifecycle.

    Implementations must be safe to call concurrently after :meth:`start`
    has returned; ``call_tool`` may be invoked from many tasks at once.
    ``start`` and ``stop`` are idempotent — calling either twice is a
    no-op the runtime relies on for lazy startup and graceful shutdown.
    """

    async def start(self) -> None:
        """Open the connection / spawn the subprocess. Idempotent."""
        ...

    async def stop(self) -> None:
        """Close the connection / terminate the subprocess. Idempotent."""
        ...

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        """Return descriptors for every tool the provider exposes.

        Must be callable after :meth:`start`. Implementations may cache
        the result; the runtime calls this once per agent build.
        """
        ...

    async def call_tool(self, name: str, args: Mapping[str, object]) -> object:
        """Invoke ``name`` with ``args`` and return the result.

        Raises :class:`murmur.core.errors.ToolExecutionError` if ``name``
        is not exposed by this provider, or if the underlying tool call
        fails. The runtime's :class:`ToolExecutor` wraps every call —
        agents never reach this method directly.
        """
        ...


__all__ = ["ToolDescriptor", "ToolsetProvider"]
