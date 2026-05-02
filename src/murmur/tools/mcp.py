"""MCP toolset provider — wraps PydanticAI's ``pydantic_ai.mcp`` servers.

Public surface is the three factory helpers — :func:`mcp_stdio`,
:func:`mcp_http`, :func:`mcp_sse`. Users do not construct
:class:`MCPToolsetProvider` directly; they hand the factory's return
value to :class:`murmur.Agent`'s ``mcp_servers`` tuple, where it is
treated as an opaque :class:`murmur.core.protocols.ToolsetProvider`.

The wrapper translates between Murmur's Protocol surface and PydanticAI's
``MCPServer`` async-context-manager / reference-counted lifecycle:

- ``start`` / ``stop`` map to ``__aenter__`` / ``__aexit__`` and are
  idempotent from the caller's view (we track a ``_started`` flag so a
  double-call is a no-op rather than a counter increment).
- ``list_tools`` round-trips through ``MCPServer.list_tools`` and maps
  each ``mcp_types.Tool`` into a :class:`ToolDescriptor`.
- ``call_tool`` pre-checks the name against the cached descriptor list
  (so unknown tools fail fast with :class:`ToolExecutionError` instead of
  a wasted network round-trip), then delegates to ``direct_call_tool``
  and translates ``ModelRetry`` / ``McpError`` into
  :class:`ToolExecutionError`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from mcp.shared.exceptions import McpError
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.mcp import (
    MCPServer,
    MCPServerSSE,
    MCPServerStdio,
    MCPServerStreamableHTTP,
)

from murmur.core.errors import ToolExecutionError
from murmur.core.protocols.toolsets import ToolDescriptor

if TYPE_CHECKING:
    from mcp import types as mcp_types


class MCPToolsetProvider:
    """:class:`ToolsetProvider` backed by a PydanticAI ``MCPServer``.

    Constructed via :func:`mcp_stdio` / :func:`mcp_http` / :func:`mcp_sse`
    in the common case. Tests may pass a pre-built ``MCPServer`` directly
    via the ``server`` argument to inject stubs.

    ``allow`` is the explicit tool allow-list. Semantics by trust level:

    - ``TrustLevel.SANDBOX`` — provider is skipped entirely.
    - ``TrustLevel.LOW`` — *requires* ``allow`` to be set; ``None`` means
      no MCP tools at LOW trust (safest default — MCP servers self-declare
      ``readOnlyHint``, which is not a security boundary).
    - ``TrustLevel.MEDIUM`` / ``TrustLevel.HIGH`` — ``None`` exposes
      everything the server reports; a non-``None`` ``allow`` opts the user
      into a narrower subset.

    ``start()`` / ``stop()`` are advisory pre-warming. ``list_tools`` and
    ``call_tool`` work without them — the underlying ``MCPServer``
    manages its own per-call context. Pre-warming via ``start()`` keeps
    the subprocess hot across calls. Always paired in the **same task**
    (anyio cancel scopes are task-bound) — a runtime supervisor that owns
    both ends of the lifecycle is in :class:`AgentRuntime`.
    """

    def __init__(
        self,
        server: MCPServer,
        *,
        allow: Sequence[str] | None = None,
    ) -> None:
        self._mcp = server
        self._started = False
        self._descriptor_cache: tuple[ToolDescriptor, ...] | None = None
        self._tool_names: frozenset[str] = frozenset()
        self._allow: frozenset[str] | None = (
            frozenset(allow) if allow is not None else None
        )

    @property
    def allow(self) -> frozenset[str] | None:
        """Allow-list passed at construction. ``None`` means "no opt-in"."""
        return self._allow

    @property
    def started(self) -> bool:
        """``True`` between :meth:`start` and :meth:`stop`."""
        return self._started

    async def start(self) -> None:
        if self._started:
            return
        await self._mcp.__aenter__()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        try:
            await self._mcp.__aexit__(None, None, None)
        finally:
            self._started = False
            self._descriptor_cache = None
            self._tool_names = frozenset()

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        """Discover the tools the server reports.

        ``MCPServer.list_tools`` manages its own ``__aenter__`` /
        ``__aexit__`` cycle internally, so calling without :meth:`start`
        is safe. Once a result is cached the underlying server isn't
        contacted again until :meth:`stop` clears the cache.
        """
        if self._descriptor_cache is not None:
            return self._descriptor_cache
        mcp_tools = await self._mcp.list_tools()
        descriptors = tuple(_to_descriptor(t) for t in mcp_tools)
        self._descriptor_cache = descriptors
        self._tool_names = frozenset(d.name for d in descriptors)
        return descriptors

    async def call_tool(self, name: str, args: Mapping[str, object]) -> object:
        """Invoke a tool by name. Pre-checks against the cached descriptor list."""
        if self._descriptor_cache is None:
            await self.list_tools()
        if name not in self._tool_names:
            raise ToolExecutionError(f"unknown MCP tool: {name!r}")
        try:
            return await self._mcp.direct_call_tool(name, dict(args))
        except (ModelRetry, McpError) as exc:
            raise ToolExecutionError(f"MCP tool {name!r} failed: {exc}") from exc


def _to_descriptor(tool: mcp_types.Tool) -> ToolDescriptor:
    """Map an ``mcp_types.Tool`` into Murmur's frozen value object."""
    annotations = tool.annotations
    read_only = bool(annotations.readOnlyHint) if annotations else False
    return ToolDescriptor(
        name=tool.name,
        description=tool.description or "",
        input_schema=dict(tool.inputSchema or {}),
        read_only=read_only,
    )


def mcp_stdio(
    command: str,
    args: Sequence[str] = (),
    *,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
    allow: Sequence[str] | None = None,
) -> MCPToolsetProvider:
    """Build an :class:`MCPToolsetProvider` over a stdio MCP server subprocess.

    The subprocess is spawned lazily on the first
    :meth:`MCPToolsetProvider.start` call (the runtime owns lifecycle).

    Args:
        command: Executable to run (e.g. ``"npx"``, ``"uv"``).
        args: Positional arguments passed to ``command``.
        env: Environment variables for the child process. ``None`` (default)
            means the child gets *no* environment, matching PydanticAI's
            default — pass ``dict(os.environ)`` to inherit the parent's.
        cwd: Working directory for the child process.
        allow: Explicit tool allow-list. See
            :class:`MCPToolsetProvider` for trust-level semantics.
    """
    return MCPToolsetProvider(
        MCPServerStdio(
            command=command,
            args=list(args),
            env=dict(env) if env is not None else None,
            cwd=cwd,
        ),
        allow=allow,
    )


def mcp_http(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    allow: Sequence[str] | None = None,
) -> MCPToolsetProvider:
    """Build an :class:`MCPToolsetProvider` over a Streamable-HTTP MCP server.

    Used for newer MCP servers that speak the streamable-HTTP transport.
    Use :func:`mcp_sse` for legacy SSE-only servers. ``allow`` is the
    optional tool allow-list — see :class:`MCPToolsetProvider`.
    """
    return MCPToolsetProvider(
        MCPServerStreamableHTTP(
            url=url,
            headers=dict(headers) if headers is not None else None,
        ),
        allow=allow,
    )


def mcp_sse(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    allow: Sequence[str] | None = None,
) -> MCPToolsetProvider:
    """Build an :class:`MCPToolsetProvider` over a Server-Sent-Events MCP server.

    ``allow`` is the optional tool allow-list — see :class:`MCPToolsetProvider`.
    """
    return MCPToolsetProvider(
        MCPServerSSE(
            url=url,
            headers=dict(headers) if headers is not None else None,
        ),
        allow=allow,
    )


__all__ = [
    "MCPToolsetProvider",
    "mcp_http",
    "mcp_sse",
    "mcp_stdio",
]
