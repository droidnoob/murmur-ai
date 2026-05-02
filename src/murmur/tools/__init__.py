"""Tools — runtime-proxied callables exposed to agents under policy.

Concretes here satisfy :class:`murmur.core.protocols.tools.ToolProvider` and
:class:`murmur.core.protocols.tools.ToolExecutor` structurally. Import the
Protocols from :mod:`murmur.core.protocols`; concretes from this package.
"""

from murmur.tools.executor import ToolExecutor
from murmur.tools.mcp import mcp_http, mcp_sse, mcp_stdio
from murmur.tools.registry import StaticToolProvider, ToolFunc, ToolRegistry

__all__ = [
    "StaticToolProvider",
    "ToolExecutor",
    "ToolFunc",
    "ToolRegistry",
    "mcp_http",
    "mcp_sse",
    "mcp_stdio",
]
