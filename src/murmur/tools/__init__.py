"""Tools — runtime-proxied callables exposed to agents under policy.

Concretes here satisfy :class:`murmur.core.protocols.tools.ToolProvider` and
:class:`murmur.core.protocols.tools.ToolExecutor` structurally. Import the
Protocols from :mod:`murmur.core.protocols`; concretes from this package.

PydanticAI's provider-side built-in tools (``WebSearchTool``,
``CodeExecutionTool``, ``ImageGenerationTool``, ``WebFetchTool``,
``FileSearchTool``, ``MemoryTool``, ``MCPServerTool``, ``XSearchTool``,
``UrlContextTool``) are re-exported so users can populate
:attr:`murmur.Agent.builtin_tools` without importing PydanticAI
directly. They are dataclasses; instantiate with the tool's own knobs
(``max_uses``, ``allowed_domains``, etc.) and pass instances to
``Agent(builtin_tools=(...))``. See :class:`murmur.Agent.builtin_tools`
for the executor-bypass caveat.
"""

from pydantic_ai.builtin_tools import (
    AbstractBuiltinTool,
    CodeExecutionTool,
    FileSearchTool,
    ImageGenerationTool,
    MCPServerTool,
    MemoryTool,
    WebFetchTool,
    WebSearchTool,
    XSearchTool,
)

from murmur.tools.executor import ToolExecutor
from murmur.tools.mcp import mcp_http, mcp_sse, mcp_stdio
from murmur.tools.registry import StaticToolProvider, ToolFunc, ToolRegistry

__all__ = [
    "AbstractBuiltinTool",
    "CodeExecutionTool",
    "FileSearchTool",
    "ImageGenerationTool",
    "MCPServerTool",
    "MemoryTool",
    "StaticToolProvider",
    "ToolExecutor",
    "ToolFunc",
    "ToolRegistry",
    "WebFetchTool",
    "WebSearchTool",
    "XSearchTool",
    "mcp_http",
    "mcp_sse",
    "mcp_stdio",
]
