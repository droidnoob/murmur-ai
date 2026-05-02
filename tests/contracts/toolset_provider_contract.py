"""Shared contract suite for ``core.protocols.ToolsetProvider``.

Every concrete (``MCPToolsetProvider`` and any in-process stub the tests
use) runs this same suite. Subclass :class:`ToolsetProviderContract` and
override the ``provider`` fixture to point at a freshly-constructed,
**not yet started** instance — the contract drives the lifecycle itself.

The contract assumes the provider exposes at least one canned tool named
``"echo"`` that takes a single ``text: str`` argument and returns the
text it was given. Subclasses pre-load this tool however the underlying
toolset works (stubs hard-code it; MCP-backed providers point at a stub
MCP server fixture that ships the same tool).
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from murmur.core.errors import ToolExecutionError
from murmur.core.protocols.toolsets import ToolDescriptor, ToolsetProvider


class ToolsetProviderContract:
    """Behavioural contract every ``ToolsetProvider`` must satisfy."""

    @pytest.fixture
    async def provider(self) -> ToolsetProvider:
        raise NotImplementedError(
            "subclass must override `provider` fixture with a fresh, unstarted instance"
        )

    # ---- lifecycle ---------------------------------------------------------

    async def test_start_is_idempotent(self, provider: ToolsetProvider) -> None:
        await provider.start()
        await provider.start()
        await provider.stop()

    async def test_stop_is_idempotent(self, provider: ToolsetProvider) -> None:
        await provider.start()
        await provider.stop()
        await provider.stop()

    async def test_stop_without_start_is_safe(self, provider: ToolsetProvider) -> None:
        await provider.stop()

    # ---- discovery ---------------------------------------------------------

    async def test_list_tools_returns_descriptors_after_start(
        self, provider: ToolsetProvider
    ) -> None:
        await provider.start()
        try:
            tools = await provider.list_tools()
            assert all(isinstance(t, ToolDescriptor) for t in tools)
            assert any(t.name == "echo" for t in tools)
        finally:
            await provider.stop()

    async def test_list_tools_descriptor_has_input_schema(
        self, provider: ToolsetProvider
    ) -> None:
        await provider.start()
        try:
            tools = await provider.list_tools()
            echo = next(t for t in tools if t.name == "echo")
            assert isinstance(echo.input_schema, Mapping)
            assert "text" in str(echo.input_schema)
        finally:
            await provider.stop()

    # ---- invocation --------------------------------------------------------

    async def test_call_tool_round_trip(self, provider: ToolsetProvider) -> None:
        await provider.start()
        try:
            result = await provider.call_tool("echo", {"text": "hello"})
            assert "hello" in str(result)
        finally:
            await provider.stop()

    async def test_call_tool_unknown_raises_tool_execution_error(
        self, provider: ToolsetProvider
    ) -> None:
        await provider.start()
        try:
            with pytest.raises(ToolExecutionError):
                await provider.call_tool("does_not_exist", {})
        finally:
            await provider.stop()
