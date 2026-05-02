"""Unit tests for ``core.protocols.toolsets`` — Protocol shape + value object.

The contract suite (``tests/contracts/toolset_provider_contract.py``) is
exercised here against an in-process stub provider so we know the
contract is well-formed *before* the first real concrete (
``MCPToolsetProvider``) lands.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest
from pydantic import ValidationError
from tests.contracts.toolset_provider_contract import ToolsetProviderContract

from murmur.core.errors import ToolExecutionError
from murmur.core.protocols import (
    ToolDescriptor as ReexportedToolDescriptor,
)
from murmur.core.protocols import (
    ToolsetProvider as ReexportedToolsetProvider,
)
from murmur.core.protocols.toolsets import ToolDescriptor, ToolsetProvider

# ---- ToolDescriptor value object ------------------------------------------


def test_tool_descriptor_minimal() -> None:
    desc = ToolDescriptor(name="echo")
    assert desc.name == "echo"
    assert desc.description == ""
    assert desc.read_only is False
    assert desc.input_schema == {}


def test_tool_descriptor_full() -> None:
    desc = ToolDescriptor(
        name="read_file",
        description="Read a file from disk.",
        read_only=True,
        input_schema={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    )
    assert desc.read_only is True
    assert desc.input_schema["required"] == ["path"]


def test_tool_descriptor_is_frozen() -> None:
    desc = ToolDescriptor(name="echo")
    with pytest.raises(ValidationError):
        desc.name = "other"  # type: ignore[misc]


def test_tool_descriptor_requires_name() -> None:
    # Pydantic raises at runtime; ty would otherwise block the negative test.
    with pytest.raises(ValidationError):
        ToolDescriptor()  # ty: ignore[missing-argument]


# ---- Protocol re-export ---------------------------------------------------


def test_protocol_reexports() -> None:
    assert ReexportedToolDescriptor is ToolDescriptor
    assert ReexportedToolsetProvider is ToolsetProvider


# ---- Stub provider — proves the contract is exercisable -------------------


class _StubToolsetProvider:
    """Minimal in-process ``ToolsetProvider`` exposing a single ``echo`` tool.

    Used only by the contract smoke test below. Real concretes live in
    ``murmur.tools.mcp`` (``9mt.3``) and bring their own test module.
    """

    def __init__(self) -> None:
        self._started = False
        self._stop_count = 0

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False
        self._stop_count += 1

    async def list_tools(self) -> Sequence[ToolDescriptor]:
        return (
            ToolDescriptor(
                name="echo",
                description="Return the text it was given.",
                input_schema={
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                },
            ),
        )

    async def call_tool(self, name: str, args: Mapping[str, object]) -> object:
        if name != "echo":
            raise ToolExecutionError(f"unknown tool: {name!r}")
        return args["text"]


class TestStubToolsetProvider(ToolsetProviderContract):
    """Smoke-tests the contract suite against the stub above."""

    @pytest.fixture
    async def provider(self) -> ToolsetProvider:
        return _StubToolsetProvider()
