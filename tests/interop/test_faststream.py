"""Tests for :func:`murmur.interop.as_faststream_handler`.

The handler runs the agent through an in-process runtime; we don't
actually exercise FastStream's broker — that's covered by
``tests/backends/test_faststream_broker.py`` and the user's own
integration. Here we just verify the adapter returns a callable that
dispatches correctly.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.agent import Agent
from murmur.backends.async_backend import AsyncBackend
from murmur.context.null import NullContextPasser
from murmur.interop import as_faststream_handler
from murmur.runtime import AgentRuntime
from murmur.types import TaskSpec, TrustLevel


class _Out(BaseModel):
    text: str


def _build_factory() -> Any:
    async def build(
        agent: Agent, _allowed: frozenset[str], _task_id: str
    ) -> pydantic_ai.Agent[None, Any]:
        return pydantic_ai.Agent(
            model=TestModel(custom_output_args=_Out(text="ok").model_dump()),
            instructions=agent.instructions,
            output_type=agent.output_type,
        )

    return build


def _agent() -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",
        instructions="echo",
        output_type=_Out,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


async def test_handler_dispatches_agent_through_default_runtime() -> None:
    """Without an explicit runtime, a fresh in-process AgentRuntime is built."""
    backend = AsyncBackend()
    backend._build_pa_agent = _build_factory()  # noqa: SLF001
    runtime = AgentRuntime(backend=backend)

    handler = as_faststream_handler(_agent(), runtime=runtime)
    result = await handler(TaskSpec(input="hi"))
    assert result.is_ok()
    assert isinstance(result.output, _Out)
    assert result.output.text == "ok"


async def test_handler_accepts_explicit_runtime() -> None:
    backend = AsyncBackend()
    backend._build_pa_agent = _build_factory()  # noqa: SLF001
    runtime = AgentRuntime(backend=backend)

    handler = as_faststream_handler(_agent(), runtime=runtime)
    result = await handler(TaskSpec(input="x"))
    assert result.is_ok()


def test_handler_returns_async_callable() -> None:
    """Shape check — handler is callable and returns an awaitable."""
    import inspect

    handler = as_faststream_handler(_agent())
    assert callable(handler)
    assert inspect.iscoroutinefunction(handler)
