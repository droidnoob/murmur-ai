"""Top-level pytest fixtures and helpers.

Mock implementations live here when they satisfy a Protocol — the rule is
that shared contract suites under ``tests/contracts/`` are parametrized over
**every** concrete that satisfies a given Protocol, including these mocks.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, TypeVar

import pytest
from pydantic import BaseModel

from murmur.agent import Agent
from murmur.context.null import NullContextPasser
from murmur.types import (
    AgentContext,
    AgentHandle,
    AgentResult,
    ResultMetadata,
    TaskSpec,
    TrustLevel,
)

if TYPE_CHECKING:
    from murmur.core.protocols.backend import BackendStatus


class _Echo(BaseModel):
    """Tiny output model for tests."""

    text: str


@pytest.fixture
def task_spec() -> TaskSpec:
    return TaskSpec(input="hello world")


@pytest.fixture
def agent_context() -> AgentContext:
    return AgentContext()


@pytest.fixture
def output_type() -> type[BaseModel]:
    return _Echo


@pytest.fixture
def echo_agent(output_type: type[BaseModel]) -> Agent:
    return Agent(
        name="echo",
        model="anthropic:claude-sonnet-4-6",
        instructions="Echo the input.",
        output_type=output_type,
        trust_level=TrustLevel.SANDBOX,
        context_passer=NullContextPasser(),
    )


# ---------------------------------------------------------------------------
# MockBackend — satisfies ``core.protocols.Backend`` structurally
# ---------------------------------------------------------------------------


class MockBackend:
    """In-memory backend that returns a canned :class:`AgentResult`.

    Used by contract tests *and* by runtime tests that don't care about
    real dispatch.
    """

    name: str = "mock"

    def __init__(self) -> None:
        self.spawn_calls: list[tuple[str, str]] = []
        self.killed: list[str] = []
        self._statuses: dict[str, BackendStatus] = {}

    async def spawn(
        self,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,  # noqa: ARG002
    ) -> AgentHandle:
        self.spawn_calls.append((agent.name, task.id))
        handle = AgentHandle(agent_name=agent.name, task_id=task.id, backend=self.name)
        # late import to avoid a cycle at module import time
        from murmur.core.protocols.backend import BackendStatus

        self._statuses[handle.handle_id] = BackendStatus.COMPLETED
        return handle

    async def status(self, handle: AgentHandle) -> BackendStatus:
        from murmur.core.protocols.backend import BackendStatus

        return self._statuses.get(handle.handle_id, BackendStatus.PENDING)

    async def kill(self, handle: AgentHandle) -> None:
        from murmur.core.protocols.backend import BackendStatus

        self.killed.append(handle.handle_id)
        self._statuses[handle.handle_id] = BackendStatus.KILLED

    async def result(self, handle: AgentHandle) -> AgentResult[BaseModel]:
        return AgentResult[BaseModel](
            output=_Echo(text="ok"),
            metadata=ResultMetadata(backend=self.name),
            agent_name=handle.agent_name,
            task_id=handle.task_id,
        )


@pytest.fixture
def mock_backend() -> MockBackend:
    return MockBackend()


# ---------------------------------------------------------------------------
# Async iter helper
# ---------------------------------------------------------------------------

_It = TypeVar("_It")


async def aiter_to_list(it: AsyncIterator[_It]) -> list[_It]:
    return [x async for x in it]
