"""Shared contract suite for ``core.protocols.Backend``.

Subclass :class:`BackendContract` and override ``backend`` for each concrete.
Stubs may use ``pytest.skip`` inside their fixture until the real dispatch
path is implemented.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur.agent import Agent
from murmur.context.null import NullContextPasser
from murmur.core.protocols.backend import Backend, BackendStatus
from murmur.types import AgentContext, TaskSpec, TrustLevel


class _Out(BaseModel):
    text: str


class BackendContract:
    """Behavioural contract every ``Backend`` must satisfy."""

    @pytest.fixture
    def backend(self) -> Backend:
        raise NotImplementedError(
            "subclass must override `backend` fixture with a concrete instance"
        )

    @pytest.fixture
    def agent(self) -> Agent:
        return Agent(
            name="echo",
            model="anthropic:claude-sonnet-4-6",
            instructions="echo",
            output_type=_Out,
            trust_level=TrustLevel.SANDBOX,
            context_passer=NullContextPasser(),
        )

    @pytest.fixture
    def task(self) -> TaskSpec:
        return TaskSpec(input="hi")

    @pytest.fixture
    def context(self) -> AgentContext:
        return AgentContext()

    async def test_spawn_returns_handle_for_agent_and_task(
        self,
        backend: Backend,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> None:
        handle = await backend.spawn(agent, task, context)
        assert handle.agent_name == agent.name
        assert handle.task_id == task.id

    async def test_status_returns_backend_status(
        self,
        backend: Backend,
        agent: Agent,
        task: TaskSpec,
        context: AgentContext,
    ) -> None:
        handle = await backend.spawn(agent, task, context)
        status = await backend.status(handle)
        assert isinstance(status, BackendStatus)
