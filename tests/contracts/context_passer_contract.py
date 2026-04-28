"""Shared contract suite for ``core.protocols.ContextPasser``.

Subclass :class:`ContextPasserContract` and override ``passer`` for each
concrete implementation.
"""

from __future__ import annotations

import pytest

from murmur.core.protocols.context import ContextPasser
from murmur.types import AgentContext, TaskSpec


class ContextPasserContract:
    """Behavioural contract every ``ContextPasser`` must satisfy."""

    @pytest.fixture
    def passer(self) -> ContextPasser:
        raise NotImplementedError(
            "subclass must override `passer` fixture with a concrete instance"
        )

    @pytest.fixture
    def task(self) -> TaskSpec:
        return TaskSpec(input="x")

    @pytest.fixture
    def context(self) -> AgentContext:
        return AgentContext(messages=({"role": "user", "content": "hi"},), depth=2)

    async def test_prepare_returns_agent_context(
        self,
        passer: ContextPasser,
        context: AgentContext,
        task: TaskSpec,
    ) -> None:
        result = await passer.prepare(context, task)
        assert isinstance(result, AgentContext)

    async def test_prepare_does_not_mutate_input(
        self,
        passer: ContextPasser,
        context: AgentContext,
        task: TaskSpec,
    ) -> None:
        before = context.model_copy(deep=True)
        await passer.prepare(context, task)
        assert context == before
