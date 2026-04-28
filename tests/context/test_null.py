"""``NullContextPasser`` — runs the shared ``ContextPasserContract`` suite."""

from __future__ import annotations

import pytest
from tests.contracts.context_passer_contract import ContextPasserContract

from murmur.context.null import NullContextPasser
from murmur.core.protocols.context import ContextPasser
from murmur.types import AgentContext, TaskSpec


class TestNullContextPasser(ContextPasserContract):
    @pytest.fixture
    def passer(self) -> ContextPasser:
        return NullContextPasser()

    async def test_null_drops_messages_and_metadata(
        self,
        passer: ContextPasser,
        context: AgentContext,
        task: TaskSpec,
    ) -> None:
        out = await passer.prepare(context, task)
        assert out == AgentContext()
