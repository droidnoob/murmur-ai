"""``FullContextPasser`` — runs the shared ``ContextPasserContract`` suite."""

from __future__ import annotations

import pytest
from tests.contracts.context_passer_contract import ContextPasserContract

from murmur.context.full import FullContextPasser
from murmur.core.protocols.context import ContextPasser
from murmur.types import AgentContext, TaskSpec


class TestFullContextPasser(ContextPasserContract):
    @pytest.fixture
    def passer(self) -> ContextPasser:
        return FullContextPasser()

    async def test_full_passes_context_through_unchanged(
        self,
        passer: ContextPasser,
        context: AgentContext,
        task: TaskSpec,
    ) -> None:
        out = await passer.prepare(context, task)
        assert out == context
