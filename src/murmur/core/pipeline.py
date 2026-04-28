"""Concrete pipeline composer + the ``PipelineContext`` carrier.

The :class:`Pipeline` Protocol lives in :mod:`murmur.core.protocols.pipeline`;
this module provides the **one** concrete that ships. The class satisfies
``core.protocols.Pipeline`` structurally (no inheritance).

``PipelineContext`` is *not* a pluggable — it is a frozen data carrier
threaded through every stage. Stages produce a new ``PipelineContext`` via
``model_copy(update=...)`` rather than mutating the one they receive.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from murmur.core.protocols.pipeline import NextStage, Stage
from murmur.types import AgentContext, AgentHandle, TaskSpec

T = TypeVar("T")


class PipelineContext(BaseModel):
    """The carrier object threaded through every stage.

    Frozen — produce a new ``PipelineContext`` via ``model_copy(update=...)``.
    ``state`` is the only field intended for free-form scratch data; prefer
    typed fields on this class for anything load-bearing.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    task: TaskSpec
    agent_name: str
    agent_context: AgentContext = Field(default_factory=AgentContext)
    handle: AgentHandle | None = None
    state: Mapping[str, object] = Field(default_factory=dict)


class Pipeline(Generic[T]):
    """The concrete pipeline composer (satisfies ``core.protocols.Pipeline``).

    Stages execute in declaration order. The first stage receives the inbound
    request and decides whether (and how) to call ``next_stage``. The final
    stage's return value bubbles back through every preceding stage.
    """

    def __init__(self, stages: list[Stage[T]]) -> None:
        if not stages:
            raise ValueError("Pipeline requires at least one stage")
        self._stages: list[Stage[T]] = list(stages)

    async def run(self, context: PipelineContext) -> T:
        """Execute the pipeline against ``context`` and return the final result."""
        return await self._build_chain()(context)

    def _build_chain(self) -> NextStage[T]:
        async def terminal(_: PipelineContext) -> T:
            raise RuntimeError(
                "Pipeline reached terminal stage — the last stage must produce "
                "a result and not call next_stage()"
            )

        chain: NextStage[T] = terminal
        for stage in reversed(self._stages):
            chain = self._wrap(stage, chain)
        return chain

    @staticmethod
    def _wrap(stage: Stage[T], next_stage: NextStage[T]) -> NextStage[T]:
        async def call(context: PipelineContext) -> T:
            return await stage(context, next_stage)

        return call


__all__ = ["Pipeline", "PipelineContext"]
