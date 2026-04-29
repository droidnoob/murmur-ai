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
from types import MappingProxyType
from typing import Generic, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from murmur.core.protocols.pipeline import NextStage, Stage
from murmur.types import AgentContext, AgentHandle, TaskSpec

T = TypeVar("T")


class PipelineContext(BaseModel):
    """The carrier object threaded through every stage.

    Frozen — produce a new :class:`PipelineContext` via
    ``model_copy(update={"state": {...new_state...}})``. ``state`` is for
    **cross-stage scratch only** — small, ephemeral data shared between
    stages within a single run. Prefer typed fields on this class for
    anything load-bearing (``handle``, ``agent_context``, etc.); never put
    domain values in ``state`` you'd want to grep for later.

    ``state`` is wrapped in a :class:`types.MappingProxyType` post-validation
    so in-place mutation raises :class:`TypeError` rather than silently
    corrupting a shared run — the "no in-place mutation, only
    ``model_copy``" rule is enforceable, not just documented.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    task: TaskSpec
    agent_name: str
    agent_context: AgentContext = Field(default_factory=AgentContext)
    handle: AgentHandle | None = None
    state: Mapping[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _freeze_state(self) -> PipelineContext:
        """Replace ``state`` with a :class:`MappingProxyType` view.

        Pydantic v2 normalises the field to a plain ``dict`` during
        validation; we re-wrap it in a read-only view post-validation so
        stage code that does ``ctx.state["k"] = v`` raises ``TypeError``.
        """
        if not isinstance(self.state, MappingProxyType):
            backing: dict[str, object] = dict(self.state)
            # ``frozen=True`` blocks normal attribute assignment; we have
            # to write through ``object.__setattr__`` here. This is the
            # one and only place that bypasses the freeze.
            object.__setattr__(
                self, "state", cast("Mapping[str, object]", MappingProxyType(backing))
            )
        return self


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
