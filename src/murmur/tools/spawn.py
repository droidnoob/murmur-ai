"""``spawn_agents`` — LLM-callable tool for dynamic fan-out under a template.

A parent agent declares ``"spawn_agents"`` in its ``tools=`` set; the LLM
calls it mid-run with a list of :class:`SpawnSpec` (name, instructions,
input). The runtime materialises each child via :meth:`AgentTemplate.agent`
— so trust level, model, and tool surface come from the template, **not**
from the LLM's call — and dispatches them through ``runtime.run`` with a
semaphore-bounded fan-out. Each child's outcome rolls up into a
:class:`SpawnResult` and the list comes back to the parent for
aggregation.

>>> from murmur import AgentRuntime, AgentTemplate
>>> from murmur.tools import make_spawn_agents_tool
>>>
>>> runtime = AgentRuntime()
>>> swarm = AgentTemplate(model="anthropic:claude-sonnet-4-6", ...)
>>> spawn = make_spawn_agents_tool(
...     runtime=runtime,
...     template=swarm,
...     output_type=Finding,        # all children share this output shape
...     max_concurrency=10,
... )
>>> runtime.tools.register("spawn_agents", spawn)
>>>
>>> orchestrator = swarm.agent(
...     name="orchestrator",
...     instructions="Decompose the task; call spawn_agents to delegate.",
...     output_type=FinalReport,
...     tools=frozenset({"spawn_agents"}),
... )

The factory bounds what the LLM can spawn (mode A): the parent picks
``name`` / ``instructions`` / ``input`` per child and nothing else.
Per-child failures are captured into ``SpawnResult(success=False,
error=...)`` rather than propagated — partial fan-outs always return.

Cascading depth limiting (parent → child → grandchild) is **not** enforced
here today: the tool's max_concurrency caps simultaneity, but a child that
also has ``spawn_agents`` in its ``tools=`` could in principle recurse.
Don't add ``spawn_agents`` to the template's tool surface — register it
explicitly only on the orchestrator's per-agent tool set. Full
cycle-detection lands alongside the cascading-spawn graph work.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from murmur.types import TaskSpec

if TYPE_CHECKING:
    from murmur.runtime import AgentRuntime
    from murmur.templates import AgentTemplate


class SpawnSpec(BaseModel):
    """One child to spawn — the shape the LLM picks per fan-out slot."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(
        description="Stable identifier for this child run; used in logs and events."
    )
    instructions: str = Field(
        description=(
            "System prompt for the child agent. The template's pre_instruction "
            "(if set) is prepended automatically — don't repeat it here."
        )
    )
    input: str = Field(description="Task input string passed to the child.")


class SpawnResult(BaseModel):
    """One child's outcome — what the parent agent consumes."""

    model_config = ConfigDict(frozen=True)

    name: str
    """The child's :attr:`SpawnSpec.name`, echoed back so the parent can
    correlate without tracking order."""

    success: bool
    """``True`` when the child returned a validated output; ``False`` on
    spawn / dispatch / validation failure."""

    output: dict[str, Any] | None = None
    """The child's output, dumped via Pydantic's ``model_dump()``. ``None``
    when ``success`` is ``False``. ``Any`` here covers the heterogeneous
    payload shapes a child's ``output_type`` may produce — typing is
    statically restored by the parent's downstream handling."""

    error: str | None = None
    """Stringified failure cause when ``success`` is ``False``."""


def make_spawn_agents_tool(
    *,
    runtime: AgentRuntime,
    template: AgentTemplate,
    output_type: type[BaseModel],
    max_concurrency: int = 10,
) -> Callable[[list[SpawnSpec]], Awaitable[list[SpawnResult]]]:
    """Build an LLM-callable tool that spawns child agents under ``template``.

    Args:
        runtime: The runtime that will dispatch the children. Children are
            run via ``runtime.run`` with the same backend / event emitter /
            cost budget as any direct call.
        template: Bounds what the LLM can spawn. Trust level, model, and
            tool surface come from the template; the LLM cannot escalate.
        output_type: Output type shared by every child this tool spawns.
            (Per-child output types — mode B — are a future extension; this
            factory enforces a single shape today.)
        max_concurrency: Cap on simultaneous in-flight children. Defaults to 10.

    Returns:
        An async callable suitable for registration on a
        :class:`ToolRegistry` (the runtime's ``runtime.tools.register(...)``)
        and forwarding to the parent agent's ``tools=`` set.

    The returned callable's signature is ``(specs: list[SpawnSpec]) ->
    list[SpawnResult]`` — PydanticAI's schema introspection turns that
    into the tool's JSON schema for the LLM.
    """
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")

    sem = asyncio.Semaphore(max_concurrency)

    async def _run_one(spec: SpawnSpec) -> SpawnResult:
        async with sem:
            try:
                child = template.agent(
                    name=spec.name,
                    instructions=spec.instructions,
                    output_type=output_type,
                )
            except Exception as exc:
                # Materialisation failure (validation error, missing model,
                # etc.) — treat as a child failure rather than propagating.
                return SpawnResult(name=spec.name, success=False, error=str(exc))
            try:
                result = await runtime.run(child, TaskSpec(input=spec.input))
            except Exception as exc:
                return SpawnResult(name=spec.name, success=False, error=str(exc))
            if result.is_ok() and result.output is not None:
                return SpawnResult(
                    name=spec.name,
                    success=True,
                    output=result.output.model_dump(),
                )
            error_msg = (
                str(result.error) if result.error is not None else "unknown failure"
            )
            return SpawnResult(name=spec.name, success=False, error=error_msg)

    async def spawn_agents(specs: list[SpawnSpec]) -> list[SpawnResult]:
        """Spawn child agents in parallel under the bound template.

        Each spec materialises a child via the template (inheriting trust
        level, model, tool surface, etc.) and dispatches it through the
        runtime. Returns one :class:`SpawnResult` per spec, in order, with
        per-child failures captured rather than raised.
        """
        if not specs:
            return []
        return await asyncio.gather(*(_run_one(s) for s in specs))

    return spawn_agents


__all__ = [
    "SpawnResult",
    "SpawnSpec",
    "make_spawn_agents_tool",
]
