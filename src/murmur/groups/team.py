"""``AgentTeam`` — coordinator + closed menu of typed delegate sub-agents.

The hierarchical-coordination shape: one coordinator :class:`Agent` plus a
``Mapping[str, Agent]`` of named delegates with disjoint
:attr:`Agent.input_type` declarations. At dispatch the runtime auto-
registers a typed ``delegate(target, input)`` tool on the coordinator's
surface; the LLM picks a target from a closed :class:`Literal` enum,
supplies typed input, gets typed output back, optionally repeats up to
:attr:`AgentTeam.max_rounds`, then synthesises against
:attr:`AgentTeam.output_type`.

This module ships the spec value type and the tool factory. Dispatch
through :meth:`murmur.AgentRuntime.run_group` lands separately —
``AgentTeam``'s integration into the polymorphic runner is the next
work unit on the coordination-v2 epic.

Per-delegate session memory: when ``retain_delegate_history=True``
(default), the auto-generated tool tracks each delegate's input/output
exchanges across one ``run_group`` invocation. The next call to the
same delegate sees the prior exchange via :attr:`AgentContext.messages`
on the dispatched context. The history dict lives in a closure inside
the per-run tool factory — distinct ``run_group(team, ...)`` calls
never share state, and cross-run memory stays explicitly out of scope
(see ``CLAUDE.md §22``).
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, Self, Union

from pydantic import BaseModel, ConfigDict, model_validator

from murmur.agent import Agent
from murmur.core.errors import MurmurError, SpecValidationError, ToolExecutionError
from murmur.types import AgentContext, TaskSpec


class AgentTeam(BaseModel):
    """Coordinator + closed menu of named sub-agents — CrewAI-style hierarchical.

    The coordinator is an :class:`Agent` like any other. At dispatch the
    runtime auto-registers a typed ``delegate(target, input)`` tool on
    its surface; the LLM picks a target name from the menu, supplies
    typed input, gets typed output back in its prompt, optionally
    repeats up to :attr:`max_rounds`, then synthesises against
    :attr:`output_type`.

    By default each delegate retains conversation history *within one*
    ``runtime.run_group(team, ...)`` invocation — when the coordinator
    calls ``delegate("billing", X)`` twice, the billing agent sees the
    prior exchange on the second call. Disable via
    ``retain_delegate_history=False`` when delegates should be stateless
    across calls (independent classification tasks where prior
    exchanges would bias the next).

    History is strictly per-run — does NOT survive between successive
    ``runtime.run_group(team, ...)`` calls. Cross-run memory remains
    explicitly out of scope per ``CLAUDE.md §22``; build that as a tool
    against your own store.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    """Stable identifier — mirrors the registry key and shows up on
    group-scoped events."""

    coordinator: Agent
    """The orchestrator agent. Receives the auto-generated ``delegate``
    tool at dispatch and synthesises the final result against
    :attr:`output_type`."""

    delegates: Mapping[str, Agent]
    """Named menu of sub-agents the coordinator can dispatch to. Keys
    are user-facing target names (the ``Literal`` choice the LLM picks
    from); values are the :class:`Agent` instances. Each delegate must
    declare a unique :attr:`Agent.input_type` for typed routing."""

    output_type: type[BaseModel]
    """Pydantic model class the coordinator's final output validates
    against. Mirrors :attr:`Agent.output_type`."""

    max_rounds: int = 10
    """Soft cap on ``delegate()`` invocations per coordinator turn.
    Independent of :attr:`RuntimeOptions.max_spawn_depth` (which still
    bounds total cascade depth)."""

    retain_delegate_history: bool = True
    """When ``True`` (default), each delegate sees its own prior
    exchanges accumulated across one ``run_group(team, ...)`` call. When
    ``False``, every delegate dispatch starts with empty
    ``AgentContext.messages``."""

    @model_validator(mode="after")
    def _validate_team(self) -> Self:
        if not self.delegates:
            raise SpecValidationError(f"AgentTeam {self.name!r} has no delegates")
        if self.coordinator in self.delegates.values():
            raise SpecValidationError(
                f"AgentTeam {self.name!r}: coordinator "
                f"{self.coordinator.name!r} cannot also be a delegate"
            )
        seen: dict[type[BaseModel], str] = {}
        for delegate_name, agent in self.delegates.items():
            if agent.input_type is None:
                raise SpecValidationError(
                    f"AgentTeam {self.name!r}: delegate {delegate_name!r} "
                    f"({agent.name!r}) must declare Agent.input_type for "
                    f"typed routing"
                )
            existing = seen.get(agent.input_type)
            if existing is not None:
                raise SpecValidationError(
                    f"AgentTeam {self.name!r}: delegates {existing!r} and "
                    f"{delegate_name!r} both claim input_type "
                    f"{agent.input_type.__name__!r}; ambiguous routing"
                )
            seen[agent.input_type] = delegate_name
        # Insulate from caller mutation by storing an independent copy
        # of the input dict. Earlier iterations wrapped this in
        # :class:`types.MappingProxyType` for hard read-only
        # enforcement; that broke ``model_copy(deep=True)`` and
        # ``copy.deepcopy`` because ``mappingproxy`` isn't picklable.
        # ``model_config(frozen=True)`` still blocks whole-attribute
        # reassignment (``team.delegates = ...``); reaching through
        # the stored dict reference is technically possible but
        # undefined behaviour — treat ``AgentTeam`` as read-only
        # after construction. Mirrors the ``GroupResult.outputs``
        # pattern.
        if not isinstance(self.delegates, dict):
            object.__setattr__(self, "delegates", dict(self.delegates))
        return self


# ---------------------------------------------------------------------------
# Internal: per-run history-injecting context passer
# ---------------------------------------------------------------------------


class _HistoryContextPasser:
    """ContextPasser that injects a fixed ``messages`` tuple at dispatch.

    Used internally by :func:`_make_delegate_tool` to thread
    per-delegate session memory into the dispatched
    :class:`AgentContext` without burdening the user with a particular
    ``ContextPasser`` choice on each delegate. The closure-bound
    history list grows as the delegate is invoked across one team run;
    the passer reads it at dispatch time, so the latest exchanges are
    visible to the LLM tool loop.

    Bookkeeping fields (``depth`` / ``parent_agent`` /
    ``parent_trace_id`` / ``ancestors``) are re-overlaid by
    ``runtime.run`` after the passer returns, so the synthesized
    context still carries cascading-spawn linkage correctly —
    ``messages`` is the only field that survives this passer's
    return value.
    """

    __slots__ = ("_history",)

    def __init__(self, history: list[Mapping[str, str]]) -> None:
        self._history = history

    async def prepare(
        self,
        context: AgentContext,  # noqa: ARG002 — ContextPasser protocol arg
        task: TaskSpec,  # noqa: ARG002 — ContextPasser protocol arg
    ) -> AgentContext:
        return AgentContext(messages=tuple(self._history))


# ---------------------------------------------------------------------------
# Tool factory
# ---------------------------------------------------------------------------


def _make_delegate_tool(
    runtime: Any,
    delegates: Mapping[str, Agent],
    *,
    retain_history: bool,
    max_rounds: int | None = None,
) -> Callable[..., Awaitable[dict[str, Any]]]:
    """Build a typed ``delegate(target, input)`` callable for one team run.

    Returns an ``async`` callable whose signature carries a
    :class:`Literal` of the delegate names and a :class:`Union` of
    every delegate's ``input_type``. PydanticAI's tool-schema
    introspection consumes those annotations as a closed enum + typed
    payload, so the LLM sees the menu (not a free-form string) and the
    typed shape of each input.

    The closure owns a per-run history dict keyed by delegate name. A
    fresh factory call produces a fresh closure — distinct
    ``runtime.run_group(team, ...)`` invocations never share memory.

    Per-call failure semantics mirror :func:`murmur.tools.make_spawn_agents_tool`:
    a failed delegate dispatch raises :class:`ToolExecutionError` so
    the coordinator's tool loop sees the failure inline. The caller
    decides whether to retry, route to a different delegate, or
    surface the error in the final synthesis.
    """
    if not delegates:
        raise SpecValidationError("_make_delegate_tool requires at least one delegate")

    # Snapshot the delegates mapping into an immutable dict so the
    # closure's routing table can't be mutated post-factory by
    # rewriting the team's stored ``delegates`` dict (post-construction
    # mutation through the model's reference is undefined behaviour
    # but technically possible — see ``AgentTeam._validate_team``).
    # Fresh dict per factory call; the closure holds it privately.
    delegates = dict(delegates)
    delegate_names = tuple(delegates.keys())
    # Dynamic Literal / Union construction — runtime values, can't be
    # static-checked. PydanticAI's tool-schema introspection consumes
    # these resolved types via ``__signature__`` below.
    target_literal: Any = Literal[delegate_names]  # ty: ignore[invalid-type-form]
    input_types = tuple(
        agent.input_type for agent in delegates.values() if agent.input_type is not None
    )
    input_union: Any = Union[input_types]  # noqa: UP007

    histories: dict[str, list[Mapping[str, str]]] = {
        name: [] for name in delegate_names
    }
    # Per-run round counter — bumped on every delegate dispatch so a
    # runaway coordinator can't burn through delegates indefinitely.
    # Independent of ``RuntimeOptions.max_spawn_depth`` (which still
    # bounds total cascade depth).
    rounds = [0]

    async def delegate(target: target_literal, input: input_union) -> dict[str, Any]:  # ty: ignore[invalid-type-form]
        agent = delegates[target]
        # Schema-side mismatch guard. ``Literal[*names] + Union[*types]``
        # advertises a closed enum + a typed input but those are
        # independent in JSON-schema land — PydanticAI accepts any
        # (target, input_member) pair. Reject mismatched routings here
        # so the coordinator's tool loop sees the failure inline.
        if agent.input_type is not None and not isinstance(input, agent.input_type):
            raise ToolExecutionError(
                f"delegate {target!r} expects input of type "
                f"{agent.input_type.__name__!r}, got "
                f"{type(input).__name__!r}"
            )
        if max_rounds is not None and rounds[0] >= max_rounds:
            raise ToolExecutionError(
                f"AgentTeam delegate budget exhausted: max_rounds={max_rounds} reached"
            )
        rounds[0] += 1

        try:
            if retain_history:
                substituted = agent.model_copy(
                    update={"context_passer": _HistoryContextPasser(histories[target])}
                )
                result = await runtime.run(
                    substituted, TaskSpec(input=input.model_dump_json())
                )
            else:
                result = await runtime.run(
                    agent, TaskSpec(input=input.model_dump_json())
                )
        except MurmurError as exc:
            # Runtime-level rejections (SpawnCycleError, DepthLimitError,
            # SpawnCapError, BudgetExceededError) can fire before the
            # backend receives the dispatch. Normalise them to the same
            # ToolExecutionError shape the per-call failure path emits
            # so the coordinator sees a consistent error surface.
            raise ToolExecutionError(
                f"delegate {target!r} failed: {type(exc).__name__}: {exc}"
            ) from exc
        if not result.is_ok():
            raise ToolExecutionError(f"delegate {target!r} failed: {result.error}")
        output_dump: dict[str, Any] = (
            result.output.model_dump() if result.output is not None else {}
        )
        if retain_history:
            histories[target].append(
                {"role": "user", "content": input.model_dump_json()}
            )
            histories[target].append(
                {"role": "assistant", "content": json.dumps(output_dump)}
            )
        return output_dump

    delegate.__name__ = "delegate"
    delegate.__doc__ = (
        "Dispatch to a named delegate sub-agent. Returns the delegate's "
        "structured output as a dict. Each delegate retains its own "
        "conversation history across calls within one team run when the "
        "team's retain_delegate_history is True."
    )
    # ``from __future__ import annotations`` stringifies static annotations
    # on the def — but PydanticAI's tool-schema introspection (and
    # ``inspect.signature`` consumers) need the dynamic Literal/Union
    # types resolved. Overwrite ``__annotations__`` with the real types
    # and pin a custom ``__signature__`` so both forms agree.
    delegate.__annotations__ = {
        "target": target_literal,
        "input": input_union,
        "return": dict[str, Any],
    }
    delegate.__signature__ = inspect.Signature(  # ty: ignore[unresolved-attribute]
        parameters=[
            inspect.Parameter(
                "target",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=target_literal,
            ),
            inspect.Parameter(
                "input",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=input_union,
            ),
        ],
        return_annotation=dict[str, Any],
    )
    return delegate


__all__ = ["AgentTeam"]
