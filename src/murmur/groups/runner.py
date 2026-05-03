"""``run_group`` — walk an :class:`AgentGroup` topology and dispatch each step.

The walker:

1. Topologically sorts the topology.
2. Walks each agent in order. The entry node receives the original
   :class:`TaskSpec`. Every other node's input is computed from its
   upstream's *output(s)*:
   - If the incoming edge has a ``mapper``: call it with either the typed
     output (single upstream) or the list of successful typed outputs
     (fan-out upstream); use whatever it returns.
   - Otherwise, if the upstream's ``output_type`` has a
     :data:`FanOut`-annotated field: spawn one downstream per item in
     that field.
   - Otherwise, JSON-serialise the upstream output and pass it as the
     ``TaskSpec.input`` for one downstream dispatch.
3. Single ``TaskSpec`` → :meth:`runtime.run`. List of ``TaskSpec`` →
   :meth:`runtime.gather` (with the edge's ``max_concurrency``).
4. Stores the typed result, keyed by upstream agent. Repeats for the
   next node.
5. Returns the terminal node's :class:`AgentResult`. With branch routing
   the topology may declare several terminal nodes; exactly one must
   fire at runtime — the runner returns its result.

Failed slots inside a fan-out tier are filtered *before* the next mapper
is called — mappers never see error envelopes. If every slot in a fan-out
fails, :class:`AllAgentsFailedError` is raised and the walker stops.

**Conditional edges:** an :class:`Edge` may carry a ``condition``
predicate. The runner evaluates it against the upstream's typed output
before traversing. Predicates may be sync or async. False → skip the
edge entirely; downstream nodes that depend exclusively on the skipped
edge are also skipped. If a predicate raises, the runner wraps the
error in :class:`TopologyError` with the offending edge's
upstream/downstream metadata.

**Multi-input aggregation:** a node may have several incoming edges.
Exactly one of those edges must carry an aggregating ``mapper`` whose
signature widens to
``Callable[[dict[str, BaseModel | list[BaseModel]]], TaskSpec | list[TaskSpec]]``
— the dict is keyed by upstream agent name. Per-upstream filtering of
failed slots happens before the mapper runs; an upstream whose entire
batch failed contributes an empty list. If *every* upstream is dead,
:class:`AllAgentsFailedError` propagates. ``FanOut × FanOut`` (two
upstreams both producing list outputs) is rejected — force users to
write an explicit aggregator rather than face an implicit cross-product.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from typing import TYPE_CHECKING

from pydantic import BaseModel

from murmur.core.errors import AllAgentsFailedError, SpecValidationError, TopologyError
from murmur.groups._introspection import get_fan_out_field
from murmur.types import AgentResult, GroupResult, ResultMetadata, TaskSpec

if TYPE_CHECKING:
    from murmur.agent import Agent
    from murmur.groups.edge import Edge, EdgeMapper
    from murmur.groups.spec import AgentGroup
    from murmur.runtime import AgentRuntime


# Shape of what each node stored after running. Either a single typed
# AgentResult (when the node was dispatched once), or a list of typed
# AgentResults (when the node was the target of a fan-out gather).
_NodeOutput = AgentResult[BaseModel] | list[AgentResult[BaseModel]]


async def run_group(
    runtime: AgentRuntime,
    group: AgentGroup,
    task: TaskSpec,
) -> AgentResult[BaseModel] | GroupResult:
    """Execute ``group`` against ``task`` and return the terminal result(s).

    Single-terminal topologies — and branch-routing topologies where the
    runtime predicates collapse onto a single fired leaf — return a
    plain :class:`AgentResult`, identical to the pre-multi-terminal
    contract. Topologies whose runtime walk fires N>=2 terminal nodes
    return a :class:`GroupResult` keyed by ``Agent.name`` with
    aggregate metadata.

    Topology shape *alone* doesn't pick the return type — runtime
    behaviour does. A two-leaf topology where one branch is gated by
    a False ``Edge.condition`` still returns :class:`AgentResult`
    because only one terminal actually fired.
    """
    terminals = group.terminal_nodes()
    if not terminals:
        raise TopologyError(f"AgentGroup {group.name!r} has no terminal node")
    tiers = group.topological_tiers()
    results: dict[Agent, _NodeOutput] = {}
    incoming: dict[Agent, list[tuple[Agent, Edge]]] = _incoming_edges(group)
    skipped: set[Agent] = set()

    for tier in tiers:
        if len(tier) == 1:
            # Fast path — preserves zero-overhead dispatch for linear DAGs.
            await _walk_one(
                node=tier[0],
                runtime=runtime,
                task=task,
                incoming=incoming,
                results=results,
                skipped=skipped,
            )
            continue

        # Tier with N>=2 sibling nodes (no inter-tier dependencies, by
        # construction of topological_tiers). Dispatch concurrently with
        # fail-fast cancellation — see ``_dispatch_tier_parallel``.
        tier_outputs = await _dispatch_tier_parallel(
            tier=tier,
            runtime=runtime,
            task=task,
            incoming=incoming,
            results=results,
            skipped=skipped,
        )
        for node, dispatched in zip(tier, tier_outputs, strict=True):
            if isinstance(dispatched, _Skipped):
                skipped.add(node)
            else:
                results[node] = dispatched

    fired_terminals = [t for t in terminals if t in results]
    if not fired_terminals:
        raise TopologyError(
            f"AgentGroup {group.name!r} produced no terminal result — every "
            "outgoing edge was skipped by a False condition"
        )

    if len(fired_terminals) == 1:
        final = results[fired_terminals[0]]
        if isinstance(final, list):
            # A terminal that received a fan-out without a downstream
            # synthesiser is a topology mistake — we don't have a single
            # result to return.
            raise TopologyError(
                f"terminal node {fired_terminals[0].name!r} received a fan-out "
                "batch without an aggregating downstream"
            )
        return final

    # Multi-terminal: build a GroupResult keyed by agent name.
    leaf_results: dict[str, AgentResult[BaseModel]] = {}
    for terminal in fired_terminals:
        node_output = results[terminal]
        if isinstance(node_output, list):
            raise TopologyError(
                f"terminal node {terminal.name!r} received a fan-out batch "
                "without an aggregating downstream"
            )
        leaf_results[terminal.name] = node_output
    return GroupResult(
        outputs=leaf_results,
        metadata=_aggregate_group_metadata(leaf_results, task),
    )


def _aggregate_group_metadata(
    leaf_results: Mapping[str, AgentResult[BaseModel]],
    task: TaskSpec,
) -> ResultMetadata:
    """Combine per-leaf :class:`ResultMetadata` into a group-level snapshot.

    Sums tokens and cost across every fired leaf; takes the max
    duration (fired leaves run in parallel tiers so wall-clock isn't
    additive); marks ``backend="group"`` and threads the originating
    task's ``request_id`` as the trace id.
    """
    total_tokens = sum(r.metadata.tokens_used for r in leaf_results.values())
    total_cost = sum(r.metadata.cost_usd for r in leaf_results.values())
    max_duration = max(
        (r.metadata.duration_ms for r in leaf_results.values()), default=0
    )
    return ResultMetadata(
        duration_ms=max_duration,
        tokens_used=total_tokens,
        cost_usd=total_cost,
        backend="group",
        trace_id=task.request_id,
    )


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------


class _Skipped:
    """Sentinel returned when a dispatch helper decides to skip the node."""

    __slots__ = ()


_SKIPPED = _Skipped()


# ---------------------------------------------------------------------------
# Tier-parallel dispatch
# ---------------------------------------------------------------------------


async def _dispatch_tier_parallel(
    *,
    tier: tuple[Agent, ...],
    runtime: AgentRuntime,
    task: TaskSpec,
    incoming: dict[Agent, list[tuple[Agent, Edge]]],
    results: dict[Agent, _NodeOutput],
    skipped: set[Agent],
) -> list[_NodeOutput | _Skipped]:
    """Dispatch a tier of >=2 sibling nodes concurrently with fail-fast cancellation.

    Each sibling becomes its own ``asyncio.Task`` so the contextvar fork
    happens at task creation — ``_current_spawn`` propagates from the
    runner task into each sibling without leaking back. On the first
    sibling failure (typically :class:`TopologyError` from a condition
    predicate or :class:`AllAgentsFailedError` from a fully-dead upstream
    batch), pending siblings are cancelled and drained before the
    exception propagates. That keeps in-flight siblings from continuing
    to claim spawn slots, charge token budget, or emit completion events
    against a run the caller has already abandoned — matching the
    sequential walker's fail-fast resource accounting one-for-one.

    The first exception is re-raised with its original type — no
    :class:`ExceptionGroup` wrapping — so callers that ``pytest.raises``
    on the original error keep working.

    ``runtime.run`` itself never raises — its failures land in
    ``AgentResult.error`` — so this fail-fast path only triggers on
    structural errors raised by the dispatch helpers themselves.
    """
    sibling_tasks: list[asyncio.Task[_NodeOutput | _Skipped]] = [
        asyncio.create_task(
            _resolve_node(
                node=node,
                runtime=runtime,
                task=task,
                incoming=incoming,
                results=results,
                skipped=skipped,
            ),
            name=f"murmur-tier:{node.name}",
        )
        for node in tier
    ]
    try:
        return await asyncio.gather(*sibling_tasks, return_exceptions=False)
    except BaseException:
        for sibling_task in sibling_tasks:
            if not sibling_task.done():
                sibling_task.cancel()
        # Drain so cancelled siblings settle before we propagate. Errors
        # surfaced during drain are swallowed — we already have the
        # original failure to re-raise.
        await asyncio.gather(*sibling_tasks, return_exceptions=True)
        raise


# ---------------------------------------------------------------------------
# Per-node walk
# ---------------------------------------------------------------------------


async def _walk_one(
    *,
    node: Agent,
    runtime: AgentRuntime,
    task: TaskSpec,
    incoming: dict[Agent, list[tuple[Agent, Edge]]],
    results: dict[Agent, _NodeOutput],
    skipped: set[Agent],
) -> None:
    """Resolve and dispatch ``node``, mutating ``results``/``skipped`` in place.

    Used on the single-tier fast path. Mirrors the inline behaviour the
    walker had before tier-parallelism — preserved here so linear DAGs
    don't pay any ``asyncio.gather`` overhead.
    """
    dispatched = await _resolve_node(
        node=node,
        runtime=runtime,
        task=task,
        incoming=incoming,
        results=results,
        skipped=skipped,
    )
    if isinstance(dispatched, _Skipped):
        skipped.add(node)
    else:
        results[node] = dispatched


async def _resolve_node(
    *,
    node: Agent,
    runtime: AgentRuntime,
    task: TaskSpec,
    incoming: dict[Agent, list[tuple[Agent, Edge]]],
    results: dict[Agent, _NodeOutput],
    skipped: set[Agent],
) -> _NodeOutput | _Skipped:
    """Compute the dispatch for one node — entry, single-input, or multi-input.

    Reads ``results`` and ``skipped`` but never writes them; the caller is
    responsible for storing the outcome. Read-only access keeps the
    function safe to invoke under ``asyncio.gather`` for sibling tiers
    where each task only sees state that was committed by *earlier* tiers.
    """
    upstream_pairs = incoming.get(node, [])
    if not upstream_pairs:
        # Entry node — receives the original TaskSpec.
        return await runtime.run(node, task)
    if len(upstream_pairs) == 1:
        return await _dispatch_single_input(
            runtime=runtime,
            node=node,
            upstream=upstream_pairs[0][0],
            edge=upstream_pairs[0][1],
            results=results,
            skipped=skipped,
        )
    return await _dispatch_multi_input(
        runtime=runtime,
        node=node,
        upstream_pairs=upstream_pairs,
        results=results,
        skipped=skipped,
    )


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------


async def _dispatch_single_input(
    *,
    runtime: AgentRuntime,
    node: Agent,
    upstream: Agent,
    edge: Edge,
    results: dict[Agent, _NodeOutput],
    skipped: set[Agent],
) -> _NodeOutput | _Skipped:
    if upstream in skipped or upstream not in results:
        return _SKIPPED
    typed_outputs = _filter_successes(results[upstream], upstream_name=upstream.name)
    if not await _condition_fires(edge, upstream, node, typed_outputs):
        return _SKIPPED

    # Heterogeneous fan-out: when the upstream's output_type carries a
    # ``FanOut[list[T1 | T2 | ...]]`` field, each downstream of the source
    # receives only the items whose declared ``Agent.input_type`` matches
    # via :func:`isinstance`. Routing uses ``isinstance`` rather than an
    # exact-type identity check so concrete Pydantic subclasses (e.g.
    # discriminated-union variants) route correctly; the construction-time
    # validator rejects subclass relationships *between* union members so
    # this lookup is unambiguous.
    if (
        edge.mapper is None
        and not isinstance(typed_outputs, list)
        and node.input_type is not None
    ):
        fan_out = get_fan_out_field(upstream.output_type)
        if fan_out is not None and len(fan_out[1]) > 1:
            field_name, item_types = fan_out
            items = getattr(typed_outputs, field_name)
            # Pre-validate: every item must be an instance of some union
            # member. An instance of an unrelated type means the user's
            # data model has drifted from the declared union — surface
            # that as a loud error rather than silently dropping the item
            # (which is what the per-handler filter would otherwise do).
            for item in items:
                if not any(isinstance(item, t) for t in item_types):
                    raise SpecValidationError(
                        f"fan-out source {upstream.name!r} produced item of "
                        f"type {type(item).__name__!r}; expected an instance "
                        f"of one of "
                        f"{[t.__name__ for t in item_types]}"
                    )
            matched = [item for item in items if isinstance(item, node.input_type)]
            tasks = [TaskSpec(input=_to_input_string(item)) for item in matched]
            return await runtime.gather(
                node, tasks, max_concurrency=edge.max_concurrency
            )

    downstream_input = _resolve_downstream_input(
        edge=edge,
        upstream=upstream,
        typed_outputs=typed_outputs,
    )
    if isinstance(downstream_input, list):
        return await runtime.gather(
            node, downstream_input, max_concurrency=edge.max_concurrency
        )
    return await runtime.run(node, downstream_input)


async def _dispatch_multi_input(
    *,
    runtime: AgentRuntime,
    node: Agent,
    upstream_pairs: list[tuple[Agent, Edge]],
    results: dict[Agent, _NodeOutput],
    skipped: set[Agent],
) -> _NodeOutput | _Skipped:
    """Aggregate inputs from multiple upstream agents into one dispatch.

    Per-upstream behaviour:
    - Skipped/missing upstream → contributes nothing.
    - Edge condition False → contributes nothing.
    - Single AgentResult success → contributes the typed output.
    - Single AgentResult failure → upstream considered dead.
    - Fan-out: filter failed slots; if every slot failed the upstream is dead.
    - Dead upstream → key still appears in the dict with an empty list,
      so the aggregator mapper can decide what to do.
    """
    contributions: dict[str, BaseModel | list[BaseModel]] = {}
    aggregator: Edge | None = None
    duplicate_aggregators: list[str] = []
    fan_out_keys: list[str] = []
    any_alive = False

    for upstream, edge in upstream_pairs:
        if edge.mapper is not None:
            if aggregator is not None:
                duplicate_aggregators.append(upstream.name)
            else:
                aggregator = edge

        if upstream in skipped or upstream not in results:
            contributions[upstream.name] = []
            continue

        node_output = results[upstream]
        typed: BaseModel | list[BaseModel]
        if isinstance(node_output, list):
            successful: list[BaseModel] = [
                r.output for r in node_output if r.is_ok() and r.output is not None
            ]
            typed = successful  # may be empty
        elif node_output.is_ok() and node_output.output is not None:
            typed = node_output.output
        else:
            contributions[upstream.name] = []
            continue

        if not await _condition_fires(edge, upstream, node, typed):
            contributions[upstream.name] = []
            continue

        contributions[upstream.name] = typed
        if isinstance(typed, list):
            if not typed:
                # Filtered down to empty — treat as dead upstream.
                continue
            fan_out_keys.append(upstream.name)
            any_alive = True
        else:
            any_alive = True

    if duplicate_aggregators:
        raise TopologyError(
            f"node {node.name!r} has multiple incoming edges with mappers "
            f"({duplicate_aggregators!r}); exactly one aggregating mapper "
            "is allowed per multi-input node"
        )
    if not any_alive:
        raise AllAgentsFailedError(
            f"every upstream of {node.name!r} failed; aggregator not called"
        )
    if aggregator is None:
        # Two distinct failure modes — both end up here without a mapper.
        # Heterogeneous fan-out routes a single source's items into N
        # parallel handler lists that converge on one synthesiser; the
        # aggregator is the user's explicit acknowledgement that they
        # know how to combine N lists. Without one we can't guess.
        if len(fan_out_keys) > 1:
            raise TopologyError(
                f"node {node.name!r} aggregates two fan-out upstreams "
                f"({fan_out_keys!r}); supply an explicit aggregator mapper "
                "rather than a cross-product"
            )
        raise TopologyError(
            f"node {node.name!r} has multiple incoming edges but none of "
            "them carries an aggregating mapper; attach a mapper to one of "
            f"the incoming edges with signature "
            f"(dict[str, BaseModel | list[BaseModel]]) -> TaskSpec"
        )

    downstream_input = _call_mapper(aggregator.mapper, contributions)
    if isinstance(downstream_input, list):
        return await runtime.gather(
            node, downstream_input, max_concurrency=aggregator.max_concurrency
        )
    return await runtime.run(node, downstream_input)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _incoming_edges(group: AgentGroup) -> dict[Agent, list[tuple[Agent, Edge]]]:
    """Map each downstream agent to **all** incoming ``(upstream, edge)`` pairs.

    Multi-incoming edges (multi-input aggregation) and multi-outgoing
    edges (branch routing) are both supported.
    """
    incoming: dict[Agent, list[tuple[Agent, Edge]]] = {}
    for src in group.topology:
        for edge in group.outgoing_edges(src):
            for tgt in edge.to:
                incoming.setdefault(tgt, []).append((src, edge))
    return incoming


async def _condition_fires(
    edge: Edge,
    upstream: Agent,
    downstream: Agent,
    typed_outputs: BaseModel | list[BaseModel],
) -> bool:
    """Evaluate ``edge.condition`` (if any) and return whether the edge fires.

    Wraps any exception the predicate raises in :class:`TopologyError`
    annotated with the edge's upstream/downstream agent names.
    """
    if edge.condition is None:
        return True
    try:
        result = edge.condition(typed_outputs)  # ty: ignore[invalid-argument-type]
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:
        raise TopologyError(
            f"condition on edge {upstream.name!r} -> {downstream.name!r} "
            f"raised {type(exc).__name__}: {exc}"
        ) from exc
    return bool(result)


def _resolve_downstream_input(
    *,
    edge: Edge,
    upstream: Agent,
    typed_outputs: BaseModel | list[BaseModel],
) -> TaskSpec | list[TaskSpec]:
    """Compute the input(s) for the downstream node from the upstream's output."""
    if edge.mapper is not None:
        return _call_mapper(edge.mapper, typed_outputs)

    # No mapper. Try auto fan-out via FanOut field on the upstream output_type.
    upstream_output_type = upstream.output_type
    fan_out = get_fan_out_field(upstream_output_type)

    if isinstance(typed_outputs, list):
        # Upstream was already a fan-out — without a mapper we can't combine
        # N typed outputs into a single downstream input. Force the user to
        # supply an aggregating mapper.
        raise TopologyError(
            f"edge from {upstream.name!r} aggregates {len(typed_outputs)} "
            f"outputs but has no mapper; supply an aggregating mapper"
        )

    if fan_out is not None:
        field_name, _item_type = fan_out
        items = getattr(typed_outputs, field_name)
        return [TaskSpec(input=_to_input_string(item)) for item in items]

    return TaskSpec(input=_to_input_string(typed_outputs))


def _filter_successes(
    result: _NodeOutput,
    *,
    upstream_name: str,
) -> BaseModel | list[BaseModel]:
    """Strip the AgentResult envelope, dropping failed slots in fan-out batches.

    Raises :class:`AllAgentsFailedError` when an entire fan-out batch failed.
    """
    if isinstance(result, list):
        successful: list[BaseModel] = [
            r.output for r in result if r.is_ok() and r.output is not None
        ]
        if not successful:
            raise AllAgentsFailedError(
                f"every result from {upstream_name!r} failed; "
                f"downstream mapper not called"
            )
        return successful
    if not result.is_ok() or result.output is None:
        # Single-task upstream failure — propagate the error rather than
        # call the mapper.
        raise AllAgentsFailedError(
            f"upstream {upstream_name!r} failed; downstream mapper not called"
        )
    return result.output


def _call_mapper(
    mapper: EdgeMapper | None,
    typed: BaseModel | list[BaseModel] | dict[str, BaseModel | list[BaseModel]],
) -> TaskSpec | list[TaskSpec]:
    """Invoke a user mapper. Mappers are sync, pure, same-type.

    Accepts the multi-input dict shape too — when the downstream node
    aggregates several upstreams the mapper receives
    ``dict[str, BaseModel | list[BaseModel]]`` keyed by upstream agent
    name.
    """
    if mapper is None:
        raise TopologyError("internal: _call_mapper invoked with no mapper")
    out = mapper(typed)
    if isinstance(out, TaskSpec):
        return out
    if isinstance(out, list) and all(isinstance(x, TaskSpec) for x in out):
        return out
    raise TopologyError(
        f"mapper must return TaskSpec or list[TaskSpec]; got {type(out).__name__}"
    )


def _to_input_string(payload: object) -> str:
    """Serialise ``payload`` to a string suitable for ``TaskSpec.input``."""
    if isinstance(payload, BaseModel):
        return payload.model_dump_json()
    if isinstance(payload, str):
        return payload
    return str(payload)


__all__ = ["run_group"]
