"""``run_group`` — walk an :class:`AgentGroup` topology and dispatch each step.

Per Addendum 3 §"How Results Flow Between Agents in the DAG". The walker:

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
5. Returns the terminal node's :class:`AgentResult`. (When there are
   multiple terminals — Phase 3 multi-output workflows — the runner raises;
   for now exactly one terminal is required, enforced in
   :class:`AgentGroup`.)

Failed slots inside a fan-out tier are filtered *before* the next mapper
is called — mappers never see error envelopes. If every slot in a fan-out
fails, :class:`AllAgentsFailedError` is raised and the walker stops.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from murmur.core.errors import AllAgentsFailedError, TopologyError
from murmur.groups._introspection import get_fan_out_field
from murmur.types import AgentResult, TaskSpec

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
) -> AgentResult[BaseModel]:
    """Execute ``group`` against ``task`` and return the terminal result."""
    terminals = group.terminal_nodes()
    if len(terminals) != 1:
        raise TopologyError(
            f"AgentGroup {group.name!r} must have exactly one terminal node "
            f"(found {len(terminals)})"
        )
    terminal = terminals[0]
    order = group.topological_order()
    results: dict[Agent, _NodeOutput] = {}
    incoming: dict[Agent, Agent] = _incoming_index(group)

    for node in order:
        upstream = incoming.get(node)
        if upstream is None:
            results[node] = await runtime.run(node, task)
            continue
        edge = group.topology[upstream]
        upstream_result = results[upstream]
        downstream_input = _resolve_downstream_input(
            edge=edge,
            upstream=upstream,
            upstream_result=upstream_result,
        )
        if isinstance(downstream_input, list):
            results[node] = await runtime.gather(
                node,
                downstream_input,
                max_concurrency=edge.max_concurrency,
            )
        else:
            results[node] = await runtime.run(node, downstream_input)

    final = results[terminal]
    if isinstance(final, list):
        # A terminal that received a fan-out without a downstream synthesiser
        # is a topology mistake — we don't have a single result to return.
        raise TopologyError(
            f"terminal node {terminal.name!r} received a fan-out batch "
            f"without an aggregating downstream"
        )
    return final


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _incoming_index(group: AgentGroup) -> dict[Agent, Agent]:
    """Map each downstream agent to its *single* upstream.

    Phase 1 simplification: each node has at most one incoming edge. The
    addendum's full DAG model allows multi-input (an aggregator with multiple
    upstreams); that lands when the workflow engine needs it.
    """
    incoming: dict[Agent, Agent] = {}
    for src, edge in group.topology.items():
        for tgt in edge.to:
            if tgt in incoming:
                raise TopologyError(
                    f"node {tgt.name!r} has multiple upstream edges "
                    f"({incoming[tgt].name!r} and {src.name!r}); Phase 1 "
                    f"supports one incoming edge per node"
                )
            incoming[tgt] = src
    return incoming


def _resolve_downstream_input(
    *,
    edge: Edge,
    upstream: Agent,
    upstream_result: _NodeOutput,
) -> TaskSpec | list[TaskSpec]:
    """Compute the input(s) for the downstream node from the upstream's output."""
    typed_outputs = _filter_successes(upstream_result, upstream_name=upstream.name)

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
    mapper: EdgeMapper,
    typed: BaseModel | list[BaseModel],
) -> TaskSpec | list[TaskSpec]:
    """Invoke a user mapper. Mappers are sync per Addendum 2."""
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
