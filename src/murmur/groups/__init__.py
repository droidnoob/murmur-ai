"""``murmur.groups`` — declarative multi-agent topologies.

Public API:

- :class:`AgentGroup` — a frozen DAG of agents.
- :class:`Edge` — one connection in that DAG, optionally with a mapper.

The runner (:func:`run_group`) is invoked through
:meth:`murmur.AgentRuntime.run_group` and is not re-exported from
``murmur.groups``.
"""

from murmur.groups._introspection import get_fan_out_field
from murmur.groups.edge import Edge, EdgeMapper
from murmur.groups.spec import AgentGroup

__all__ = [
    "AgentGroup",
    "Edge",
    "EdgeMapper",
    "get_fan_out_field",
]
