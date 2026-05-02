# Groups

Declarative multi-agent topologies. `AgentGroup` holds a frozen DAG of
agents connected by `Edge` objects; the runner is invoked through
`AgentRuntime.run_group()`.

```python
from murmur import AgentGroup, Edge, FanOut
from murmur.groups import EdgeMapper, get_fan_out_field
```

## `AgentGroup`

::: murmur.AgentGroup
    options:
      heading_level: 3
      show_bases: false

## `Edge`

::: murmur.Edge
    options:
      heading_level: 3
      show_bases: false

## `EdgeMapper`

::: murmur.groups.EdgeMapper
    options:
      heading_level: 3
      show_bases: false

## `FanOut`

::: murmur.FanOut
    options:
      heading_level: 3
      show_bases: false

## `get_fan_out_field`

::: murmur.groups.get_fan_out_field
    options:
      heading_level: 3
