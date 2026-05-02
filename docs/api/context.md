# Context passers

A context passer decides what flows into a spawn — fresh, full, or a
subset. Concretes here satisfy the `ContextPasser` Protocol structurally.

```python
from murmur.context import FullContextPasser, NullContextPasser
```

Two more concretes are queued: `SummaryContextPasser`
(cheap-model summarisation with token-budget aware deduction) and
`SelectiveContextPasser` (relevance-pruned, doubles as the
untrusted-context sanitiser on the result-return path).

## `FullContextPasser`

::: murmur.context.FullContextPasser
    options:
      heading_level: 3
      show_bases: false

## `NullContextPasser`

::: murmur.context.NullContextPasser
    options:
      heading_level: 3
      show_bases: false
