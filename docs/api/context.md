# Context passers

A context passer decides what flows into a spawn — fresh, full, or a
subset. Concretes here satisfy the `ContextPasser` Protocol structurally.

```python
from murmur.context import FullContextPasser, NullContextPasser
```

Phase 3 adds `SummaryContextPasser` (issue `murmur-ai-7cw`) and
`SelectiveContextPasser` (issue `murmur-ai-d50`). The latter doubles as
the untrusted-context sanitiser on the result-return path.

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
