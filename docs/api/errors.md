# Errors

Every failure mode in Murmur wraps in a `MurmurError` subclass.
Catch narrow, raise specific. Core code never raises raw `Exception`
or `ValueError`.

```python
from murmur.core.errors import (
    AllAgentsFailedError,
    BudgetExceededError,
    ContextError,
    DepthLimitError,
    MurmurError,
    RegistryError,
    SpawnError,
    SpecValidationError,
    ToolExecutionError,
    TopologyError,
    TrustViolationError,
)
```

## Hierarchy

```
MurmurError
├── SpawnError
├── ToolExecutionError
├── ContextError
├── BudgetExceededError
├── DepthLimitError
├── RegistryError
├── TrustViolationError
├── AllAgentsFailedError
└── SpecValidationError
    └── TopologyError
```

## Base

### `MurmurError`

::: murmur.core.errors.MurmurError
    options:
      heading_level: 4
      show_bases: false
      members: false

## Runtime errors

### `SpawnError`

::: murmur.core.errors.SpawnError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `ToolExecutionError`

::: murmur.core.errors.ToolExecutionError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `ContextError`

::: murmur.core.errors.ContextError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `BudgetExceededError`

::: murmur.core.errors.BudgetExceededError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `DepthLimitError`

::: murmur.core.errors.DepthLimitError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `TrustViolationError`

::: murmur.core.errors.TrustViolationError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `AllAgentsFailedError`

::: murmur.core.errors.AllAgentsFailedError
    options:
      heading_level: 4
      show_bases: false
      members: false

## Validation errors

### `RegistryError`

::: murmur.core.errors.RegistryError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `SpecValidationError`

::: murmur.core.errors.SpecValidationError
    options:
      heading_level: 4
      show_bases: false
      members: false

### `TopologyError`

::: murmur.core.errors.TopologyError
    options:
      heading_level: 4
      show_bases: false
      members: false
