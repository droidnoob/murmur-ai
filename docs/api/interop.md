# Interop

Migration adapters between Murmur and the underlying libraries. **Only
this package may import `pydantic_ai` or `faststream` directly** — the
public API rule.

```python
from murmur.interop import as_faststream_handler, from_pydantic_ai
```

See [Migrating from PydanticAI / FastStream](../guides/migration.md) for
worked examples.

## `from_pydantic_ai`

::: murmur.interop.from_pydantic_ai
    options:
      heading_level: 3

## `as_faststream_handler`

::: murmur.interop.as_faststream_handler
    options:
      heading_level: 3
