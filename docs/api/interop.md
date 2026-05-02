# Interop

Migration adapters between Murmur and the underlying libraries. **Only
this package may import `pydantic_ai` or `faststream` directly** — the
public API rule.

```python
from murmur.interop import as_faststream_handler, from_pydantic_ai
```

See the migration guides for worked examples:

- [From PydanticAI](../guides/migration-pydantic-ai.md)
- [From FastStream](../guides/migration-faststream.md)
- [From raw asyncio](../guides/migration-asyncio.md)

## `from_pydantic_ai`

::: murmur.interop.from_pydantic_ai
    options:
      heading_level: 3

## `as_faststream_handler`

::: murmur.interop.as_faststream_handler
    options:
      heading_level: 3
