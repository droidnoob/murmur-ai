# Runs

Long-lived run handles for the submit / poll / stream pattern used by
the HTTP server.

```python
from murmur.runs import (
    InMemoryRunStore,
    RedisRunStore,
    RocksDBRunStore,
    RunEvent,
    RunEventType,
    RunProgress,
    RunState,
    RunStatus,
    RunStore,
    SQLiteRunStore,
)
```

The persistent concretes (`SQLite`, `RocksDB`, `Redis`) are lazy-loaded
— `import murmur.runs` works without any of the optional extras
installed. They show up only when accessed.

## `RunStore` Protocol

::: murmur.runs.RunStore
    options:
      heading_level: 3
      show_bases: false

## Value types

### `RunState`

::: murmur.runs.RunState
    options:
      heading_level: 4
      show_bases: false

### `RunStatus`

::: murmur.runs.RunStatus
    options:
      heading_level: 4
      show_bases: false

### `RunProgress`

::: murmur.runs.RunProgress
    options:
      heading_level: 4
      show_bases: false

### `RunEvent`

::: murmur.runs.RunEvent
    options:
      heading_level: 4
      show_bases: false

### `RunEventType`

::: murmur.runs.RunEventType
    options:
      heading_level: 4
      show_bases: false

## Concretes

### `InMemoryRunStore`

::: murmur.runs.InMemoryRunStore
    options:
      heading_level: 4
      show_bases: false

### `SQLiteRunStore`

Requires `pip install "murmur-runtime[sqlite]"`.

::: murmur.runs.sqlite.SQLiteRunStore
    options:
      heading_level: 4
      show_bases: false

### `RocksDBRunStore`

Requires `pip install "murmur-runtime[rocksdb]"`.

::: murmur.runs.rocksdb.RocksDBRunStore
    options:
      heading_level: 4
      show_bases: false

### `RedisRunStore`

Requires `pip install "murmur-runtime[redis-runstore]"`.

::: murmur.runs.redis.RedisRunStore
    options:
      heading_level: 4
      show_bases: false
