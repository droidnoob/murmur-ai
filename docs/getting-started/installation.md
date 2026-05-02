# Installation

Murmur supports Python 3.11, 3.12, and 3.13.

## Core install

```bash
pip install murmur-ai
```

This pulls in `pydantic`, `pydantic-ai`, `faststream`, `structlog`, and
`pyyaml`. It's enough to run [`AsyncBackend`](../concepts/backends.md) — the
default in-process backend — without any broker or external service.

## Broker extras

When you're ready to distribute work across machines, add the matching extra:

=== "Kafka"

    ```bash
    pip install "murmur-ai[kafka]"
    ```

=== "NATS"

    ```bash
    pip install "murmur-ai[nats]"
    ```

=== "RabbitMQ"

    ```bash
    pip install "murmur-ai[rabbitmq]"
    ```

=== "Redis"

    ```bash
    pip install "murmur-ai[redis]"
    ```

=== "All four"

    ```bash
    pip install "murmur-ai[all]"
    ```

The extra installs the matching `faststream` integration. Murmur's
[`AgentRuntime`](../concepts/runtime.md) parses the broker URL scheme
(`kafka://`, `nats://`, `amqp://`, `redis://`) and constructs the right
broker internally — you never import `KafkaBroker` etc.

## Server extras

The HTTP server (`murmur.server.AgentServer` and the `murmur serve` CLI)
bring `fastapi`, `uvicorn`, and `sse-starlette`:

```bash
pip install "murmur-ai[server]"
```

## Persistent run-store extras

The default `InMemoryRunStore` loses in-flight runs on restart. For
production, pick one:

```bash
pip install "murmur-ai[sqlite]"          # single-host, file-backed
pip install "murmur-ai[redis-runstore]"  # cluster-wide
pip install "murmur-ai[rocksdb]"         # high-throughput single-host
```

All three implement the same `RunStore` Protocol and pass the same
`RunStoreContract` test suite.

## Container backend (preview)

`murmur-ai[container]` will pull `docker==7.1.0` once `ContainerBackend`
ships — full container isolation per agent run, queued for a future
release.

## Development install

```bash
git clone https://github.com/murmur-ai/murmur && cd murmur
uv sync --group dev
uv run pytest -m "not integration" -q     # 556 tests, all green
```

The dev group installs ruff, ty, pytest, hypothesis, pre-commit, and every
broker / runstore concrete so the contract suites run end-to-end.
