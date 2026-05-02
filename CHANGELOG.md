# Changelog

All notable changes to **Murmur** are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) at v0.1.

## [Unreleased]

The runtime is feature-complete for the public-API surface; no version has
shipped yet (`0.0.0` placeholder). Highlights since project inception:

### Added — runtime core

- `Agent` — single unified frozen value object wrapping PydanticAI internally.
  Public fields: `name`, `model`, `instructions`, `output_type`, `input_type`,
  `tools`, `mcp_servers`, `builtin_tools`, `fallback_models`,
  `model_settings`, `trust_level`, `context_passer`, `pre_process`,
  `post_process`.
- `AgentRuntime` with broker-URL parsing (`kafka://`, `nats://`, `amqp://`,
  `redis://`, `memory://`).
- `RuntimeOptions` — `timeout_seconds`, `max_spawn_depth`,
  `retry_max_attempts`, `retry_backoff_factor`, `token_budget`,
  `mcp_eager_start`.
- `runtime.run`, `runtime.gather`, `runtime.run_group`, `runtime.shutdown`
  + `_sync` variants.
- Backends: `ThreadBackend` (default, asyncio) and `JobBackend`
  (FastStream-driven, four broker schemes plus in-memory). Both pass the
  shared `BackendContract` test suite.
- Context passers: `NullContextPasser`, `FullContextPasser`.
- Tool surface: `ToolRegistry`, `StaticToolProvider`, `ToolExecutor` with
  trust-level gating, `ToolFunc[T]` generic.
- Pipeline middleware: `RetryMiddleware`, `TimeoutMiddleware`,
  `DepthLimitMiddleware`, `CostTrackingMiddleware` with `TokenBudget`.
- Domain errors: `MurmurError` hierarchy with 11 subclasses.
- 12 Protocols in `murmur.core.protocols/` defining every pluggable surface.

### Added — groups & DAGs

- `AgentGroup`, `Edge`, `EdgeMapper`, `FanOut` typing helper,
  `get_fan_out_field` introspection.
- Multi-input aggregation, conditional edges (`Edge.condition`), branch
  routing.

### Added — distributed

- `Worker` distributed consumer with lifecycle hooks (`on_task_start`,
  `on_task_complete`, `on_task_error`).
- `Worker --all-from <PATH>` registry auto-discovery.
- Underlying-broker-lib log silencing — `aiokafka` / `aio_pika` / `redis` /
  `nats` / `faststream` lifted to `WARNING`. `Worker.start()` prints a
  Murmur-branded banner to stderr.

### Added — server / client

- `AgentServer` (standalone HTTP) and `AgentRouter` (embedded) — FastAPI
  surface with `POST /run`, `POST /submit`, `GET /runs/{id}`, SSE event
  stream.
- `/healthz` and `/readyz` split.
- `MurmurClient` (HTTP) and `LocalClient` (in-process), both satisfying a
  shared `_RunBackend` Protocol.
- `RunStore` Protocol + four concretes: `InMemoryRunStore`,
  `SQLiteRunStore`, `RocksDBRunStore`, `RedisRunStore`. All pass the shared
  `RunStoreContract` test suite.

### Added — observability

- `RuntimeEvent` typed envelope + `EventType` (12 variants).
- `EventEmitter` Protocol + four concretes: `LogEventEmitter`,
  `SSEEventEmitter`, `MultiEventEmitter`, `BrokerEventBridge`. All pass the
  shared `EventEmitterContract` test suite.
- `RuntimeOptions(token_budget=…)` enforces token-cost ceilings via
  `CostTrackingMiddleware` with pre-check + post-charge semantics; emits
  `BUDGET_EXCEEDED` before raising.
- Distributed event bridge — `AgentRuntime(publish_events=True)` opts the
  publisher into `murmur.events.{runtime_id}` so worker events roll up to a
  central dashboard.
- `murmur serve` standalone HTTP server with `GET /events/stream` SSE.

### Added — MCP

- MCP consume side — `agent.mcp_servers=` accepts `mcp_stdio` / `mcp_http` /
  `mcp_sse` factories. Calls flow through `ToolExecutor` for the same trust
  + lifecycle gate native tools get.
- MCP tool prefixing — `prefix=` lets two servers exposing same-named tools
  coexist.
- MCP eager-start — `RuntimeOptions(mcp_eager_start=True)` holds
  subprocesses warm across runs via per-provider supervisor tasks.
- `Agent.builtin_tools` — PydanticAI provider-side tools (`WebSearchTool`,
  `CodeExecutionTool`, etc.) re-exported under `murmur.tools` so users
  never `import pydantic_ai`. Bypasses `ToolExecutor` by design (provider-
  side execution).
- `Agent.fallback_models` — ordered fallback model strings; wraps as
  `FallbackModel` at dispatch.
- `Agent.model_settings` — per-provider knobs (temperature, max_tokens,
  …) without leaking PydanticAI types.

### Added — interop

- `murmur.interop.from_pydantic_ai` — wrap an existing `pydantic_ai.Agent`
  into a Murmur `Agent`.
- `murmur.interop.as_faststream_handler` — expose a Murmur `Agent` as a
  FastStream subscriber.

### Added — CLI

- `murmur run script.py`, `murmur validate specs/`,
  `murmur worker start`, `murmur serve`.

### Added — packaging

- Workspace layout: `murmur-ai` (this) + `murmur-client` (separate wheel).
- Optional extras: `[kafka]`, `[nats]`, `[rabbitmq]`, `[redis]`, `[all]`,
  `[server]`, `[sqlite]`, `[rocksdb]`, `[redis-runstore]`, `[container]`,
  `[docs]`.

### Added — docs

- `docs/` site built with `mkdocs-material` — 17 content pages (index,
  getting-started, concepts, guides, changelog, contributing) and 15 API
  reference pages with full mkdocstrings coverage.
- GitHub Pages workflow at `.github/workflows/docs.yml` using
  `actions/upload-pages-artifact@v3` + `actions/deploy-pages@v4`.

[Unreleased]: https://github.com/murmur-ai/murmur/commits/main
