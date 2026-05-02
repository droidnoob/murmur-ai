# Changelog

The full changelog will live at the project root in `CHANGELOG.md`
following the [Keep a Changelog][keep] format. Tracking issue:
[`murmur-ai-r6z`][r6z].

[keep]: https://keepachangelog.com/en/1.1.0/
[r6z]: https://github.com/murmur-ai/murmur/issues

Until the file lands, see [GitHub Releases][releases] for the running
notes.

[releases]: https://github.com/murmur-ai/murmur/releases

## What's shipped so far

- **Phase 1** — public API surface: `Agent`, `AgentRuntime`, `TaskSpec`,
  `AgentResult`, `TrustLevel`. ThreadBackend + JobBackend. Static tools.
  YAML registry. `Worker`. CLI: `murmur run`, `murmur validate`,
  `murmur worker start`.
- **Phase 1.5** — multi-input aggregation in `run_group`, conditional
  edges, sync entry points, `Worker --all-from`, `/healthz`/`/readyz`
  split, MCP consume side, persistent `RunStore` × 3 (SQLite, RocksDB,
  Redis).
- **Phase 1.6** — MCP tool prefixing, MCP eager-start lifecycle (opt-in),
  FastStream/aiokafka log silencing, typed `ToolFunc[T]`,
  `Agent.builtin_tools`, `Agent.fallback_models`.
- **Phase 2** — observability events (`LogEventEmitter`,
  `SSEEventEmitter`, `MultiEventEmitter`, `BrokerEventBridge`), cost
  tracking (`TokenBudget` + `CostTrackingMiddleware`), distributed event
  bridge with `publish_events=`, standalone `murmur serve` with
  `GET /events/stream`, `EventEmitterContract` shared suite.

## What's coming

- **Phase 3** — smart context passers (`SummaryContextPasser`,
  `SelectiveContextPasser`), group coordination tools (`SharedMemoryTool`,
  `BarrierTool`, `VotingTool`), tool providers
  (`RoleBasedToolProvider`, `DenylistToolProvider`), YAML workflow engine
  with Jinja templating, untrusted-context sanitisation, `SECURITY.md`.
- **Phase 4** — `ContainerBackend` + Docker SDK, full `TrustLevel`
  matrix enforcement, cascading-spawn parent→child graph,
  `WebSocketEventEmitter`, `FastStreamEventEmitter`, operator guide.
