# API reference

The complete public surface of `murmur` and the companion `murmur-client`
package, auto-generated from docstrings.

> Per the [public API rule](../concepts/architecture.md#public-api-rule),
> users only import from `murmur` and `murmur_client`. PydanticAI and
> FastStream are dependencies, not public API — the only place you'll
> see them surface is `murmur.interop`.

## Packages

| Page | Symbols |
|---|---|
| [Agents & types](agent.md) | `Agent`, `AgentTemplate`, `TaskSpec`, `AgentResult`, `AgentHandle`, `AgentContext`, `ResultMetadata`, `TrustLevel` |
| [Models](models.md) | `Model`, `AnthropicModel`, `BedrockConverseModel`, `CerebrasModel`, `CohereModel`, `FallbackModel`, `GoogleModel`, `GroqModel`, `HuggingFaceModel`, `MistralModel`, `OllamaModel`, `OpenAIChatModel`, `OpenAIResponsesModel`, `OpenRouterModel`, `XaiModel`, `ConcurrencyLimiter`, `ConcurrencyLimit`, `AbstractConcurrencyLimiter` |
| [Providers](providers.md) | `AnthropicProvider`, `AzureProvider`, `BedrockProvider`, `CerebrasProvider`, `CohereProvider`, `GoogleProvider`, `GroqProvider`, `HuggingFaceProvider`, `LiteLLMProvider`, `MistralProvider`, `OllamaProvider`, `OpenAIProvider`, `OpenRouterProvider`, `XaiProvider` |
| [Runtime](runtime.md) | `AgentRuntime`, `RuntimeOptions` |
| [Groups](groups.md) | `AgentGroup`, `Edge`, `EdgeMapper`, `FanOut`, `get_fan_out_field` |
| [Events](events.md) | `RuntimeEvent`, `EventType`, `LogEventEmitter`, `SSEEventEmitter`, `MultiEventEmitter`, `BrokerEventBridge` |
| [Middleware](middleware.md) | `RetryMiddleware`, `TimeoutMiddleware`, `DepthLimitMiddleware`, `CostTrackingMiddleware`, `TokenBudget` |
| [Tools](tools.md) | `ToolRegistry`, `StaticToolProvider`, `ToolExecutor`, `ToolFunc`, `mcp_stdio` / `mcp_http` / `mcp_sse`, built-in tools |
| [Context passers](context.md) | `FullContextPasser`, `NullContextPasser` |
| [Server](server.md) | `AgentServer`, `AgentRouter`, `ErrorResponse` |
| [MCP server](mcp_server.md) | `AgentServer.register_mcp`, `AgentServer.serve_mcp`, `MCPEnrollment` |
| [Worker](worker.md) | `Worker` |
| [Runs](runs.md) | `RunStore`, `InMemoryRunStore`, `SQLiteRunStore`, `RocksDBRunStore`, `RedisRunStore`, value types |
| [Interop](interop.md) | `from_pydantic_ai`, `as_faststream_handler` |
| [Protocols](protocols.md) | `Backend`, `ContextPasser`, `ToolProvider`, `ToolsetProvider`, `EventEmitter`, `Router`, `Registry`, `Pipeline`, `Stage`, `Middleware`, `Broker`, `Worker` |
| [Errors](errors.md) | `MurmurError` hierarchy |
| [Client](client.md) | `MurmurClient`, `LocalClient`, `Run` |

## Conventions

- **Frozen value objects.** Spec types (`Agent`, `TaskSpec`, `Edge`,
  `RuntimeEvent`, etc.) are frozen Pydantic models. Update via
  `model_copy(update={...})`.
- **Async by default.** Every I/O method is `async`. Sync wrappers
  (`run_sync`, `gather_sync`, `run_group_sync`) are convenience
  shortcuts that call `asyncio.run` internally — they raise if invoked
  from inside a running event loop.
- **Domain errors.** Every failure mode wraps in a `MurmurError`
  subclass. See [Errors](errors.md).
- **Protocols, not ABCs.** Pluggable components (`Backend`,
  `ContextPasser`, `ToolProvider`, `EventEmitter`, `RunStore`,
  `Worker`) are `typing.Protocol`. Concretes match by shape, no
  inheritance required.
