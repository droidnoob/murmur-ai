# Models

Re-exports from PydanticAI under `murmur.models` so user code never imports
`pydantic_ai` directly (Public API Rule). Two unrelated families live here:
**Model classes** (one per LLM vendor — pass to `Agent.model` for non-default
Provider / endpoint / HTTP client configuration) and **concurrency
primitives** (cap provider-side HTTP request concurrency).

## Model classes

| Class | Vendor |
|---|---|
| `AnthropicModel` | Anthropic |
| `BedrockConverseModel` | AWS Bedrock |
| `CerebrasModel` | Cerebras |
| `CohereModel` | Cohere |
| `GoogleModel` | Gemini (Google AI Studio / Vertex) |
| `GroqModel` | Groq |
| `HuggingFaceModel` | HuggingFace Inference Providers |
| `MistralModel` | Mistral |
| `OllamaModel` | Ollama |
| `OpenAIChatModel` | OpenAI Chat Completions API (and OpenAI-compatible endpoints) |
| `OpenAIResponsesModel` | OpenAI Responses API |
| `OpenRouterModel` | OpenRouter |
| `XaiModel` | xAI (Grok) |
| `FallbackModel` | Wraps several models for automatic failover (Murmur builds this for you when you set `Agent.fallback_models=`) |
| `Model` | Abstract base class — useful for type-annotating user code that holds a model of any kind |

Pair a Model with the matching [Provider](providers.md) when you need
non-default authentication, an alternative endpoint, or a custom HTTP
client. For the common case, prefer `Agent(model="vendor:model_name")` —
PydanticAI auto-resolves it. See the [Models & providers concept
guide](../concepts/models-and-providers.md) and the upstream [PydanticAI
per-vendor docs](https://ai.pydantic.dev/models/overview/) for each Model's
constructor signature and supported model IDs.

`OutlinesModel` is intentionally not re-exported because it requires the
optional `outlines` extra. If you need it, install the extra and import
directly from `pydantic_ai.models.outlines`.

## Concurrency primitives

Used to cap provider-side HTTP request concurrency:

```python
from murmur.models import (
    AbstractConcurrencyLimiter,
    ConcurrencyLimit,
    ConcurrencyLimiter,
)
```

See the [Agents concept guide](../concepts/agents.md#capping-provider-http-concurrency)
for usage. The two `Agent` knobs that consume these are:

- `Agent.max_concurrent_requests: int | None` — convenience int knob,
  builds a fresh limiter per agent.
- `Agent.model_concurrency_limiter: AbstractConcurrencyLimiter | None`
  — pre-built limiter shared across agents.

The two are mutually exclusive. The runtime wraps the resolved model in
`pydantic_ai.models.concurrency.ConcurrencyLimitedModel` *outside* any
`FallbackModel`, so one slot covers the whole run regardless of which
fallback served the request.

## Classes

| Class | Purpose |
|---|---|
| `ConcurrencyLimiter` | The default in-process limiter. Constructor: `ConcurrencyLimiter(max_running, *, max_queued=None, name=None, tracer=None)`. Tracks waiting count + emits OpenTelemetry spans while awaiting a slot. |
| `ConcurrencyLimit` | Frozen dataclass: `ConcurrencyLimit(max_running, max_queued=None)`. Pass to `ConcurrencyLimiter.from_limit(...)` for backpressure (`ConcurrencyLimitExceeded` when queue depth exceeds `max_queued`). |
| `AbstractConcurrencyLimiter` | Base class for custom limiters. Subclass + implement `acquire(source)` / `release()` for cross-process backends (e.g. Redis-backed for fleet-wide RPM caps). |

```python
from murmur import Agent
from murmur.models import ConcurrencyLimit, ConcurrencyLimiter

# Per-agent cap (one limiter per agent):
solo = Agent(name="solo", model="openai:gpt-5.2", max_concurrent_requests=5, …)

# Shared cap across agents:
pool = ConcurrencyLimiter(max_running=10, name="openai-pool")
head   = Agent(name="head",   model="openai:gpt-5.2", model_concurrency_limiter=pool, …)
minion = Agent(name="minion", model="openai:gpt-5.2", model_concurrency_limiter=pool, …)

# Backpressure (raises ConcurrencyLimitExceeded when queue exceeds max_queued):
bp = ConcurrencyLimiter.from_limit(
    ConcurrencyLimit(max_running=5, max_queued=20),
    name="openai-bp",
)
strict = Agent(name="strict", model="openai:gpt-5.2", model_concurrency_limiter=bp, …)
```

For the full primitive reference, see PydanticAI's
[concurrency module docs](https://ai.pydantic.dev/concurrency/).
