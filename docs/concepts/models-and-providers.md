# Models & providers

Murmur drives [PydanticAI](https://ai.pydantic.dev/) under the hood for the
LLM call itself. PydanticAI ships a **Model** class per vendor and a
**Provider** class for authentication / endpoint / HTTP-client configuration.
You hand a Model to `Agent.model` — Murmur forwards it verbatim and never
inspects it.

The Public API Rule (CLAUDE.md §2) says **user code never imports from
`pydantic_ai` directly**. So Murmur re-exports the Model and Provider classes
under `murmur.models` and `murmur.providers`.

## Two forms

`Agent.model` accepts either a string identifier or a constructed `Model`.

**String form** — common case. PydanticAI auto-resolves the matching
`Model` and the default `Provider`, picking up credentials from the standard
env vars (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `OPENROUTER_API_KEY`, …).

```python
from murmur import Agent

agent = Agent(
    name="researcher",
    model="anthropic:claude-sonnet-4-6",
    instructions="…",
    output_type=Finding,
)
```

**Instance form** — used when you need a non-default Provider (Azure
OpenAI, Bedrock-hosted Anthropic, Vertex Gemini, OpenRouter), a custom HTTP
client, custom auth, or a private base URL. The Provider rides inside the
Model:

```python
from murmur import Agent
from murmur.models import OpenRouterModel
from murmur.providers import OpenRouterProvider

agent = Agent(
    name="researcher",
    model=OpenRouterModel(
        "anthropic/claude-sonnet-4-5",
        provider=OpenRouterProvider(api_key="sk-or-..."),
    ),
    instructions="…",
    output_type=Finding,
)
```

There's no separate `Agent(provider=...)` field by design — that mirrors
PydanticAI's API and avoids inventing a parallel surface.

## Available re-exports

`murmur.models` covers Anthropic, Bedrock, Cerebras, Cohere, Google, Groq,
HuggingFace, Mistral, Ollama, OpenAI (chat & responses), OpenRouter, and
xAI. `murmur.providers` covers the same vendors plus Azure, Google Vertex,
and LiteLLM.

For OpenAI-compatible vendors that PydanticAI doesn't ship a dedicated
Provider for (Together, Fireworks, DeepSeek, Vercel, Heroku, etc.), use
`OpenAIProvider(base_url=...)` or `LiteLLMProvider`.

## Per-vendor details

Per-vendor configuration (env-var names, supported model IDs, Provider
constructor knobs) is upstream PydanticAI documentation:

- [Models & providers overview](https://ai.pydantic.dev/models/overview/)
- [Anthropic](https://ai.pydantic.dev/models/anthropic/) ·
  [OpenAI](https://ai.pydantic.dev/models/openai/) ·
  [OpenRouter](https://ai.pydantic.dev/models/openrouter/) ·
  [Google](https://ai.pydantic.dev/models/google/) ·
  [Bedrock](https://ai.pydantic.dev/models/bedrock/)
- Or any of the vendor-specific pages on the PydanticAI docs site.

The class names there match the names re-exported under `murmur.models`
and `murmur.providers` one-for-one — when the upstream docs say
`OpenRouterModel`, that's `from murmur.models import OpenRouterModel`.
