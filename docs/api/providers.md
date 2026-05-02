# Providers

`Provider` classes — re-exported from PydanticAI under the public API. A
Provider handles authentication, endpoint, and HTTP client for an LLM
vendor. Attach a Provider to a [Model](models.md), then pass that Model to
`Agent.model`. There's no separate `Agent(provider=...)` field by design.

```python
from murmur.models import OpenRouterModel
from murmur.providers import OpenRouterProvider

model = OpenRouterModel(
    "anthropic/claude-sonnet-4-5",
    provider=OpenRouterProvider(api_key="sk-or-..."),
)
```

See the [Models & providers concept guide](../concepts/models-and-providers.md)
for when to use the Provider form vs. the string form, and the upstream
[PydanticAI per-vendor docs](https://ai.pydantic.dev/models/overview/) for
each Provider's constructor knobs (api_key, base_url, http_client, custom
auth, etc.).

## Available providers

| Class | Vendor / use case |
|---|---|
| `AnthropicProvider` | Anthropic API or Anthropic-compatible endpoint |
| `AzureProvider` | Azure OpenAI Service |
| `BedrockProvider` | AWS Bedrock-hosted models |
| `CerebrasProvider` | Cerebras inference |
| `CohereProvider` | Cohere API |
| `GoogleProvider` | Gemini API — covers both Google AI Studio and Vertex AI (set `vertexai=True` on the constructor) |
| `GroqProvider` | Groq API |
| `HuggingFaceProvider` | HuggingFace Inference Providers |
| `LiteLLMProvider` | LiteLLM proxy (use for any vendor LiteLLM routes) |
| `MistralProvider` | Mistral API |
| `OllamaProvider` | Ollama local or remote endpoint |
| `OpenAIProvider` | OpenAI API or OpenAI-compatible endpoint (`base_url=...`) |
| `OpenRouterProvider` | OpenRouter API |
| `XaiProvider` | xAI (Grok) API |

::: murmur.providers
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members_order: alphabetical
