"""Provider classes — re-exported from PydanticAI under the public API.

A :class:`Provider` handles authentication and the HTTP client used to talk to
an LLM vendor. For most agents, the default Provider attached by PydanticAI's
string-form model resolution (``"openai:gpt-5.2"`` etc.) is enough — credentials
flow from the standard env vars (``OPENAI_API_KEY``, ``ANTHROPIC_API_KEY``,
``OPENROUTER_API_KEY``, …).

Use a Provider explicitly when you need:

- An alternative endpoint — Azure OpenAI, Bedrock-hosted Anthropic, Vertex
  Gemini, a private gateway, or an Ollama instance on another host.
- Custom auth — Azure AD, IAM-signed requests, bearer tokens.
- A custom HTTP client — shared connection pool, custom timeouts, mTLS,
  outbound proxy.
- A non-default base URL behind one of the OpenAI-compatible vendors —
  LiteLLM, Together, Fireworks, etc. (Use :class:`OpenAIProvider` with a
  ``base_url=``, or :class:`LiteLLMProvider` for the LiteLLM proxy.)

>>> from murmur.models import OpenRouterModel
>>> from murmur.providers import OpenRouterProvider
>>> Agent(
...     name="researcher",
...     model=OpenRouterModel(
...         "anthropic/claude-sonnet-4-5",
...         provider=OpenRouterProvider(api_key="sk-or-..."),
...     ),
...     ...,
... )

Re-exporting these here keeps the Public API Rule (CLAUDE.md §2) intact —
user code never imports from ``pydantic_ai`` directly. PydanticAI ships
additional thin OpenAI-compatible Provider shims (Together, Fireworks,
DeepSeek, Vercel, Heroku, etc.); for those, use :class:`OpenAIProvider` with
a custom ``base_url``, or :class:`LiteLLMProvider` for any vendor LiteLLM
already routes.
"""

from __future__ import annotations

from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.providers.azure import AzureProvider
from pydantic_ai.providers.bedrock import BedrockProvider
from pydantic_ai.providers.cerebras import CerebrasProvider
from pydantic_ai.providers.cohere import CohereProvider
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.groq import GroqProvider
from pydantic_ai.providers.huggingface import HuggingFaceProvider
from pydantic_ai.providers.litellm import LiteLLMProvider
from pydantic_ai.providers.mistral import MistralProvider
from pydantic_ai.providers.ollama import OllamaProvider
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.providers.openrouter import OpenRouterProvider
from pydantic_ai.providers.xai import XaiProvider

__all__ = [
    "AnthropicProvider",
    "AzureProvider",
    "BedrockProvider",
    "CerebrasProvider",
    "CohereProvider",
    "GoogleProvider",
    "GroqProvider",
    "HuggingFaceProvider",
    "LiteLLMProvider",
    "MistralProvider",
    "OllamaProvider",
    "OpenAIProvider",
    "OpenRouterProvider",
    "XaiProvider",
]
