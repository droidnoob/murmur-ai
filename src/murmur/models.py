"""Model classes and concurrency primitives — re-exported from PydanticAI.

User code passes one of these to :attr:`murmur.Agent.model` (instance form) or
to :attr:`murmur.Agent.model_concurrency_limiter` (concurrency form). All
re-exported here so the Public API Rule (CLAUDE.md §2) holds: user code only
ever imports from ``murmur``.

Two things live in this module:

**Concurrency primitives** — used to cap provider-side HTTP request
concurrency:

>>> from murmur.models import ConcurrencyLimiter
>>> pool = ConcurrencyLimiter(max_running=10, name="openai-pool")

The convenience int knob (``Agent(max_concurrent_requests=5)``) constructs a
limiter implicitly per agent. Use a shared :class:`ConcurrencyLimiter` when
several agents share one provider key and need to share one cap.
:class:`ConcurrencyLimit` carries an optional ``max_queued`` for backpressure;
:class:`AbstractConcurrencyLimiter` is the base class for custom limiters
(e.g. a Redis-backed cross-process limiter).

**Model classes** — used when the simple ``"<vendor>:<model>"`` string isn't
enough (custom Provider, custom HTTP client, custom auth, alternative
endpoint, etc.):

>>> from murmur.models import OpenAIChatModel
>>> from murmur.providers import OpenAIProvider
>>> agent = Agent(
...     name="r",
...     model=OpenAIChatModel(
...         "gpt-5.2",
...         provider=OpenAIProvider(base_url="https://api.example.com/v1"),
...     ),
...     ...,
... )

For the common case, prefer the string form — PydanticAI auto-resolves it to
the matching Model + default Provider:

>>> Agent(name="r", model="anthropic:claude-sonnet-4-6", ...)

:class:`Model` is the abstract base class — useful for type-annotating user
code that holds a model of any kind. ``OutlinesModel`` is intentionally not
re-exported because it requires the optional ``outlines`` extra; users who
need it should import directly from ``pydantic_ai.models.outlines``.
"""

from __future__ import annotations

from pydantic_ai.concurrency import (
    AbstractConcurrencyLimiter,
    ConcurrencyLimit,
    ConcurrencyLimiter,
)
from pydantic_ai.models import Model
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.models.cerebras import CerebrasModel
from pydantic_ai.models.cohere import CohereModel
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.groq import GroqModel
from pydantic_ai.models.huggingface import HuggingFaceModel
from pydantic_ai.models.mistral import MistralModel
from pydantic_ai.models.ollama import OllamaModel
from pydantic_ai.models.openai import (
    OpenAIChatModel,
    OpenAIResponsesModel,
)
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.models.xai import XaiModel

__all__ = [
    "AbstractConcurrencyLimiter",
    "AnthropicModel",
    "BedrockConverseModel",
    "CerebrasModel",
    "CohereModel",
    "ConcurrencyLimit",
    "ConcurrencyLimiter",
    "FallbackModel",
    "GoogleModel",
    "GroqModel",
    "HuggingFaceModel",
    "MistralModel",
    "Model",
    "OllamaModel",
    "OpenAIChatModel",
    "OpenAIResponsesModel",
    "OpenRouterModel",
    "XaiModel",
]
