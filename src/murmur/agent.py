"""``murmur.Agent`` — the single unified agent type.

An :class:`Agent` combines LLM-side configuration (model, instructions,
output_type, tools) with Murmur orchestration configuration (trust level,
context-passer policy, backend hint). Internally it drives PydanticAI; users
never see PydanticAI types.

The ``Agent`` is a frozen Pydantic value object — pure data. Dispatch lives
on :class:`murmur.AgentRuntime` so the agent stays broker-safe and trivially
serializable. Tools are registered on the runtime's :class:`ToolRegistry`,
not on the agent.

Pre/post hooks are same-type, sync, pure transformations. They run
*inside* the agent's run boundary — pre-hooks before the LLM call,
post-hooks after. ``ty`` enforces the signature alignment when the user
declares the hooks against a typed ``input_type`` / ``output_type``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ``AbstractBuiltinTool`` lives in :mod:`pydantic_ai.builtin_tools` and is used
# as the runtime type for the ``builtin_tools`` field — Pydantic v2 needs the
# class object resolvable at field-validation time, so this is a real (not
# TYPE_CHECKING-guarded) import. The Public API Rule (CLAUDE.md §2) bars
# *user-facing* PydanticAI imports; internal modules like ``agent.py`` are free
# to depend on PydanticAI directly. The concrete tool classes (WebSearchTool,
# etc.) are re-exported under :mod:`murmur.tools` so users still don't need to
# import from PydanticAI to populate this field.
from pydantic_ai.builtin_tools import AbstractBuiltinTool
from pydantic_ai.concurrency import AbstractConcurrencyLimiter
from pydantic_ai.models import Model

from murmur.context.null import NullContextPasser
from murmur.core.protocols.context import ContextPasser
from murmur.core.protocols.toolsets import ToolsetProvider
from murmur.types import TrustLevel

ProcessHook = Callable[..., Any]
"""Pre/post-process hook callable.

The user's hook is typed against the agent's ``input_type`` /
``output_type`` (``(T) -> T``); ``Agent`` itself is generic-erased on
``T`` so the field type stays ``Callable[..., Any]``. ``ty`` checks
the alignment at the call site where the typed hook is constructed.
"""


class Agent(BaseModel):
    """A Murmur agent — frozen, broker-safe, serializable.

    The ``model``, ``instructions``, ``output_type``, and ``tools`` fields
    drive PydanticAI internally. The ``trust_level``, ``context_passer``, and
    ``backend`` fields drive Murmur orchestration. Users compose them on a
    single object; the runtime splits them apart at dispatch time.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    """Stable identifier used as the registry key, broker topic suffix, and
    ``agent_name`` field on every log line and ``RuntimeEvent``."""

    model: str | Model
    """The agent's model — either a PydanticAI string identifier or a
    constructed :class:`pydantic_ai.models.Model` instance.

    String form: ``"<provider>:<model_name>"`` — e.g.
    ``"anthropic:claude-sonnet-4-6"``, ``"openai:gpt-5.2"``,
    ``"openrouter:anthropic/claude-sonnet-4-5"``. PydanticAI auto-resolves
    the provider, applies the default authentication (env var per vendor),
    and constructs the matching :class:`Model` internally. Use this form
    for the common case.

    Instance form: a constructed Model — used when you need a non-default
    Provider (Azure OpenAI, Bedrock-hosted Anthropic, Vertex Gemini), a
    custom HTTP client, custom auth, or a private base URL:

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

    The full Model and Provider matrix is re-exported from
    :mod:`murmur.models` and :mod:`murmur.providers` so user code never
    has to import from :mod:`pydantic_ai` directly.

    Forwarded to PydanticAI verbatim at dispatch; Murmur does not maintain
    its own model registry."""

    fallback_models: tuple[str, ...] = ()
    """Ordered fallback model names. ``()`` (default) means no fallbacks.

    When non-empty, the runtime builds
    :class:`pydantic_ai.models.fallback.FallbackModel(model, *fallback_models)`
    at dispatch and uses it instead of ``model`` directly. The default
    fallback trigger is :class:`pydantic_ai.ModelAPIError` (4xx / 5xx) — the
    common "provider down / rate limited" case. Each entry is a
    PydanticAI-style model string (``"openai:gpt-5.2"``,
    ``"google:gemini-3-pro-preview"``, etc.); per-fallback ``ModelSettings``
    and ``Provider`` overrides are deferred (single ``model_settings`` is
    shared across primary + all fallbacks for now).

    >>> Agent(
    ...     name="r",
    ...     model="anthropic:claude-sonnet-4-6",
    ...     fallback_models=("openai:gpt-5.2",),
    ...     ...,
    ... )

    Caveats:

    - PydanticAI provider SDKs may have built-in retry logic that delays
      fallback activation. Set ``max_retries=0`` on a custom client if you
      need immediate fallback.
    - All-models-failed raises :class:`pydantic_ai.FallbackExceptionGroup`
      (an :class:`ExceptionGroup` subclass). User code that catches
      :class:`pydantic_ai.ModelAPIError` needs ``except*`` on Python 3.11+
      to catch through the group; Murmur's :class:`SpawnError` translation
      at the dispatch boundary unwraps and stringifies whichever exception
      surfaces first, so most callers don't see the group.
    - Validation errors (structured-output retries) do **not** trigger
      fallback — they use PydanticAI's per-model retry mechanism.
    """
    instructions: str
    """System prompt forwarded to PydanticAI as the agent's ``system_prompt``.
    Plain string — variable interpolation happens upstream of construction
    (e.g. before the YAML loader resolves the spec)."""

    output_type: type[BaseModel]
    """Pydantic model class the agent's output is validated against.
    PydanticAI re-prompts on validation failure up to its built-in retry
    budget; the runtime surfaces a final failure as
    :class:`SpawnError`."""

    input_type: type[BaseModel] | None = None
    """Optional structured input type. ``None`` = the agent takes a plain string."""

    tools: frozenset[str] = Field(default_factory=frozenset)
    """Native tool names registered in the runtime's :class:`ToolRegistry`.
    Each call flows through :class:`ToolExecutor` for trust gating, allow-list
    filtering, and ``TOOL_CALL_*`` lifecycle events. Frozen — the agent's tool
    set is fixed at construction; use :meth:`with_` to derive a variant."""

    mcp_servers: tuple[ToolsetProvider, ...] = ()
    """Remote toolset providers — tools discovered at dispatch time.

    Each provider's tools are exposed to the agent alongside its native
    ``tools=…`` set. Calls flow through the same :class:`ToolExecutor`
    gate, so trust gating and lifecycle events apply identically. Build
    via :func:`murmur.tools.mcp_stdio` / :func:`murmur.tools.mcp_http` /
    :func:`murmur.tools.mcp_sse`.

    The runtime owns provider lifecycle — it calls ``start()`` lazily on
    first dispatch and ``stop()`` on shutdown.
    """

    builtin_tools: tuple[AbstractBuiltinTool, ...] = ()
    """Provider-side built-in tools — executed by the LLM provider, not Murmur.

    Examples: :class:`pydantic_ai.WebSearchTool`,
    :class:`pydantic_ai.CodeExecutionTool`,
    :class:`pydantic_ai.ImageGenerationTool`,
    :class:`pydantic_ai.WebFetchTool`,
    :class:`pydantic_ai.FileSearchTool`. Pass instances (with their
    own configuration knobs — ``max_uses``, ``allowed_domains``, etc.)
    in this tuple and they're forwarded to
    ``pydantic_ai.Agent(builtin_tools=...)`` at dispatch.

    For ergonomics, the concrete classes are also re-exported from
    :mod:`murmur.tools` so users can avoid importing PydanticAI directly:

    >>> from murmur.tools import WebSearchTool
    >>> Agent(name="r", model="anthropic:claude-sonnet-4-6",
    ...       builtin_tools=(WebSearchTool(max_uses=5),), ...)

    **CAVEAT** — these run on the provider's infrastructure, so they
    bypass Murmur's :class:`ToolExecutor`: no trust gate, no allow-list
    filtering, no per-tool ``TOOL_CALL_*`` lifecycle events (PydanticAI
    surfaces them post-hoc via ``ModelResponse.builtin_tool_calls``).
    Token cost still flows through ``CostTrackingMiddleware`` because
    PydanticAI's ``usage()`` includes provider-side tool tokens. Provider
    support varies by tool — an unsupported combo raises ``UserError`` at
    run time, which surfaces as :class:`SpawnError`.
    """

    max_concurrent_requests: int | None = None
    """Convenience cap on concurrent HTTP requests *to this model*.

    When set to a positive integer, the runtime wraps the resolved model in
    :class:`pydantic_ai.models.concurrency.ConcurrencyLimitedModel` with a
    fresh per-agent :class:`murmur.models.ConcurrencyLimiter`. Distinct from
    :meth:`AgentRuntime.gather`'s ``max_concurrency`` (which caps Murmur's
    task fan-out): this caps the **provider-side** request count, useful when
    a shared API key is rate-limited.

    >>> Agent(name="r", model="openai:gpt-5.2", max_concurrent_requests=5, ...)

    Mutually exclusive with :attr:`model_concurrency_limiter`. Use the limiter
    field instead when several agents need to share one cap (e.g. one Anthropic
    key behind a fleet of agents).
    """

    model_concurrency_limiter: AbstractConcurrencyLimiter | None = None
    """Pre-built concurrency limiter shared across agents — wraps the resolved
    model with the same limiter instance every dispatch.

    Build via :class:`murmur.models.ConcurrencyLimiter` (or any
    :class:`murmur.models.AbstractConcurrencyLimiter` subclass for custom
    backends — e.g. a Redis-backed cross-process limiter):

    >>> from murmur.models import ConcurrencyLimiter
    >>> pool = ConcurrencyLimiter(max_running=10, name="openai-pool")
    >>> head = Agent(name="head", model="openai:gpt-5.2",
    ...              model_concurrency_limiter=pool, ...)
    >>> minion = Agent(name="minion", model="openai:gpt-5.2",
    ...                model_concurrency_limiter=pool, ...)

    Mutually exclusive with :attr:`max_concurrent_requests`. Limiting is
    **single-process** by default — pass a custom ``AbstractConcurrencyLimiter``
    subclass (e.g. Redis-backed) for cross-process limiting across a worker
    fleet.
    """

    model_settings: Mapping[str, object] | None = None
    """Per-provider knobs forwarded to the underlying model — temperature,
    max_tokens, top_p, etc.

    The map is passed through to ``pydantic_ai.Agent(model_settings=...)``
    verbatim. Recognised keys are provider-specific (PydanticAI validates
    per-provider at request time); a typo is a silent no-op rather than
    a Murmur error. Common keys:

    - ``temperature: float``
    - ``max_tokens: int``
    - ``top_p: float``
    - ``stop_sequences: list[str]``
    - ``timeout: float`` — per-request, distinct from
      ``RuntimeOptions.timeout_seconds`` which gates the whole agent run

    ``None`` (default) means PydanticAI picks per-provider defaults.
    """

    trust_level: TrustLevel = TrustLevel.MEDIUM
    """Tool-access policy. Drives :class:`ToolExecutor`'s gate (allow-list for
    LOW, full set for MEDIUM/HIGH, no tools for SANDBOX) and — once Phase 4
    lands — backend selection (SANDBOX agents always run via
    ``ContainerBackend``)."""

    context_passer: ContextPasser = Field(default_factory=NullContextPasser)
    """Policy deciding what conversation history flows into a spawn.
    :class:`NullContextPasser` (default) hands the agent a fresh context;
    :class:`FullContextPasser` forwards everything. Phase 3 adds
    ``SummaryContextPasser`` and ``SelectiveContextPasser``."""

    backend: str = "auto"
    """Routing hint for :class:`AgentRuntime` to pick a :class:`Backend`.
    ``"auto"`` (default) defers to the runtime's configured backend
    (typically AsyncBackend in local mode, JobBackend when a broker URL was
    supplied). Reserved for future overrides — currently informational."""

    pre_process: tuple[ProcessHook, ...] = ()
    """Hooks applied left-to-right to the input before the LLM call.

    Each hook is ``(input_type) -> input_type``. Sync, pure — no I/O, no async.
    Empty tuple = identity.
    """

    post_process: tuple[ProcessHook, ...] = ()
    """Hooks applied left-to-right to the output after the LLM call.

    Each hook is ``(output_type) -> output_type``. Sync, pure — no I/O, no
    async. Empty tuple = identity.
    """

    @model_validator(mode="after")
    def _validate_concurrency_limit(self) -> Self:
        if (
            self.max_concurrent_requests is not None
            and self.model_concurrency_limiter is not None
        ):
            raise ValueError(
                "max_concurrent_requests and model_concurrency_limiter are "
                "mutually exclusive — pick the int knob for a per-agent cap, "
                "or the limiter for a shared cap across agents."
            )
        if (
            self.max_concurrent_requests is not None
            and self.max_concurrent_requests < 1
        ):
            raise ValueError("max_concurrent_requests must be a positive integer.")
        return self

    def with_(self, **updates: object) -> Agent:
        """Return a copy with the given fields replaced — the only mutation path."""
        return self.model_copy(update=updates)


__all__ = ["Agent", "ProcessHook"]
