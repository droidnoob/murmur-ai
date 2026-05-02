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
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# ``AbstractBuiltinTool`` lives in :mod:`pydantic_ai.builtin_tools` and is used
# as the runtime type for the ``builtin_tools`` field — Pydantic v2 needs the
# class object resolvable at field-validation time, so this is a real (not
# TYPE_CHECKING-guarded) import. The Public API Rule (CLAUDE.md §2) bars
# *user-facing* PydanticAI imports; internal modules like ``agent.py`` are free
# to depend on PydanticAI directly. The concrete tool classes (WebSearchTool,
# etc.) are re-exported under :mod:`murmur.tools` so users still don't need to
# import from PydanticAI to populate this field.
from pydantic_ai.builtin_tools import AbstractBuiltinTool

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
    model: str
    instructions: str
    output_type: type[BaseModel]
    input_type: type[BaseModel] | None = None
    """Optional structured input type. ``None`` = the agent takes a plain string."""

    tools: frozenset[str] = Field(default_factory=frozenset)

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
    context_passer: ContextPasser = Field(default_factory=NullContextPasser)
    backend: str = "auto"

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

    def with_(self, **updates: object) -> Agent:
        """Return a copy with the given fields replaced — the only mutation path."""
        return self.model_copy(update=updates)


__all__ = ["Agent", "ProcessHook"]
