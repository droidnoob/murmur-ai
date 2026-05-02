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
