"""``murmur.AgentTemplate`` — a frozen builder for shared agent config.

A template carries optional defaults for the fields a fleet of agents
typically share — ``pre_instruction``, ``model``, ``trust_level``,
``context_passer``, ``tools``, ``mcp_servers``, etc. — and produces
concrete frozen :class:`murmur.Agent` instances via ``.agent(...)``.
Per-call kwargs override template defaults; ``pre_instruction``
concatenates with the per-agent ``instructions``:

>>> swarm = AgentTemplate(
...     pre_instruction="You are part of an automated pipeline. JSON only.",
...     model="anthropic:claude-sonnet-4-6",
...     trust_level=TrustLevel.MEDIUM,
... )
>>> researcher = swarm.agent(
...     name="researcher",
...     instructions="Find verifiable facts about the topic.",
...     output_type=Findings,
... )
>>> # researcher.instructions == "You are ... JSON only.\n\nFind verifiable facts ..."

The template is pure data — no dispatch impact, no runtime mutation. It
complements LLM-driven fan-out (``spawn_agents``) by bounding what an
LLM can spawn: trust level, model, tools come from the template, not
from the LLM's call.

Override semantics:

- ``None`` per-call kwarg → use template's value (or the underlying
  :class:`Agent` default if the template hasn't set it either).
- non-``None`` per-call kwarg → wins.
- collection fields (``tools``, ``mcp_servers``, ``builtin_tools``,
  ``fallback_models``) **replace**, they don't extend. If you want a
  union, build it explicitly: ``tools=template.tools | extra``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, model_validator
from pydantic_ai.builtin_tools import AbstractBuiltinTool
from pydantic_ai.concurrency import AbstractConcurrencyLimiter

from murmur.agent import Agent, ProcessHook
from murmur.core.protocols.context import ContextPasser
from murmur.core.protocols.toolsets import ToolsetProvider
from murmur.types import TrustLevel


class AgentTemplate(BaseModel):
    """Frozen builder for shared :class:`murmur.Agent` config.

    Materialize concrete agents via :meth:`agent`. The template itself
    is broker-safe — it serialises through Pydantic with no callables
    on its surface.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    pre_instruction: str | None = None
    """Prepended to every materialised agent's ``instructions`` with a
    blank line between. ``None`` (default) means no prefix.

    Useful for fleet-wide preamble: ``"You are part of the Murmur
    swarm. Always return JSON. Never apologize."``
    """

    model: str | None = None
    """Default model string for materialised agents. ``None`` (default)
    means the per-call kwarg must supply ``model=``."""

    fallback_models: tuple[str, ...] | None = None
    """Default fallback chain. See :attr:`Agent.fallback_models`."""

    input_type: type[BaseModel] | None = None
    """Default ``input_type``. See :attr:`Agent.input_type`."""

    tools: frozenset[str] | None = None
    """Default native tool set. Per-call ``tools=`` **replaces** this —
    it doesn't extend. Build a union explicitly when you want both."""

    mcp_servers: tuple[ToolsetProvider, ...] | None = None
    """Default MCP toolset providers. Per-call ``mcp_servers=`` replaces."""

    builtin_tools: tuple[AbstractBuiltinTool, ...] | None = None
    """Default provider-side built-in tools. Per-call replaces."""

    max_concurrent_requests: int | None = None
    """Default per-agent provider HTTP concurrency cap. Mutually
    exclusive with :attr:`model_concurrency_limiter` on the template."""

    model_concurrency_limiter: AbstractConcurrencyLimiter | None = None
    """Default shared concurrency limiter. Mutually exclusive with
    :attr:`max_concurrent_requests` on the template."""

    model_settings: Mapping[str, object] | None = None
    """Default provider model_settings. See :attr:`Agent.model_settings`."""

    trust_level: TrustLevel | None = None
    """Default trust level. ``None`` means materialised agents fall back
    to :attr:`Agent.trust_level`'s default (``MEDIUM``)."""

    context_passer: ContextPasser | None = None
    """Default :class:`ContextPasser`. ``None`` means materialised agents
    fall back to :class:`NullContextPasser` (Agent's default)."""

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

    def agent(
        self,
        *,
        name: str,
        instructions: str,
        output_type: type[BaseModel],
        model: str | None = None,
        fallback_models: tuple[str, ...] | None = None,
        input_type: type[BaseModel] | None = None,
        tools: frozenset[str] | None = None,
        mcp_servers: tuple[ToolsetProvider, ...] | None = None,
        builtin_tools: tuple[AbstractBuiltinTool, ...] | None = None,
        max_concurrent_requests: int | None = None,
        model_concurrency_limiter: AbstractConcurrencyLimiter | None = None,
        model_settings: Mapping[str, object] | None = None,
        trust_level: TrustLevel | None = None,
        context_passer: ContextPasser | None = None,
        pre_process: tuple[ProcessHook, ...] = (),
        post_process: tuple[ProcessHook, ...] = (),
        backend: str = "auto",
    ) -> Agent:
        """Materialize a concrete :class:`Agent` from this template.

        ``name``, ``instructions``, and ``output_type`` are always per-agent.
        Every other kwarg is an optional override of the template's
        corresponding field; pass ``None`` (the default) to inherit.

        ``pre_instruction`` (when set on the template) prefixes the
        per-agent ``instructions`` with a blank line between.
        """
        if self.pre_instruction is not None:
            final_instructions = f"{self.pre_instruction}\n\n{instructions}"
        else:
            final_instructions = instructions

        kwargs: dict[str, Any] = {
            "name": name,
            "instructions": final_instructions,
            "output_type": output_type,
            "pre_process": pre_process,
            "post_process": post_process,
            "backend": backend,
        }

        # For each templatable field: per-call override wins; else template
        # value if set; else omit so Agent's own default applies.
        per_field: tuple[tuple[str, object, object], ...] = (
            ("model", model, self.model),
            ("fallback_models", fallback_models, self.fallback_models),
            ("input_type", input_type, self.input_type),
            ("tools", tools, self.tools),
            ("mcp_servers", mcp_servers, self.mcp_servers),
            ("builtin_tools", builtin_tools, self.builtin_tools),
            (
                "max_concurrent_requests",
                max_concurrent_requests,
                self.max_concurrent_requests,
            ),
            (
                "model_concurrency_limiter",
                model_concurrency_limiter,
                self.model_concurrency_limiter,
            ),
            ("model_settings", model_settings, self.model_settings),
            ("trust_level", trust_level, self.trust_level),
            ("context_passer", context_passer, self.context_passer),
        )
        for field_name, call_value, template_value in per_field:
            resolved = call_value if call_value is not None else template_value
            if resolved is not None:
                kwargs[field_name] = resolved

        return Agent(**kwargs)


__all__ = ["AgentTemplate"]
