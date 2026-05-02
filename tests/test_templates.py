"""Tests for ``murmur.AgentTemplate`` (zse).

A template is a frozen builder that materialises concrete frozen
:class:`Agent` instances. Verifies field round-trip, override semantics
(per-call wins, ``None`` inherits, collections replace not extend),
``pre_instruction`` concatenation, mutual-exclusivity validation, and
public-API exposure.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from murmur import Agent, AgentTemplate, TrustLevel
from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser
from murmur.models import ConcurrencyLimiter


class _Out(BaseModel):
    text: str


class _AltOut(BaseModel):
    n: int


def _agent(template: AgentTemplate, **overrides: Any) -> Agent:
    """Helper: materialise an agent with sensible defaults."""
    defaults: dict[str, Any] = {
        "name": "child",
        "instructions": "Do the thing.",
        "output_type": _Out,
    }
    defaults.update(overrides)
    return template.agent(**defaults)


# ---------------------------------------------------------------------------
# Field shape — template itself
# ---------------------------------------------------------------------------


def test_empty_template_constructs() -> None:
    t = AgentTemplate()
    assert t.pre_instruction is None
    assert t.model is None
    assert t.trust_level is None


def test_template_is_frozen() -> None:
    t = AgentTemplate(model="anthropic:claude-sonnet-4-6")
    with pytest.raises(ValidationError):
        t.model = "openai:gpt-5.2"


def test_template_roundtrips_all_fields() -> None:
    pool = ConcurrencyLimiter(max_running=3)
    t = AgentTemplate(
        pre_instruction="Always JSON.",
        model="anthropic:claude-sonnet-4-6",
        fallback_models=("openai:gpt-5.2",),
        input_type=_AltOut,
        tools=frozenset({"web_search"}),
        model_concurrency_limiter=pool,
        model_settings={"temperature": 0.0},
        trust_level=TrustLevel.LOW,
        context_passer=FullContextPasser(),
    )
    assert t.pre_instruction == "Always JSON."
    assert t.model == "anthropic:claude-sonnet-4-6"
    assert t.fallback_models == ("openai:gpt-5.2",)
    assert t.input_type is _AltOut
    assert t.tools == frozenset({"web_search"})
    assert t.model_concurrency_limiter is pool
    assert t.trust_level is TrustLevel.LOW
    assert isinstance(t.context_passer, FullContextPasser)


def test_template_concurrency_mutual_exclusivity() -> None:
    pool = ConcurrencyLimiter(max_running=3)
    with pytest.raises(ValidationError, match="mutually exclusive"):
        AgentTemplate(max_concurrent_requests=5, model_concurrency_limiter=pool)


def test_template_zero_max_concurrent_requests_rejected() -> None:
    with pytest.raises(ValidationError, match="positive integer"):
        AgentTemplate(max_concurrent_requests=0)


# ---------------------------------------------------------------------------
# pre_instruction — concatenation rule
# ---------------------------------------------------------------------------


def test_no_pre_instruction_passes_through_verbatim() -> None:
    t = AgentTemplate(model="test")
    a = _agent(t, instructions="Find facts.")
    assert a.instructions == "Find facts."


def test_pre_instruction_prepends_with_blank_line() -> None:
    t = AgentTemplate(model="test", pre_instruction="JSON only.")
    a = _agent(t, instructions="Find facts.")
    assert a.instructions == "JSON only.\n\nFind facts."


def test_pre_instruction_applies_to_every_materialised_agent() -> None:
    t = AgentTemplate(model="test", pre_instruction="Be concise.")
    a = _agent(t, name="a", instructions="Question one.")
    b = _agent(t, name="b", instructions="Question two.")
    assert a.instructions.startswith("Be concise.\n\n")
    assert b.instructions.startswith("Be concise.\n\n")
    assert a.instructions != b.instructions  # per-agent body differs


def test_pre_instruction_with_multiline_text() -> None:
    """Multi-line preamble is preserved verbatim — no normalisation."""
    pre = "Line 1.\nLine 2.\nLine 3."
    t = AgentTemplate(model="test", pre_instruction=pre)
    a = _agent(t, instructions="Body.")
    assert a.instructions == f"{pre}\n\nBody."


# ---------------------------------------------------------------------------
# Override semantics — per-call kwarg vs template default
# ---------------------------------------------------------------------------


def test_template_default_used_when_call_omits() -> None:
    t = AgentTemplate(model="anthropic:claude-sonnet-4-6", trust_level=TrustLevel.LOW)
    a = _agent(t)
    assert a.model == "anthropic:claude-sonnet-4-6"
    assert a.trust_level is TrustLevel.LOW


def test_per_call_kwarg_overrides_template() -> None:
    t = AgentTemplate(model="anthropic:claude-sonnet-4-6", trust_level=TrustLevel.LOW)
    a = _agent(t, model="openai:gpt-5.2", trust_level=TrustLevel.HIGH)
    assert a.model == "openai:gpt-5.2"
    assert a.trust_level is TrustLevel.HIGH


def test_per_call_none_inherits_template() -> None:
    """Explicitly passing ``None`` per call is equivalent to omitting —
    the template's value (if any) is used. ``None`` is the unspecified
    marker, not a real value."""
    t = AgentTemplate(model="anthropic:claude-sonnet-4-6")
    a = _agent(t, model=None)
    assert a.model == "anthropic:claude-sonnet-4-6"


def test_agent_default_used_when_neither_template_nor_call_set() -> None:
    """trust_level falls back to Agent's default (MEDIUM) when neither
    the template nor the call sets it."""
    t = AgentTemplate(model="test")
    a = _agent(t)
    assert a.trust_level is TrustLevel.MEDIUM


def test_required_field_unset_raises() -> None:
    """Agent.model is required. Empty template + no per-call model =
    ValidationError at Agent construction."""
    t = AgentTemplate()
    with pytest.raises(ValidationError):
        t.agent(name="x", instructions="hi", output_type=_Out)


# ---------------------------------------------------------------------------
# Override semantics — collections replace, don't extend
# ---------------------------------------------------------------------------


def test_tools_per_call_replaces_template() -> None:
    """Documented behaviour: per-call ``tools=`` REPLACES the template's
    set; it doesn't extend. If the user wants both they build the union
    explicitly."""
    t = AgentTemplate(model="test", tools=frozenset({"a", "b"}))
    a = _agent(t, tools=frozenset({"c"}))
    assert a.tools == frozenset({"c"})


def test_tools_explicit_union_pattern() -> None:
    """Documented escape hatch: build the union from template.tools."""
    t = AgentTemplate(model="test", tools=frozenset({"a"}))
    extra = frozenset({"c"})
    assert t.tools is not None
    a = _agent(t, tools=t.tools | extra)
    assert a.tools == frozenset({"a", "c"})


def test_fallback_models_per_call_replaces() -> None:
    t = AgentTemplate(model="test", fallback_models=("a", "b"))
    a = _agent(t, fallback_models=("c",))
    assert a.fallback_models == ("c",)


# ---------------------------------------------------------------------------
# Shared limiter — same instance threads through to every materialised agent
# ---------------------------------------------------------------------------


def test_shared_concurrency_limiter_threads_through() -> None:
    pool = ConcurrencyLimiter(max_running=2, name="shared")
    t = AgentTemplate(model="test", model_concurrency_limiter=pool)
    a = _agent(t, name="a")
    b = _agent(t, name="b")
    assert a.model_concurrency_limiter is pool
    assert b.model_concurrency_limiter is pool


# ---------------------------------------------------------------------------
# Resulting Agent is fully-formed and frozen
# ---------------------------------------------------------------------------


def test_materialised_agent_is_frozen() -> None:
    t = AgentTemplate(model="test")
    a = _agent(t)
    with pytest.raises(ValidationError):
        a.model = "openai:gpt-5.2"


def test_materialised_agent_has_required_fields() -> None:
    t = AgentTemplate(model="test")
    a = _agent(t, name="researcher", instructions="Find facts.", output_type=_Out)
    assert a.name == "researcher"
    assert a.instructions == "Find facts."
    assert a.output_type is _Out


def test_context_passer_default_when_template_none() -> None:
    """Template doesn't set context_passer → Agent's default (NullContextPasser)."""
    t = AgentTemplate(model="test")
    a = _agent(t)
    assert isinstance(a.context_passer, NullContextPasser)


def test_context_passer_template_default_used() -> None:
    t = AgentTemplate(model="test", context_passer=FullContextPasser())
    a = _agent(t)
    assert isinstance(a.context_passer, FullContextPasser)


# ---------------------------------------------------------------------------
# Per-agent hooks pass through
# ---------------------------------------------------------------------------


def test_pre_post_process_hooks_pass_through() -> None:
    def double(x: _Out) -> _Out:
        return _Out(text=x.text * 2)

    t = AgentTemplate(model="test")
    a = _agent(t, post_process=(double,))
    assert a.post_process == (double,)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def test_agent_template_is_publicly_exported() -> None:
    import murmur

    assert murmur.AgentTemplate is AgentTemplate
    assert "AgentTemplate" in murmur.__all__
