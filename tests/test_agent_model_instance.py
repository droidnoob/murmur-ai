"""Tests for ``Agent.model`` accepting a constructed PydanticAI Model.

The string form (``"openai:gpt-5.2"``) is the common case, and PydanticAI
auto-resolves the matching Model and default Provider. The instance form is
used when the user needs a non-default Provider (Azure, Bedrock, OpenRouter,
custom HTTP client). These tests cover the wiring — that ``Agent.model``
accepts a ``Model`` instance, that it survives validation, that it round-trips
through dispatch, and that the re-export modules surface the expected vendor
classes without forcing user code to import from ``pydantic_ai``.

Provider semantics themselves are upstream PydanticAI concerns; we only
assert that Murmur preserves the Model the user constructed.
"""

from __future__ import annotations

from typing import Any

import pydantic_ai
import pytest
from pydantic import BaseModel, ValidationError
from pydantic_ai.models import Model
from pydantic_ai.models.test import TestModel

from murmur import Agent, AgentRuntime, TaskSpec
from murmur._dispatch import build_pydantic_ai_agent
from murmur.tools.executor import ToolExecutor
from murmur.tools.registry import ToolRegistry


class _Out(BaseModel):
    answer: str


def _make_agent(**kwargs: Any) -> Agent:
    defaults: dict[str, Any] = {
        "name": "r",
        "model": "test",
        "instructions": "...",
        "output_type": _Out,
    }
    defaults.update(kwargs)
    return Agent(**defaults)


def _capture_pa_init() -> tuple[dict[str, Any], Any]:
    """Test seam — patch ``pydantic_ai.Agent.__init__`` to capture kwargs."""
    captured: dict[str, Any] = {}
    original_init = pydantic_ai.Agent.__init__

    def _capture(self: pydantic_ai.Agent, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)
        original_init(self, *args, **kwargs)

    pydantic_ai.Agent.__init__ = _capture  # ty: ignore[invalid-assignment]  # test seam
    return captured, original_init


def _restore_pa_init(original_init: Any) -> None:
    pydantic_ai.Agent.__init__ = original_init  # test seam restore


# ---------------------------------------------------------------------------
# Field shape — the str | Model union holds for both forms.
# ---------------------------------------------------------------------------


def test_string_form_round_trips() -> None:
    a = _make_agent(model="anthropic:claude-sonnet-4-6")
    assert a.model == "anthropic:claude-sonnet-4-6"


def test_model_instance_form_round_trips() -> None:
    m = TestModel()
    a = _make_agent(model=m)
    assert a.model is m
    assert isinstance(a.model, Model)


def test_model_field_is_frozen() -> None:
    a = _make_agent(model=TestModel())
    with pytest.raises(ValidationError):
        a.model = TestModel()


def test_with_swaps_model_instance() -> None:
    m1, m2 = TestModel(), TestModel()
    a = _make_agent(model=m1)
    b = a.with_(model=m2)
    assert a.model is m1
    assert b.model is m2


def test_invalid_model_type_rejected() -> None:
    """Non-string, non-Model values fail validation."""
    with pytest.raises(ValidationError):
        _make_agent(model=12345)


# ---------------------------------------------------------------------------
# Dispatch — Model instance flows through to pydantic_ai.Agent(model=...).
# ---------------------------------------------------------------------------


async def test_dispatch_forwards_model_instance() -> None:
    m = TestModel()
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(model=m),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    assert captured["model"] is m


async def test_dispatch_forwards_string() -> None:
    captured, original = _capture_pa_init()
    try:
        await build_pydantic_ai_agent(
            agent=_make_agent(model="test"),
            allowed=frozenset(),
            registry=ToolRegistry(),
            executor=ToolExecutor(ToolRegistry()),
            task_id="t-1",
        )
    finally:
        _restore_pa_init(original)
    assert captured["model"] == "test"


@pytest.mark.asyncio
async def test_runtime_run_with_model_instance() -> None:
    """End-to-end — Model instance dispatches through AgentRuntime.run()."""
    a = _make_agent(model=TestModel())
    rt = AgentRuntime()
    result = await rt.run(a, TaskSpec(input="ping"))
    assert result.is_ok()
    assert result.output is not None
    assert isinstance(result.output, _Out)


# ---------------------------------------------------------------------------
# Re-exports — Public API Rule (CLAUDE.md §2): user code never imports from
# pydantic_ai. Every Model/Provider that murmur supports must be importable
# from murmur.models / murmur.providers and resolve to the same class.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "AnthropicModel",
        "BedrockConverseModel",
        "CerebrasModel",
        "CohereModel",
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
    ],
)
def test_murmur_models_reexports(name: str) -> None:
    """Every advertised Model class is importable from murmur.models."""
    import murmur.models as m

    cls = getattr(m, name)
    assert isinstance(cls, type)
    # Every concrete model is a Model subclass; the abstract Model itself
    # is also a class (the base class).
    if name != "Model":
        assert issubclass(cls, Model)
    assert name in m.__all__


@pytest.mark.parametrize(
    "name",
    [
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
    ],
)
def test_murmur_providers_reexports(name: str) -> None:
    """Every advertised Provider class is importable from murmur.providers."""
    import murmur.providers as p

    cls = getattr(p, name)
    assert isinstance(cls, type)
    assert name in p.__all__


def test_provider_attaches_to_model() -> None:
    """Smoke test for the user-facing pattern: Provider rides inside Model.

    This is the canonical shape — there's no separate ``Agent.provider=``
    field, by design. The Provider is constructed, attached to a Model, and
    the Model lands in ``Agent.model``. The Provider survives unchanged.
    """
    from murmur.models import OpenAIChatModel
    from murmur.providers import OpenAIProvider

    provider = OpenAIProvider(api_key="sk-test", base_url="https://example.test/v1")
    model = OpenAIChatModel("gpt-5.2", provider=provider)
    a = _make_agent(model=model)
    assert a.model is model
    # Round-trip the Provider via the Model — it's the user's contract that
    # whatever Provider they wired up is preserved end-to-end.
    assert isinstance(a.model, OpenAIChatModel)
    assert a.model.client.api_key == "sk-test"
