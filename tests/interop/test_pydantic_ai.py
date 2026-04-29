"""Tests for :func:`murmur.interop.from_pydantic_ai`.

Build a PydanticAI agent (using ``TestModel`` so no network), hand it
to the adapter, and check the resulting :class:`murmur.Agent` carries
the right ``model`` / ``instructions`` / ``output_type``.
"""

from __future__ import annotations

import pydantic_ai
import pytest
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from murmur.interop import from_pydantic_ai
from murmur.types import TrustLevel


class _Out(BaseModel):
    text: str


def _build_pa_agent(instructions: str = "you are a tester") -> pydantic_ai.Agent:
    return pydantic_ai.Agent(
        model=TestModel(),
        instructions=instructions,
        output_type=_Out,
    )


def test_extracts_model_string_from_test_model() -> None:
    """``TestModel`` reports ``system="test"``, ``model_name="test"``."""
    pa = _build_pa_agent()
    mu = from_pydantic_ai(pa, name="t1", output_type=_Out)
    assert mu.model == "test:test"


def test_extracts_instructions_from_pa_internal_list() -> None:
    pa = _build_pa_agent("be kind and concise")
    mu = from_pydantic_ai(pa, name="t1", output_type=_Out)
    assert "kind and concise" in mu.instructions


def test_explicit_overrides_take_precedence() -> None:
    pa = _build_pa_agent("ignored")
    mu = from_pydantic_ai(
        pa,
        name="t1",
        output_type=_Out,
        model="anthropic:claude-sonnet-4-6",
        instructions="actual instructions",
    )
    assert mu.model == "anthropic:claude-sonnet-4-6"
    assert mu.instructions == "actual instructions"


def test_trust_level_default_is_medium() -> None:
    pa = _build_pa_agent()
    mu = from_pydantic_ai(pa, name="t1", output_type=_Out)
    assert mu.trust_level is TrustLevel.MEDIUM


def test_explicit_trust_level_propagates() -> None:
    pa = _build_pa_agent()
    mu = from_pydantic_ai(pa, name="t1", output_type=_Out, trust_level=TrustLevel.LOW)
    assert mu.trust_level is TrustLevel.LOW


def test_name_is_required() -> None:
    pa = _build_pa_agent()
    with pytest.raises(TypeError):
        # name is keyword-only and required — TypeError on missing.
        from_pydantic_ai(pa, output_type=_Out)  # ty: ignore[missing-argument]  # the test point
