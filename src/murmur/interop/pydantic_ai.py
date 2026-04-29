"""PydanticAI interop — wrap a user-supplied PydanticAI agent into a Murmur ``Agent``.

This module is the **only** place in the public package allowed to import
``pydantic_ai`` symbols. It exists to ease migration; new code should construct
:class:`murmur.Agent` directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from murmur.agent import Agent
from murmur.types import TrustLevel

if TYPE_CHECKING:
    from pydantic import BaseModel
    from pydantic_ai import Agent as PydanticAIAgent


def from_pydantic_ai(
    pydantic_ai_agent: PydanticAIAgent,
    *,
    name: str,
    output_type: type[BaseModel],
    trust_level: TrustLevel = TrustLevel.MEDIUM,
    model: str | None = None,
    instructions: str | None = None,
) -> Agent:
    """Wrap an existing PydanticAI ``Agent`` into a Murmur :class:`Agent`.

    Extracts ``model`` (as ``"{system}:{model_name}"``) and ``instructions``
    from the PydanticAI agent's internals; you can override either via the
    matching kwarg if extraction picks up something unhelpful (notably for
    ``TestModel``, which has ``system="test"``). ``output_type`` is required
    — PydanticAI's internal output schema is wrapped, so we don't try to
    excavate the user's original Pydantic class. Tools are not extracted —
    re-register them on Murmur's :class:`murmur.tools.ToolRegistry` directly
    (they execute through Murmur's policy gate, not the agent's).

    >>> from pydantic_ai import Agent as PAAgent
    >>> from murmur.interop import from_pydantic_ai
    >>> mu_agent = from_pydantic_ai(my_pa_agent, name="researcher", output_type=Finding)
    """
    pa = pydantic_ai_agent

    extracted_model: str
    if model is not None:
        extracted_model = model
    else:
        # ``pa._model`` is typed as ``Model | str | None`` — cast to ``Any``
        # so we can branch on the runtime shape without ty fighting us. This
        # is the adapter; this is the place that knows about PA internals.
        pa_model = cast("Any", pa._model)  # noqa: SLF001 — sanctioned
        if isinstance(pa_model, str):
            extracted_model = pa_model
        else:
            try:
                extracted_model = f"{pa_model.system}:{pa_model.model_name}"
            except AttributeError as exc:  # pragma: no cover — pa-version drift
                raise ValueError(
                    "Could not extract model spec from the PydanticAI agent — "
                    "pass `model='provider:name'` explicitly."
                ) from exc

    extracted_instructions: str
    if instructions is not None:
        extracted_instructions = instructions
    else:
        raw_instructions: Any = pa._instructions  # noqa: SLF001
        if isinstance(raw_instructions, list):
            parts = [s for s in raw_instructions if isinstance(s, str)]
            extracted_instructions = " ".join(parts) if parts else ""
        elif isinstance(raw_instructions, str):
            extracted_instructions = raw_instructions
        else:
            extracted_instructions = ""

    return Agent(
        name=name,
        model=extracted_model,
        instructions=extracted_instructions,
        output_type=output_type,
        trust_level=trust_level,
    )


__all__ = ["from_pydantic_ai"]
