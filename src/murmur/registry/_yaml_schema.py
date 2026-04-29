"""Canonical YAML surface for :class:`murmur.Agent`.

Per **D9** (`.planning/phase-1-mvp-dispatch.md`): the YAML is a frozen
Pydantic model, used as both schema validator and parser. Per **D4**, the
``output_type`` (and optional ``input_type``) field is an importable
**class path** — ``my_pkg.outputs.ResearchOutput`` — rather than raw JSON
Schema. This keeps the YAML small, fully static for ``ty``, and avoids
the JSON-Schema → dynamic-Pydantic conversion whose edge cases would
otherwise leak into the loader.

This module is internal — leading underscore. The public ``YamlRegistry``
is what users import.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from murmur.core.errors import SpecValidationError
from murmur.types import TrustLevel

if TYPE_CHECKING:
    pass


_KnownContextPasser = Literal["null", "full"]
_KnownBackend = Literal["auto", "thread", "job"]


class AgentSpecYaml(BaseModel):
    """The on-disk YAML form of a :class:`murmur.Agent`.

    Round-trips with the in-memory ``Agent`` via
    :func:`spec_to_agent` / :func:`agent_to_spec`. Extra fields are
    rejected so typos surface as clear errors. ``version`` is required so
    future schema migrations have a discriminator to switch on instead of
    sniffing field presence; the validator rejects any value other than
    the supported integer(s).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[1]
    """Schema version. Must be ``1`` — future migrations bump this."""

    name: str
    model: str
    instructions: str

    output_type: str
    """Importable class path of the agent's output Pydantic model.

    Format: ``module.path.ClassName`` — the loader does
    ``importlib.import_module`` + ``getattr`` and validates that the class
    is a :class:`pydantic.BaseModel` subclass.
    """

    input_type: str | None = None
    """Optional structured-input class path. ``None`` (the default) means
    the agent takes a plain string."""

    tools: list[str] = Field(default_factory=list)
    trust_level: TrustLevel = TrustLevel.MEDIUM
    context_passer: _KnownContextPasser = "null"
    backend: _KnownBackend = "auto"


def class_to_path(cls: type[object]) -> str:
    """Render a class as ``"module.ClassName"`` for storage in YAML."""
    return f"{cls.__module__}.{cls.__qualname__}"


def resolve_class(path: str, *, base: type[BaseModel] = BaseModel) -> type[BaseModel]:
    """Resolve a class path string to its Python class.

    Raises :class:`SpecValidationError` if the import fails, the attribute
    is missing, or the resolved object is not a ``BaseModel`` subclass.
    """
    if "." not in path:
        raise SpecValidationError(
            f"class path {path!r} must include a module — got bare name"
        )
    module_path, _, class_name = path.rpartition(".")
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise SpecValidationError(
            f"could not import module for class path {path!r}: {exc}"
        ) from exc
    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise SpecValidationError(
            f"class {class_name!r} not found in module {module_path!r}"
        ) from exc
    if not (isinstance(cls, type) and issubclass(cls, base)):
        raise SpecValidationError(
            f"{path!r} resolved to {cls!r}, which is not a {base.__name__} subclass"
        )
    return cls


__all__ = [
    "AgentSpecYaml",
    "class_to_path",
    "resolve_class",
]
