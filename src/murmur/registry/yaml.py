"""YAML-backed spec registry.

Loads :class:`Agent` definitions from ``<root>/agents/*.yaml``. Satisfies
:class:`murmur.core.protocols.registry.Registry` structurally — required
surface: ``get``, ``list``, ``validate``.

Convention (per ``specs/README.md``): each YAML file's filename
(without extension) **must** equal the spec's ``name`` field. ``validate``
flags mismatches alongside Pydantic / class-resolution failures.

>>> reg = YamlRegistry(Path("specs"))
>>> errors = reg.validate()         # list of "<file>: <error>" strings
>>> agent = reg.get("researcher")   # raises RegistryError if absent
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import yaml as _yaml
from pydantic import ValidationError

from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser
from murmur.core.errors import RegistryError, SpecValidationError
from murmur.core.protocols.registry import ValidationErrors
from murmur.registry._yaml_schema import (
    AgentSpecYaml,
    class_to_path,
    resolve_class,
)
from murmur.types import TrustLevel

if TYPE_CHECKING:
    from murmur.agent import Agent
    from murmur.core.protocols.context import ContextPasser


_CONTEXT_PASSERS: dict[str, type[ContextPasser]] = {
    "null": NullContextPasser,
    "full": FullContextPasser,
}


class YamlRegistry:
    """File-backed spec registry rooted at a directory."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()
        if not self._root.is_dir():
            raise RegistryError(f"YAML registry root does not exist: {self._root}")
        self._agents: dict[str, Agent] = {}
        self._errors: list[str] = []
        self._loaded: bool = False

    @property
    def root(self) -> Path:
        return self._root

    def reload(self) -> None:
        """Re-walk ``<root>/agents`` and rebuild the in-memory map.

        Errors during load are collected (not raised) — surface them via
        :meth:`validate`. ``get`` raises :class:`RegistryError` for missing
        names regardless.
        """
        self._agents.clear()
        self._errors.clear()
        agents_dir = self._root / "agents"
        if agents_dir.is_dir():
            for path in sorted(agents_dir.glob("*.yaml")):
                self._load_one(path)
        self._loaded = True

    def get(self, name: str) -> Agent:
        if not self._loaded:
            self.reload()
        if name not in self._agents:
            raise RegistryError(f"agent {name!r} not found in {self._root}")
        return self._agents[name]

    def list(self) -> frozenset[str]:
        if not self._loaded:
            self.reload()
        return frozenset(self._agents.keys())

    def validate(self) -> ValidationErrors:
        """Return a list of ``"<file>: <error>"`` strings (empty == OK)."""
        self.reload()
        return list(self._errors)

    # ------------------------------------------------------------------ private

    def _load_one(self, path: Path) -> None:
        rel = path.relative_to(self._root)
        try:
            raw = _yaml.safe_load(path.read_text())
        except _yaml.YAMLError as exc:
            self._errors.append(f"{rel}: invalid YAML — {exc}")
            return

        if not isinstance(raw, dict):
            self._errors.append(
                f"{rel}: top-level must be a mapping, got {type(raw).__name__}"
            )
            return

        try:
            spec = AgentSpecYaml.model_validate(raw)
        except ValidationError as exc:
            self._errors.append(f"{rel}: {_render_pydantic_errors(exc)}")
            return

        if spec.name != path.stem:
            self._errors.append(
                f"{rel}: filename {path.stem!r} does not match spec name {spec.name!r}"
            )
            return

        try:
            agent = spec_to_agent(spec)
        except SpecValidationError as exc:
            self._errors.append(f"{rel}: {exc}")
            return

        if spec.name in self._agents:
            self._errors.append(
                f"{rel}: duplicate agent name {spec.name!r} (already loaded)"
            )
            return

        self._agents[spec.name] = agent


def spec_to_agent(spec: AgentSpecYaml) -> Agent:
    """Convert a validated :class:`AgentSpecYaml` into a runtime ``Agent``."""
    from murmur.agent import Agent

    output_cls = resolve_class(spec.output_type)
    input_cls = resolve_class(spec.input_type) if spec.input_type else None
    passer_cls = _CONTEXT_PASSERS.get(spec.context_passer)
    if passer_cls is None:  # pragma: no cover — Literal exhausts the type
        raise SpecValidationError(f"unknown context_passer {spec.context_passer!r}")
    return Agent(
        name=spec.name,
        model=spec.model,
        instructions=spec.instructions,
        output_type=output_cls,
        input_type=input_cls,
        tools=frozenset(spec.tools),
        trust_level=TrustLevel(spec.trust_level),
        context_passer=passer_cls(),
        backend=spec.backend,
    )


def agent_to_spec(agent: Agent) -> AgentSpecYaml:
    """Render an ``Agent`` back to the YAML wire form (round-trip).

    The agent's ``context_passer`` must be one of the YAML-supported
    concretes (``NullContextPasser`` / ``FullContextPasser``); other
    types raise :class:`SpecValidationError`.
    """
    passer_name: str | None = None
    for name, cls in _CONTEXT_PASSERS.items():
        if isinstance(agent.context_passer, cls):
            passer_name = name
            break
    if passer_name is None:
        raise SpecValidationError(
            f"context_passer {type(agent.context_passer).__name__!r} is not "
            f"YAML-serialisable; supported: {sorted(_CONTEXT_PASSERS)}"
        )
    # The instance form of ``Agent.model`` (a constructed pydantic_ai Model)
    # holds live state — HTTP clients, Provider auth, custom base URLs — and
    # cannot round-trip through YAML. Reject it explicitly; authors who need
    # an instance-form model must construct the Agent in code.
    if not isinstance(agent.model, str):
        raise SpecValidationError(
            f"agent.model is a constructed Model instance "
            f"({type(agent.model).__name__!r}); only the string form "
            f"('vendor:model_name') is YAML-serialisable. Construct this "
            f"agent in Python code instead."
        )
    # ``context_passer`` was narrowed by the lookup loop above; ``backend``
    # was validated by AgentSpecYaml when the spec was loaded — agents
    # constructed in code use ``backend: str`` which Pydantic re-validates
    # against the Literal in AgentSpecYaml's __init__.
    return AgentSpecYaml(
        version=1,
        name=agent.name,
        model=agent.model,
        instructions=agent.instructions,
        output_type=class_to_path(agent.output_type),
        input_type=class_to_path(agent.input_type) if agent.input_type else None,
        tools=sorted(agent.tools),
        trust_level=agent.trust_level,
        context_passer=cast('Literal["null", "full"]', passer_name),
        backend=cast('Literal["auto", "thread", "job"]', agent.backend),
    )


def _render_pydantic_errors(exc: ValidationError) -> str:
    parts: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(x) for x in err.get("loc", ()))
        msg = err.get("msg", "")
        parts.append(f"{loc}: {msg}" if loc else msg)
    return "; ".join(parts)


__all__ = [
    "YamlRegistry",
    "agent_to_spec",
    "spec_to_agent",
]
