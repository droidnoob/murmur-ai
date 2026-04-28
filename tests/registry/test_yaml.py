"""``YamlRegistry`` — contract suite + round-trip + property tests."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml as _yaml
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from tests.contracts.registry_contract import RegistryContract
from tests.registry._yaml_fixtures import FixtureInput, FixtureOutput

from murmur.agent import Agent
from murmur.context.full import FullContextPasser
from murmur.context.null import NullContextPasser
from murmur.core.errors import RegistryError, SpecValidationError
from murmur.core.protocols.context import ContextPasser
from murmur.registry._yaml_schema import AgentSpecYaml
from murmur.registry.yaml import (
    YamlRegistry,
    agent_to_spec,
    spec_to_agent,
)
from murmur.types import TrustLevel

_OUT_PATH = "tests.registry._yaml_fixtures.FixtureOutput"
_IN_PATH = "tests.registry._yaml_fixtures.FixtureInput"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_spec(root: Path, name: str, body: dict[str, object]) -> Path:
    agents_dir = root / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    target = agents_dir / f"{name}.yaml"
    target.write_text(_yaml.safe_dump(body))
    return target


def _good_body(name: str = "researcher") -> dict[str, object]:
    return {
        "version": 1,
        "name": name,
        "model": "anthropic:claude-sonnet-4-6",
        "instructions": "Be terse.",
        "output_type": _OUT_PATH,
        "tools": [],
        "trust_level": "medium",
        "context_passer": "null",
        "backend": "auto",
    }


# ---------------------------------------------------------------------------
# Contract suite — required Protocol surface
# ---------------------------------------------------------------------------


class TestYamlRegistryContract(RegistryContract):
    @pytest.fixture
    def registry(self, tmp_path: Path) -> YamlRegistry:
        # Empty agents dir → list() == frozenset(); validate() == [].
        (tmp_path / "agents").mkdir()
        return YamlRegistry(tmp_path)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_root_must_exist(tmp_path: Path) -> None:
    with pytest.raises(RegistryError, match="does not exist"):
        YamlRegistry(tmp_path / "ghost")


def test_no_agents_dir_is_silent(tmp_path: Path) -> None:
    """A bare directory with no ``agents/`` subdir is just an empty registry."""
    reg = YamlRegistry(tmp_path)
    assert reg.list() == frozenset()
    assert reg.validate() == []


# ---------------------------------------------------------------------------
# Loading good specs
# ---------------------------------------------------------------------------


def test_loads_a_good_spec(tmp_path: Path) -> None:
    _write_spec(tmp_path, "researcher", _good_body("researcher"))
    reg = YamlRegistry(tmp_path)
    assert "researcher" in reg.list()
    a = reg.get("researcher")
    assert a.name == "researcher"
    assert a.output_type is FixtureOutput
    assert isinstance(a.context_passer, NullContextPasser)


def test_loads_optional_input_type(tmp_path: Path) -> None:
    body = _good_body("typed-in")
    body["input_type"] = _IN_PATH
    _write_spec(tmp_path, "typed-in", body)
    reg = YamlRegistry(tmp_path)
    a = reg.get("typed-in")
    assert a.input_type is FixtureInput


def test_full_context_passer_is_resolved(tmp_path: Path) -> None:
    body = _good_body("full-ctx")
    body["context_passer"] = "full"
    _write_spec(tmp_path, "full-ctx", body)
    reg = YamlRegistry(tmp_path)
    a = reg.get("full-ctx")
    assert isinstance(a.context_passer, FullContextPasser)


def test_unknown_agent_raises(tmp_path: Path) -> None:
    (tmp_path / "agents").mkdir()
    reg = YamlRegistry(tmp_path)
    with pytest.raises(RegistryError, match="not found"):
        reg.get("missing")


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_validate_reports_filename_mismatch(tmp_path: Path) -> None:
    _write_spec(tmp_path, "different-name", _good_body("researcher"))
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("does not match" in e for e in errors)


def test_validate_reports_invalid_yaml(tmp_path: Path) -> None:
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / "broken.yaml").write_text("not: valid: yaml: ::: [")
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("invalid YAML" in e for e in errors)


def test_validate_reports_unknown_field(tmp_path: Path) -> None:
    body = _good_body("researcher")
    body["mystery_field"] = "boom"
    _write_spec(tmp_path, "researcher", body)
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert errors
    assert any("mystery_field" in e for e in errors)


def test_validate_reports_unresolvable_class_path(tmp_path: Path) -> None:
    body = _good_body("researcher")
    body["output_type"] = "no.such.module.WhoKnows"
    _write_spec(tmp_path, "researcher", body)
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("could not import" in e or "not found" in e for e in errors)


def test_validate_reports_missing_version(tmp_path: Path) -> None:
    body = _good_body("researcher")
    del body["version"]
    _write_spec(tmp_path, "researcher", body)
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("version" in e.lower() for e in errors)


def test_validate_reports_unsupported_version(tmp_path: Path) -> None:
    body = _good_body("researcher")
    body["version"] = 999
    _write_spec(tmp_path, "researcher", body)
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("version" in e.lower() for e in errors)


def test_validate_reports_top_level_not_mapping(tmp_path: Path) -> None:
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / "list-spec.yaml").write_text("- just\n- a\n- list\n")
    reg = YamlRegistry(tmp_path)
    errors = reg.validate()
    assert any("must be a mapping" in e for e in errors)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def _agent(
    name: str = "r",
    *,
    passer: ContextPasser | None = None,
    tools: frozenset[str] = frozenset(),
) -> Agent:
    return Agent(
        name=name,
        model="anthropic:claude-sonnet-4-6",
        instructions="x",
        output_type=FixtureOutput,
        tools=tools,
        trust_level=TrustLevel.MEDIUM,
        context_passer=passer or NullContextPasser(),
    )


def test_agent_to_spec_then_back_yields_equal_agent() -> None:
    original = _agent(
        "round-trip", passer=FullContextPasser(), tools=frozenset({"web_search"})
    )
    spec = agent_to_spec(original)
    restored = spec_to_agent(spec)
    assert restored.name == original.name
    assert restored.model == original.model
    assert restored.instructions == original.instructions
    assert restored.output_type is original.output_type
    assert restored.tools == original.tools
    assert restored.trust_level == original.trust_level
    assert isinstance(restored.context_passer, FullContextPasser)


def test_agent_to_spec_rejects_unsupported_context_passer() -> None:
    class _Custom:
        async def prepare(self, *_a: object, **_k: object) -> object:  # noqa: ANN001
            return None

    a = _agent("weird", passer=_Custom())  # ty: ignore[invalid-argument-type]
    with pytest.raises(SpecValidationError, match="not YAML-serialisable"):
        agent_to_spec(a)


def test_yaml_round_trip_via_disk(tmp_path: Path) -> None:
    original = _agent("disk-rt", tools=frozenset({"web_search", "read_file"}))
    spec = agent_to_spec(original)
    _write_spec(tmp_path, original.name, spec.model_dump(mode="json"))
    reg = YamlRegistry(tmp_path)
    assert reg.validate() == []
    restored = reg.get(original.name)
    assert restored.name == original.name
    assert restored.tools == original.tools
    assert restored.output_type is FixtureOutput


# ---------------------------------------------------------------------------
# Hypothesis property: any valid spec round-trips losslessly
# ---------------------------------------------------------------------------


_NAME = st.from_regex(r"[a-z][a-z0-9-]{0,30}", fullmatch=True)
_INSTRUCTIONS = st.text(min_size=1, max_size=200).filter(lambda s: s.strip())
_TOOLS = st.lists(
    st.from_regex(r"[a-z_][a-z0-9_]{0,20}", fullmatch=True),
    min_size=0,
    max_size=5,
    unique=True,
)


@settings(
    max_examples=30,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
@given(
    name=_NAME,
    model=st.sampled_from(
        ["anthropic:claude-sonnet-4-6", "openai:gpt-4o", "anthropic:claude-haiku-4-5"]
    ),
    instructions=_INSTRUCTIONS,
    tools=_TOOLS,
    trust=st.sampled_from(list(TrustLevel)),
    passer=st.sampled_from(["null", "full"]),
    backend=st.sampled_from(["auto", "thread", "job"]),
)
def test_round_trip_for_arbitrary_specs(
    tmp_path_factory: pytest.TempPathFactory,
    name: str,
    model: str,
    instructions: str,
    tools: list[str],
    trust: TrustLevel,
    passer: str,
    backend: str,
) -> None:
    body = {
        "version": 1,
        "name": name,
        "model": model,
        "instructions": instructions,
        "output_type": _OUT_PATH,
        "tools": tools,
        "trust_level": trust.value,
        "context_passer": passer,
        "backend": backend,
    }
    spec = AgentSpecYaml.model_validate(body)
    agent = spec_to_agent(spec)
    spec_back = agent_to_spec(agent)
    # Tools become a sorted list on round-trip; compare as sets.
    assert spec.name == spec_back.name
    assert spec.model == spec_back.model
    assert spec.instructions == spec_back.instructions
    assert spec.output_type == spec_back.output_type
    assert set(spec.tools) == set(spec_back.tools)
    assert spec.trust_level == spec_back.trust_level
    assert spec.context_passer == spec_back.context_passer
    assert spec.backend == spec_back.backend


# ---------------------------------------------------------------------------
# A literal example matching the format users will actually write
# ---------------------------------------------------------------------------


def test_canonical_example_loads(tmp_path: Path) -> None:
    """Mirrors the YAML shape from CLAUDE.md §5 / specs/README.md."""
    yaml_text = textwrap.dedent(
        f"""\
        version: 1
        name: researcher
        model: anthropic:claude-sonnet-4-6
        trust_level: medium
        context_passer: "null"
        backend: auto
        instructions: |
          You are a research minion. Produce a finding.
        output_type: {_OUT_PATH}
        tools:
          - web_search
        """
    )
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / "researcher.yaml").write_text(yaml_text)
    reg = YamlRegistry(tmp_path)
    assert reg.validate() == []
    a = reg.get("researcher")
    assert a.tools == frozenset({"web_search"})
