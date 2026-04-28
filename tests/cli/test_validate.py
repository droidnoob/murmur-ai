"""``murmur validate <dir>`` — exit codes + output shape."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from murmur.cli import main

_OUT_PATH = "tests.registry._yaml_fixtures.FixtureOutput"


def _good_yaml(name: str = "researcher") -> str:
    return textwrap.dedent(
        f"""\
        version: 1
        name: {name}
        model: anthropic:claude-sonnet-4-6
        instructions: be terse
        output_type: {_OUT_PATH}
        trust_level: medium
        context_passer: "null"
        backend: auto
        tools: []
        """
    )


def _write_agent(root: Path, name: str, body: str) -> None:
    agents = root / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    (agents / f"{name}.yaml").write_text(body)


def test_validate_empty_directory_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(["validate", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "no agents to validate" in out


def test_validate_one_good_spec_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path, "researcher", _good_yaml("researcher"))
    rc = main(["validate", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "[ok] researcher" in captured.out
    assert "1 agent(s) validated." in captured.out


def test_validate_bad_spec_exits_one_with_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = _good_yaml("researcher").replace("version: 1", "version: 999")
    _write_agent(tmp_path, "researcher", bad)
    rc = main(["validate", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 1
    assert "[error]" in captured.err
    assert "version" in captured.err.lower()


def test_validate_filename_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path, "wrong-filename", _good_yaml("right-name"))
    rc = main(["validate", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 1
    assert "does not match" in captured.err


def test_validate_missing_directory_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(["validate", str(tmp_path / "ghost")])
    captured = capsys.readouterr()
    assert rc == 2
    assert "[error]" in captured.err
