"""``murmur run <script.py>`` — exit codes + argv forwarding."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from murmur.cli import main


def _write_script(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body))
    return path


def test_run_executes_script_and_returns_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    script = _write_script(
        tmp_path / "ok.py",
        """\
        from murmur import Agent  # noqa: F401  — just prove the import works
        print("hello from script")
        """,
    )
    rc = main(["run", str(script)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "hello from script" in captured.out


def test_run_propagates_systemexit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    script = _write_script(
        tmp_path / "fail.py",
        """\
        import sys
        sys.exit(7)
        """,
    )
    rc = main(["run", str(script)])
    assert rc == 7


def test_run_missing_script_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(["run", str(tmp_path / "ghost.py")])
    captured = capsys.readouterr()
    assert rc == 2
    assert "[error]" in captured.err


def test_run_forwards_argv(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = _write_script(
        tmp_path / "argv.py",
        """\
        import sys
        print(",".join(sys.argv[1:]))
        """,
    )
    rc = main(["run", str(script), "--", "--alpha", "beta"])
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out.strip() == "--alpha,beta"
