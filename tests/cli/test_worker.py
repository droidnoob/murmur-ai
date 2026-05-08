"""``murmur worker start`` — argument parsing, setup paths, and a real
startup-then-shutdown over the in-process ``memory://`` broker."""

from __future__ import annotations

import asyncio
import os
import signal
import textwrap
from pathlib import Path

import pytest

from murmur.cli import main
from murmur.cli.worker import _run_worker

_OUT_PATH = "tests.registry._yaml_fixtures.FixtureOutput"


def _write_agent(root: Path, name: str = "researcher") -> None:
    agents = root / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    (agents / f"{name}.yaml").write_text(
        textwrap.dedent(
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
    )


# ---------------------------------------------------------------------------
# Argument validation paths
# ---------------------------------------------------------------------------


def test_empty_agents_argument_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path, "researcher")
    rc = main(
        [
            "worker",
            "start",
            "--agents",
            "",
            "--broker",
            "memory://",
            "--specs",
            str(tmp_path),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "[error]" in captured.err


def test_unknown_agent_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path, "researcher")
    rc = main(
        [
            "worker",
            "start",
            "--agents",
            "ghost",
            "--broker",
            "memory://",
            "--specs",
            str(tmp_path),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "not found" in captured.err


def test_missing_specs_dir_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "worker",
            "start",
            "--agents",
            "any",
            "--broker",
            "memory://",
            "--specs",
            str(tmp_path / "ghost"),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "does not exist" in captured.err


def test_validation_errors_exit_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path, "researcher")
    # Break it: replace version with an unsupported one.
    bad = (
        (tmp_path / "agents" / "researcher.yaml")
        .read_text()
        .replace("version: 1", "version: 99")
    )
    (tmp_path / "agents" / "researcher.yaml").write_text(bad)
    rc = main(
        [
            "worker",
            "start",
            "--agents",
            "researcher",
            "--broker",
            "memory://",
            "--specs",
            str(tmp_path),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 1
    assert "[error]" in captured.err


def test_unsupported_broker_url_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """memory:// works; an unsupported URL scheme errors out cleanly."""
    _write_agent(tmp_path, "researcher")
    rc = main(
        [
            "worker",
            "start",
            "--agents",
            "researcher",
            "--broker",
            "ftp://nope",
            "--specs",
            str(tmp_path),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "unsupported broker URL scheme" in captured.err


# ---------------------------------------------------------------------------
# Happy path: real startup → SIGTERM → clean shutdown
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not hasattr(signal, "SIGTERM") or os.name != "posix",
    reason="signal-based shutdown test only on POSIX",
)
async def test_worker_starts_then_shuts_down_on_sigterm(tmp_path: Path) -> None:
    """End-to-end: build the worker, signal SIGTERM, verify clean exit."""
    import argparse as _argparse

    _write_agent(tmp_path, "researcher")
    args = _argparse.Namespace(
        agents="researcher",
        all_from=None,
        broker="memory://",
        specs=tmp_path,
        concurrency=4,
        prefetch=2,
        consumer_id=None,
        heartbeat_seconds=0,
    )

    async def _trigger_sigterm() -> None:
        await asyncio.sleep(0.1)  # let the worker subscribe before we kill it
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.create_task(_trigger_sigterm())
    rc = await _run_worker(args)
    assert rc == 0


# ---------------------------------------------------------------------------
# ``--all-from`` registry auto-discovery
# ---------------------------------------------------------------------------


def test_all_from_registers_every_agent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--all-from`` resolves every agent under the given specs root."""
    import argparse as _argparse

    _write_agent(tmp_path, "alpha")
    _write_agent(tmp_path, "beta")

    args = _argparse.Namespace(
        agents=None,
        all_from=tmp_path,
        broker="memory://",
        specs=tmp_path,
        concurrency=2,
        prefetch=1,
        consumer_id=None,
        heartbeat_seconds=0,
    )

    async def _trigger_sigterm() -> None:
        await asyncio.sleep(0.1)
        os.kill(os.getpid(), signal.SIGTERM)

    async def _drive() -> int:
        asyncio.create_task(_trigger_sigterm())
        return await _run_worker(args)

    rc = asyncio.run(_drive())
    assert rc == 0


def test_agents_and_all_from_mutually_exclusive(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """argparse rejects passing both flags (exits via ``SystemExit``)."""
    _write_agent(tmp_path, "researcher")
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "worker",
                "start",
                "--agents",
                "researcher",
                "--all-from",
                str(tmp_path),
                "--broker",
                "memory://",
            ]
        )
    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert "not allowed with" in captured.err or "mutually exclusive" in captured.err


def test_neither_agents_nor_all_from_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["worker", "start", "--broker", "memory://"])
    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert "one of the arguments" in captured.err or "required" in captured.err


def test_empty_registry_under_all_from_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--all-from`` against a directory with no agents errors with a hint."""
    (tmp_path / "agents").mkdir()
    rc = main(
        [
            "worker",
            "start",
            "--all-from",
            str(tmp_path),
            "--broker",
            "memory://",
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "no agents found" in captured.err
