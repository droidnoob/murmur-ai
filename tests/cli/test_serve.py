"""``murmur serve`` — argument parsing and setup-path validation.

We don't actually bind a port in tests — uvicorn would block. Instead we
exercise:

- argparse → ``--help`` succeeds (subcommand registers cleanly)
- registry / spec validation paths share the same exit codes as
  ``murmur worker start``
- ``--publish-events`` requires ``--broker``
- the constructed ``AgentServer`` exposes the expected routes
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest

from murmur.cli import main

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
# Argument parsing
# ---------------------------------------------------------------------------


def test_serve_help_succeeds(capsys: pytest.CaptureFixture[str]) -> None:
    """``murmur serve --help`` exits 0 and prints the usage string."""
    with pytest.raises(SystemExit) as excinfo:
        main(["serve", "--help"])
    captured = capsys.readouterr()
    assert excinfo.value.code == 0
    assert "events" in captured.out.lower()
    assert "--port" in captured.out
    assert "--broker" in captured.out


def test_publish_events_without_broker_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path)
    rc = main(
        [
            "serve",
            "--all-from",
            str(tmp_path),
            "--publish-events",
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "--publish-events requires --broker" in captured.err


def test_missing_specs_dir_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "serve",
            "--specs",
            str(tmp_path / "ghost"),
        ]
    )
    captured = capsys.readouterr()
    assert rc == 2
    assert "[error]" in captured.err


def test_validation_errors_exit_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path)
    bad = (
        (tmp_path / "agents" / "researcher.yaml")
        .read_text()
        .replace("version: 1", "version: 99")
    )
    (tmp_path / "agents" / "researcher.yaml").write_text(bad)
    rc = main(["serve", "--specs", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 1
    assert "[error]" in captured.err


def test_empty_specs_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "agents").mkdir()
    rc = main(["serve", "--specs", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 2
    assert "no agents found" in captured.err


def test_empty_agents_argument_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path)
    rc = main(["serve", "--specs", str(tmp_path), "--agents", ""])
    captured = capsys.readouterr()
    assert rc == 2
    assert "[error]" in captured.err


def test_agents_and_all_from_mutually_exclusive(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_agent(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "serve",
                "--agents",
                "researcher",
                "--all-from",
                str(tmp_path),
            ]
        )
    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert "not allowed with" in captured.err or "mutually exclusive" in captured.err


# ---------------------------------------------------------------------------
# Setup-path: build the server WITHOUT serving — verify route shape
# ---------------------------------------------------------------------------


def _build_server_via_serve_path(tmp_path: Path, **overrides: Any) -> Any:
    """Drive the same construction code ``_run_serve`` does, but stop
    before ``server.serve(...)`` so no port is bound. Returns the live
    AgentServer for route assertions."""
    import argparse as _argparse

    from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter
    from murmur.registry.yaml import YamlRegistry
    from murmur.runtime import AgentRuntime
    from murmur.server.app import AgentServer

    args = _argparse.Namespace(
        agents=overrides.get("agents"),
        all_from=overrides.get("all_from"),
        specs=overrides.get("specs", tmp_path),
        broker=overrides.get("broker"),
        publish_events=overrides.get("publish_events", False),
        host="127.0.0.1",
        port=overrides.get("port", 8420),
        heartbeat_interval=overrides.get("heartbeat_interval", 15.0),
        no_events=overrides.get("no_events", False),
    )

    registry = YamlRegistry(args.specs)
    assert not registry.validate()

    if args.all_from is not None:
        names = sorted(registry.list())
    elif args.agents:
        names = [n.strip() for n in args.agents.split(",") if n.strip()]
    else:
        names = sorted(registry.list())
    agents = {name: registry.get(name) for name in names}

    sse_emitter: SSEEventEmitter | None = None
    if not args.no_events:
        sse_emitter = SSEEventEmitter(heartbeat_interval=args.heartbeat_interval)
        emitter = MultiEventEmitter([LogEventEmitter(), sse_emitter])
    else:
        emitter = MultiEventEmitter([LogEventEmitter()])

    runtime = AgentRuntime(
        broker=args.broker,
        event_emitter=emitter,
        publish_events=args.publish_events,
    )
    server = AgentServer(runtime=runtime, sse_emitter=sse_emitter)
    for agent in agents.values():
        server.register(agent)
    return server


def test_serve_setup_registers_events_stream_by_default(tmp_path: Path) -> None:
    _write_agent(tmp_path)
    server = _build_server_via_serve_path(tmp_path)
    paths = {getattr(r, "path", None) for r in server.app.routes}
    assert "/events/stream" in paths
    # Agent dispatch route is templated — check the template + that the
    # agent landed in the registry.
    assert "/agents/{name}/run" in paths
    assert "researcher" in server._agents


def test_serve_setup_omits_events_stream_when_no_events(tmp_path: Path) -> None:
    _write_agent(tmp_path)
    server = _build_server_via_serve_path(tmp_path, no_events=True)
    paths = {getattr(r, "path", None) for r in server.app.routes}
    assert "/events/stream" not in paths
    assert "/agents/{name}/run" in paths


def test_serve_setup_with_broker_constructs_jobbackend(tmp_path: Path) -> None:
    """``--broker memory://`` flips the runtime's backend to JobBackend."""
    from murmur.backends.job import JobBackend

    _write_agent(tmp_path)
    server = _build_server_via_serve_path(tmp_path, broker="memory://")
    assert isinstance(server.runtime.backend, JobBackend)


def test_serve_setup_with_publish_events_opts_in(tmp_path: Path) -> None:
    from murmur.backends.job import JobBackend

    _write_agent(tmp_path)
    server = _build_server_via_serve_path(
        tmp_path, broker="memory://", publish_events=True
    )
    backend = server.runtime.backend
    assert isinstance(backend, JobBackend)
    assert backend.publish_events is True


def test_serve_setup_with_explicit_agents_filters(tmp_path: Path) -> None:
    _write_agent(tmp_path, "alpha")
    _write_agent(tmp_path, "beta")
    server = _build_server_via_serve_path(tmp_path, agents="alpha")
    # ``--agents alpha`` should register only ``alpha`` in the server's
    # internal registry — beta is on disk but excluded.
    assert "alpha" in server._agents
    assert "beta" not in server._agents
