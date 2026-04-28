"""``murmur run <script.py>`` — execute a Python script.

Convenience wrapper for ``uv run python <script.py>`` that ensures
``murmur`` is importable. The script runs in its own ``__main__`` module
so it sees ``__name__ == "__main__"`` exactly like a direct interpreter
invocation. Pass ``--`` followed by extra args to forward them to the
script via ``sys.argv``.

CTRL-C is caught at the top level so async runtimes inside the script
get a chance to cancel cleanly.
"""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path


def register_run(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = sub.add_parser(
        "run",
        help="Run a Python script that uses murmur.",
    )
    p.add_argument(
        "script",
        type=Path,
        help="Path to a Python script.",
    )
    p.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Args forwarded to the script via sys.argv. "
        "Prefix with ``--`` to disambiguate from murmur's own flags.",
    )
    p.set_defaults(handler=_run)


def _run(args: argparse.Namespace) -> int:
    script: Path = args.script
    if not script.is_file():
        print(f"[error] script not found: {script}", file=sys.stderr)
        return 2

    # Strip a leading ``--`` separator so users can ``murmur run script.py -- --foo``.
    forwarded = list(args.script_args)
    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    saved_argv = sys.argv
    sys.argv = [str(script), *forwarded]
    try:
        runpy.run_path(str(script), run_name="__main__")
    except KeyboardInterrupt:
        return 130  # POSIX convention: 128 + SIGINT
    except SystemExit as exc:
        # Honour the script's exit code if it called sys.exit(...).
        return int(exc.code) if isinstance(exc.code, int) else (1 if exc.code else 0)
    finally:
        sys.argv = saved_argv
    return 0


__all__ = ["register_run"]
