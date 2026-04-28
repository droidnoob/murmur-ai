"""``murmur`` CLI entry point.

Phase 1 surface:

- ``murmur run <script.py>`` — execute a Python script with the runtime in scope
- ``murmur validate <specs/>`` — validate every YAML spec under a directory
- ``murmur worker start --agents X`` — start a distributed consumer

Anything else (``serve``, ``workflow``, ``status``) is a later phase.
"""

from __future__ import annotations

import argparse
import sys

from murmur.cli.run import register_run
from murmur.cli.validate import register_validate
from murmur.cli.worker import register_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="murmur",
        description="Murmur — agents that move as one.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    register_run(sub)
    register_validate(sub)
    register_worker(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = args.handler  # set by each register_* function
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["build_parser", "main"]
