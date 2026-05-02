"""``murmur`` CLI entry point.

Subcommands:

- ``murmur run <script.py>`` — execute a Python script with the runtime in scope
- ``murmur validate <specs/>`` — validate every YAML spec under a directory
- ``murmur worker start --agents X --broker URL`` — start a distributed consumer
- ``murmur serve --port 8420`` — HTTP server for registered agents + live event SSE

``main`` is the ``[project.scripts] murmur`` entry point: ``murmur …`` runs
:func:`main` with the parsed arguments. Tests drive the same function via
``main(argv=[...])`` and assert on captured stdout / exit code.
"""

from __future__ import annotations

import argparse
import logging
import sys

import structlog

from murmur.cli.run import register_run
from murmur.cli.serve import register_serve
from murmur.cli.status import register_status
from murmur.cli.validate import register_validate
from murmur.cli.worker import register_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="murmur",
        description="Murmur — agents that move as one.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level for structlog output.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    register_run(sub)
    register_validate(sub)
    register_worker(sub)
    register_serve(sub)
    register_status(sub)
    return parser


def _configure_logging(level: str) -> None:
    """One-shot ``structlog`` setup for CLI invocations.

    Every log entry from the runtime carries ``agent_name`` / ``task_id`` /
    ``backend`` / ``trust_level`` / ``request_id`` already; this binds the
    rendering pipeline so they actually surface.
    """
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    handler = args.handler  # set by each register_* function
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["build_parser", "main"]
