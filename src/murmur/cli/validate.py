"""``murmur validate`` — validate every YAML spec under a directory."""

from __future__ import annotations

import argparse
from pathlib import Path


def register_validate(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = sub.add_parser("validate", help="Validate YAML specs in a directory.")
    p.add_argument("path", type=Path, help="Directory containing specs.")
    p.set_defaults(handler=_validate)


def _validate(args: argparse.Namespace) -> int:
    raise NotImplementedError(f"murmur validate {args.path!s} — Phase 1 stub")


__all__ = ["register_validate"]
