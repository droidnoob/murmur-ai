"""``murmur validate <dir>`` — validate every YAML spec under a directory.

Walks ``<dir>/agents/*.yaml`` and runs each through :class:`YamlRegistry`
validation. Prints one line per file (``[ok]`` or ``[error] file: msg``)
and exits non-zero if any spec fails. Empty directories are treated as
``[ok]`` — there's nothing to invalidate.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from murmur.core.errors import RegistryError
from murmur.registry.yaml import YamlRegistry


def register_validate(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    p = sub.add_parser("validate", help="Validate YAML specs in a directory.")
    p.add_argument(
        "path",
        type=Path,
        help="Directory containing an ``agents/`` subdirectory of *.yaml specs.",
    )
    p.set_defaults(handler=_validate)


def _validate(args: argparse.Namespace) -> int:
    path: Path = args.path
    try:
        registry = YamlRegistry(path)
    except RegistryError as exc:
        print(f"[error] {path}: {exc}", file=sys.stderr)
        return 2

    errors = registry.validate()
    if errors:
        for err in errors:
            print(f"[error] {err}", file=sys.stderr)
        print(
            f"\n{len(errors)} error(s) across {len(registry.list())} loaded agent(s).",
            file=sys.stderr,
        )
        return 1

    names = sorted(registry.list())
    if not names:
        print(f"[ok] {path}: no agents to validate.")
        return 0
    for name in names:
        print(f"[ok] {name}")
    print(f"\n{len(names)} agent(s) validated.")
    return 0


__all__ = ["register_validate"]
