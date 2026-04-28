"""``murmur run`` — execute a user script with Murmur on the path."""

from __future__ import annotations

import argparse


def register_run(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = sub.add_parser("run", help="Run a Python script that uses murmur.")
    p.add_argument("script", help="Path to a Python script.")
    p.set_defaults(handler=_run)


def _run(args: argparse.Namespace) -> int:
    raise NotImplementedError(f"murmur run {args.script!r} — Phase 1 stub")


__all__ = ["register_run"]
