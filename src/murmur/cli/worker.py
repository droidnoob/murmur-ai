"""``murmur worker start`` — launch a distributed consumer."""

from __future__ import annotations

import argparse


def register_worker(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = sub.add_parser("worker", help="Manage Murmur workers.")
    wsub = p.add_subparsers(dest="worker_command", required=True)

    start = wsub.add_parser("start", help="Start a worker.")
    start.add_argument(
        "--agents",
        required=True,
        help="Comma-separated agent names this worker should consume.",
    )
    start.add_argument(
        "--broker",
        required=True,
        help="Broker URL (kafka://host:port, nats://, amqp://, redis://).",
    )
    start.add_argument("--concurrency", type=int, default=10)
    start.add_argument("--prefetch", type=int, default=5)
    start.set_defaults(handler=_start)


def _start(args: argparse.Namespace) -> int:
    raise NotImplementedError(
        f"murmur worker start --agents {args.agents!r} --broker {args.broker!r} "
        f"— Phase 1 stub"
    )


__all__ = ["register_worker"]
