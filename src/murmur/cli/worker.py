"""``murmur worker start`` — launch a distributed consumer.

Loads agents from a YAML registry, connects to the named broker, runs
:class:`murmur.worker.worker.Worker` until SIGTERM / SIGINT. The worker's
inner runtime is always thread-mode (per the worker docstring: a
broker-mode runtime would re-publish tasks).

Discovery currently goes through ``YamlRegistry`` rooted at ``--specs``
(default ``./specs``). Phase 3's workflow engine can extend this.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
from pathlib import Path

import structlog

from murmur.core.errors import RegistryError, SpecValidationError
from murmur.registry.yaml import YamlRegistry

log: structlog.stdlib.BoundLogger = structlog.get_logger()


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
        help="Broker URL (kafka://host:port, nats://, amqp://, redis://, "
        "or memory:// for single-process mode).",
    )
    start.add_argument(
        "--specs",
        type=Path,
        default=Path("./specs"),
        help="Directory containing YAML agent specs (default: ./specs).",
    )
    start.add_argument("--concurrency", type=int, default=10)
    start.add_argument("--prefetch", type=int, default=5)
    start.set_defaults(handler=_start)


def _start(args: argparse.Namespace) -> int:
    return asyncio.run(_run_worker(args))


async def _run_worker(args: argparse.Namespace) -> int:
    names = [n.strip() for n in args.agents.split(",") if n.strip()]
    if not names:
        print("[error] --agents is empty", file=sys.stderr)
        return 2

    try:
        registry = YamlRegistry(args.specs)
    except RegistryError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    errors = registry.validate()
    if errors:
        for err in errors:
            print(f"[error] {err}", file=sys.stderr)
        return 1

    try:
        agents = {name: registry.get(name) for name in names}
    except RegistryError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    # Construct the worker's inner runtime (no broker) and the broker-side
    # wrapper. Lazy imports so non-worker CLI invocations don't pay the cost.
    from murmur.runtime import AgentRuntime
    from murmur.worker.worker import Worker

    try:
        publisher_runtime = AgentRuntime(broker=args.broker)
    except SpecValidationError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    worker_runtime = AgentRuntime()  # ThreadBackend internally

    # Reach into the publisher runtime to recover the constructed Broker
    # and reuse it for the worker. AgentRuntime exposes ``backend`` and the
    # JobBackend exposes its broker via the private ``_broker`` attribute —
    # this is the one place the CLI bridges the two.
    worker_broker = getattr(publisher_runtime.backend, "_broker", None)
    if worker_broker is None:
        print(
            f"[error] runtime did not produce a Broker for url {args.broker!r}; "
            "is this a thread-mode URL?",
            file=sys.stderr,
        )
        return 2

    worker = Worker(
        broker=worker_broker,
        agents=agents,
        runtime=worker_runtime,
        concurrency=args.concurrency,
        prefetch=args.prefetch,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, ValueError):  # Windows-safe
            loop.add_signal_handler(sig, stop_event.set)

    await worker.start()
    await log.ainfo(
        "worker_cli_running",
        agents=list(agents),
        broker=args.broker,
        concurrency=args.concurrency,
    )
    try:
        await stop_event.wait()
    finally:
        await log.ainfo("worker_cli_shutting_down")
        await worker.stop()
    return 0


__all__ = ["register_worker"]
