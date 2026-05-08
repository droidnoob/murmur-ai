"""``murmur serve`` — standalone HTTP server for registered agents + events.

Boots an :class:`murmur.server.AgentServer` over a YAML-discovered set of
agents, optionally connected to a broker. Adds a live ``GET /events/stream``
firehose by wiring a :class:`SSEEventEmitter` into the runtime's emitter
chain (paired with :class:`LogEventEmitter` via :class:`MultiEventEmitter`
so structured logs keep flowing alongside).

CLI shape::

    murmur serve --specs ./specs                                    # local in-process
    murmur serve --specs ./specs --broker kafka://host:9092         # broker dispatch
    murmur serve --specs ./specs --broker kafka://… --publish-events
                                                                    # also receive
                                                                    # worker-side
                                                                    # events through
                                                                    # the bridge

``--publish-events`` requires ``--broker`` — without a broker there's no
fleet to receive events from. ``--specs`` defaults to ``./specs`` to match
``murmur worker start``; ``--all-from`` mirrors the worker's bulk-discovery
flag so a single command can register every YAML-defined agent.

Pairs with ``zxn.3.2`` (``murmur status``) — that command is the SSE
consumer side of this firehose.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from murmur.core.errors import RegistryError, SpecValidationError
from murmur.registry.yaml import YamlRegistry

if TYPE_CHECKING:
    from murmur.agent import Agent

log: structlog.stdlib.BoundLogger = structlog.get_logger()


def register_serve(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = sub.add_parser("serve", help="Run the Murmur HTTP server with live event SSE.")
    selection = p.add_mutually_exclusive_group()
    selection.add_argument(
        "--agents",
        help=(
            "Comma-separated agent names to expose. Default: every agent under --specs."
        ),
    )
    selection.add_argument(
        "--all-from",
        type=Path,
        metavar="PATH",
        help=(
            "Discover every agent under the given specs root and "
            "register all of them. Mutually exclusive with --agents."
        ),
    )
    p.add_argument(
        "--specs",
        type=Path,
        default=Path("./specs"),
        help="Directory containing YAML agent specs (default: ./specs).",
    )
    p.add_argument(
        "--broker",
        default=None,
        help=(
            "Broker URL (kafka://host:port, nats://, amqp://, redis://, or "
            "memory://). Omit to run in in-process — no distributed dispatch."
        ),
    )
    p.add_argument(
        "--publish-events",
        action="store_true",
        help=(
            "Subscribe to the per-runtime broker events topic and surface "
            "worker-side RuntimeEvents through GET /events/stream. Requires "
            "--broker."
        ),
    )
    p.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0).")
    p.add_argument("--port", type=int, default=8420, help="Bind port (default: 8420).")
    p.add_argument(
        "--heartbeat-interval",
        type=float,
        default=15.0,
        help=(
            "SSE keepalive interval in seconds (default: 15). Lower values "
            "tolerate aggressive proxy idle timeouts."
        ),
    )
    p.add_argument(
        "--no-events",
        action="store_true",
        help="Disable the GET /events/stream endpoint entirely.",
    )
    p.add_argument(
        "--dashboard-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Mount a built dashboard static bundle at /dashboard. Pass the "
            "directory containing index.html (e.g. packages/dashboard/dist). "
            "Off by default — exposure is opt-in."
        ),
    )
    p.add_argument(
        "--event-store",
        default=None,
        metavar="PATH_OR_MEMORY",
        help=(
            "Persist RuntimeEvents for the dashboard. Pass a SQLite path "
            "(events.db) for durable history, or 'memory' for an in-process "
            "ring. Adds GET /runs, /runs/{id}/tree, /events, /usage. Off by "
            "default. Combine with --broker --publish-events to also capture "
            "worker-fleet events relayed through the broker bridge."
        ),
    )
    p.add_argument(
        "--reload",
        action="store_true",
        help=(
            "Watch --reload-dir paths and restart the server on file changes. "
            "Requires the [reload] extra (watchfiles). Dev-only — production "
            "deployments should not use this."
        ),
    )
    p.add_argument(
        "--reload-dir",
        type=Path,
        action="append",
        default=None,
        metavar="PATH",
        help=(
            "Directory to watch for changes. May be repeated. Default: "
            "--specs and the current working directory."
        ),
    )
    p.add_argument(
        "--reload-include",
        action="append",
        default=None,
        metavar="GLOB",
        help=(
            "Filename glob to include in reload watching (default: *.py, "
            "*.yaml, *.yml). May be repeated."
        ),
    )
    p.add_argument(
        "--reload-exclude",
        action="append",
        default=None,
        metavar="GLOB",
        help="Filename glob to exclude from reload watching. May be repeated.",
    )
    p.add_argument(
        "--uvloop",
        action="store_true",
        help=(
            "Use uvloop for the asyncio event loop (POSIX only). Requires "
            "the [uvloop] extra. Equivalent to MURMUR_USE_UVLOOP=1. Falls "
            "back to the default loop with a warning on Windows or when "
            "the extra isn't installed."
        ),
    )
    p.set_defaults(handler=_start)


def _start(args: argparse.Namespace) -> int:
    if getattr(args, "reload", False):
        from murmur.cli._reload import is_reload_child, reload_wrap

        if not is_reload_child():
            reload_dirs = args.reload_dir or [args.specs, Path.cwd()]
            return reload_wrap(
                reload_dirs=reload_dirs,
                includes=args.reload_include,
                excludes=args.reload_exclude,
            )
    from murmur.cli._uvloop import install_uvloop_policy, resolve_uvloop_enabled

    install_uvloop_policy(resolve_uvloop_enabled(getattr(args, "uvloop", False)))
    return asyncio.run(_run_serve(args))


async def _run_serve(args: argparse.Namespace) -> int:
    if args.publish_events and not args.broker:
        print(
            "[error] --publish-events requires --broker (no fleet without a broker)",
            file=sys.stderr,
        )
        return 2

    specs_root: Path = args.all_from if args.all_from is not None else args.specs

    try:
        registry = YamlRegistry(specs_root)
    except RegistryError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    errors = registry.validate()
    if errors:
        for err in errors:
            print(f"[error] {err}", file=sys.stderr)
        return 1

    agents = _resolve_agents(args, registry)
    if agents is None:
        return 2

    # Lazy imports — keep `murmur run` / `murmur validate` startup lean and
    # avoid pulling FastAPI / uvicorn / sse-starlette in for every CLI use.
    from murmur.core.protocols.event_store import EventStore
    from murmur.core.protocols.events import EventEmitter
    from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter
    from murmur.runtime import AgentRuntime
    from murmur.server.app import AgentServer

    sse_emitter: SSEEventEmitter | None = None
    sinks: list[EventEmitter] = [LogEventEmitter()]
    if not args.no_events:
        sse_emitter = SSEEventEmitter(heartbeat_interval=args.heartbeat_interval)
        sinks.append(sse_emitter)

    event_store: EventStore | None = None
    if args.event_store is not None:
        from murmur.events.store import (
            InMemoryEventStore,
            SQLiteEventStore,
            StoreEventEmitter,
        )

        if args.event_store == "memory":
            event_store = InMemoryEventStore()
        else:
            sqlite_store = SQLiteEventStore(path=args.event_store)
            await sqlite_store.start_pruning()
            event_store = sqlite_store
        sinks.append(StoreEventEmitter(event_store))

    emitter = MultiEventEmitter(sinks)

    try:
        runtime = AgentRuntime(
            broker=args.broker,
            event_emitter=emitter,
            publish_events=args.publish_events,
        )
    except SpecValidationError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    dashboard_dir: Path | None = args.dashboard_dir
    # Startup-time existence check before binding the server — sync I/O is fine here.
    if dashboard_dir is not None and (
        not dashboard_dir.is_dir()  # noqa: ASYNC240
        or not (dashboard_dir / "index.html").is_file()  # noqa: ASYNC240
    ):
        print(
            f"[error] --dashboard-dir {dashboard_dir} does not contain index.html",
            file=sys.stderr,
        )
        return 2

    server = AgentServer(
        runtime=runtime,
        sse_emitter=sse_emitter,
        dashboard_dir=dashboard_dir,
        event_store=event_store,
    )
    for agent in agents.values():
        server.register(agent)

    await log.ainfo(
        "serve_starting",
        host=args.host,
        port=args.port,
        broker=args.broker,
        publish_events=args.publish_events,
        agents=sorted(agents),
        events_endpoint=("/events/stream" if sse_emitter is not None else None),
        dashboard=str(dashboard_dir) if dashboard_dir is not None else None,
        event_store=args.event_store,
    )
    await server.serve(host=args.host, port=args.port)
    return 0


def _resolve_agents(
    args: argparse.Namespace, registry: YamlRegistry
) -> dict[str, Agent] | None:
    """Pick the agent set to register. Same flag semantics as ``worker start``.

    Returns ``None`` (after printing to stderr) on misconfiguration so the
    caller can return the right exit code.
    """
    if args.all_from is not None:
        names = sorted(registry.list())
        if not names:
            print(f"[error] no agents found under {args.all_from}", file=sys.stderr)
            return None
    elif args.agents is not None:
        # Explicit ``--agents`` (even empty string) takes the explicit
        # branch — passing it should never silently fall through to
        # "register everything" or we'd boot a server with surprise
        # endpoints.
        names = [n.strip() for n in args.agents.split(",") if n.strip()]
        if not names:
            print("[error] --agents is empty", file=sys.stderr)
            return None
    else:
        # No flag → register every agent under --specs.
        names = sorted(registry.list())
        if not names:
            print(f"[error] no agents found under {args.specs}", file=sys.stderr)
            return None

    try:
        return {name: registry.get(name) for name in names}
    except RegistryError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return None


__all__ = ["register_serve"]
