"""``murmur status`` — tail-style live view of an MurmurServer's event stream.

Reads ``GET /events/stream`` from a running ``murmur serve`` (or any
embedded :class:`AgentServer` exposing the SSE route) and prints each
:class:`RuntimeEvent` to stdout in a one-line format. Useful in CI logs,
when SSH'd into a server, or as a quick sanity probe before pointing
the React dashboard at the same endpoint.

Thin client — no separate process. Holds the SSE connection open until
Ctrl-C; reconnects on transient network errors with a small backoff.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from contextlib import suppress
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


_DEFAULT_URL = "http://127.0.0.1:8420/events/stream"
_RECONNECT_DELAY_SECONDS = 2.0


def register_status(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = sub.add_parser(
        "status",
        help="Tail an AgentServer's /events/stream over SSE.",
        description=(
            "Open an SSE connection to a Murmur server and print each "
            "RuntimeEvent as it arrives. Reconnects on dropped connection."
        ),
    )
    parser.add_argument(
        "--url",
        default=_DEFAULT_URL,
        help=f"SSE endpoint URL (default: {_DEFAULT_URL}).",
    )
    parser.add_argument(
        "--filter-event-type",
        action="append",
        default=[],
        metavar="EVENT_TYPE",
        help=(
            "Only show events of this type (e.g. agent_failed). Repeatable. "
            "If unset, shows every event."
        ),
    )
    parser.add_argument(
        "--filter-agent",
        action="append",
        default=[],
        metavar="AGENT_NAME",
        help="Only show events for this agent_name. Repeatable.",
    )
    parser.add_argument(
        "--no-reconnect",
        action="store_true",
        help="Exit instead of reconnecting on a dropped connection.",
    )
    parser.set_defaults(handler=_handler)


def _handler(args: argparse.Namespace) -> int:
    try:
        return asyncio.run(_run_status(args))
    except KeyboardInterrupt:
        return 0


async def _run_status(args: argparse.Namespace) -> int:
    try:
        import httpx
    except ImportError:
        sys.stderr.write(
            "murmur status requires httpx. Install via: "
            "pip install 'murmur-ai[server]'\n"
        )
        return 2

    type_filter = frozenset(args.filter_event_type)
    agent_filter = frozenset(args.filter_agent)

    while True:
        try:
            async with (
                httpx.AsyncClient(timeout=None) as client,
                client.stream("GET", args.url) as response,
            ):
                if response.status_code != 200:
                    sys.stderr.write(
                        f"unexpected status {response.status_code} from {args.url}\n"
                    )
                    return 1
                sys.stderr.write(f"connected to {args.url}\n")
                async for event in _parse_sse_frames(response.aiter_lines()):
                    if type_filter and event.get("event_type") not in type_filter:
                        continue
                    if agent_filter and event.get("agent_name") not in agent_filter:
                        continue
                    sys.stdout.write(_format_event(event) + "\n")
                    sys.stdout.flush()
        except httpx.HTTPError as exc:
            sys.stderr.write(f"connection error: {exc}\n")
            if args.no_reconnect:
                return 1
            sys.stderr.write(f"reconnecting in {_RECONNECT_DELAY_SECONDS}s…\n")
            with suppress(asyncio.CancelledError):
                await asyncio.sleep(_RECONNECT_DELAY_SECONDS)


async def _parse_sse_frames(
    lines: AsyncIterator[str],
) -> AsyncIterator[dict[str, Any]]:
    """Decode SSE-framed JSON payloads from ``aiter_lines()``.

    Yields one dict per ``data:`` line whose body parses as a JSON object.
    Handles the SSE event/data/blank-line framing; ignores comment lines
    and ``event: ping`` heartbeats. Reads the ``event:`` line as the event
    type when present, falling back to a payload-embedded ``event_type``.
    """
    current_event_type: str | None = None
    async for raw in lines:
        line = raw.rstrip("\r")
        if not line:
            current_event_type = None
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            current_event_type = line[len("event:") :].strip()
            continue
        if line.startswith("data:"):
            payload = line[len("data:") :].strip()
            if not payload:
                continue
            try:
                body = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if not isinstance(body, dict):
                continue
            if current_event_type and "event_type" not in body:
                body["event_type"] = current_event_type
            yield body


def _format_event(event: dict[str, Any]) -> str:
    """One-line tail-style render. Empty fields are skipped."""
    parts: list[str] = []
    et = event.get("event_type", "?")
    parts.append(str(et))
    if agent := event.get("agent_name"):
        parts.append(f"agent={agent}")
    if task := event.get("task_id"):
        parts.append(f"task={task}")
    if trace := event.get("trace_id"):
        parts.append(f"trace={trace}")
    payload = event.get("payload")
    if isinstance(payload, dict) and payload:
        tail = ", ".join(f"{k}={v}" for k, v in payload.items())
        parts.append(f"[{tail}]")
    return " ".join(parts)


__all__ = ["register_status"]
