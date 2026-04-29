"""Shared helper for sync-API entry points (#27).

``run_sync`` / ``gather_sync`` on :class:`murmur.AgentRuntime` and
``run_sync`` on :class:`murmur_client.MurmurClient` /
:class:`murmur_client.LocalClient` all wrap :func:`asyncio.run`. That
function refuses to start when there's already a running event loop,
but its built-in error is opaque (``RuntimeError: asyncio.run() cannot
be called from a running event loop``) and tells the caller nothing
about how to fix it. We catch the case eagerly with a pointer to the
async variant.

The helper lives at module scope (not in ``runtime.py``) so the
``murmur-client`` package can re-use it without reaching into a
sibling's underscore-prefixed surface.
"""

from __future__ import annotations

import asyncio


def reject_if_in_event_loop(method_name: str) -> None:
    """Raise if the caller is inside a running asyncio loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        f"{method_name} called from a running event loop; use the async variant instead"
    )


__all__ = ["reject_if_in_event_loop"]
