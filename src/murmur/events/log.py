"""LogEventEmitter — the always-on default emitter (structlog backend).

Forwards each :class:`RuntimeEvent` to structlog at INFO level (or ERROR
for the failure variants) so observability lands in whatever sink the
host has configured for stdlib / structlog. The event's ``payload`` is
flattened into the log entry's keyword args; ``timestamp`` /
``trace_id`` / ``parent_trace_id`` / ``agent_name`` / ``task_id`` are
emitted as top-level fields so log-search backends can filter on them.

Wired in by default — see :class:`murmur.AgentRuntime`. Combine with
:class:`MultiEventEmitter` to add SSE / custom sinks alongside.

Implementation notes:

- The emit path is ``await log.ainfo(...)`` / ``await log.aerror(...)``
  so it's safe inside async code without blocking the event loop.
- We deliberately do **not** raise from emit — a logging sink that
  raises would take an agent run down with it. Errors during emission
  are swallowed and logged via structlog's own error handling.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from murmur.events.types import RuntimeEvent


_FAILURE_TYPES: frozenset[str] = frozenset(
    {
        "agent_failed",
        "tool_call_failed",
        "budget_exceeded",
        "depth_limit_exceeded",
    }
)
"""Event types routed to ``log.aerror`` instead of ``log.ainfo``.

Lets log search filter on level alone for the "something went wrong" view
without parsing event_type strings.
"""


class LogEventEmitter:
    """Default :class:`EventEmitter` — writes via structlog.

    Stateless and trivially safe to share across runtimes / threads /
    runs. Construction takes no arguments; the bound logger is resolved
    per-call so ``structlog.testing.capture_logs`` can intercept emit
    output even after the CLI has set ``cache_logger_on_first_use=True``.
    """

    async def emit(self, event: RuntimeEvent) -> None:
        log = structlog.get_logger()
        method = log.aerror if event.event_type.value in _FAILURE_TYPES else log.ainfo
        # Last-resort guard: a custom structlog processor pipeline that
        # raises must not take an agent run down with it.
        with contextlib.suppress(Exception):  # pragma: no cover
            await method(
                event.event_type.value,
                timestamp=event.timestamp.isoformat(),
                agent_name=event.agent_name,
                task_id=event.task_id,
                trace_id=event.trace_id,
                parent_trace_id=event.parent_trace_id,
                **event.payload,
            )


__all__ = ["LogEventEmitter"]
