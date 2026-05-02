"""Concurrency-limit primitives — re-exported from PydanticAI under the public API.

Users cap provider-side HTTP request concurrency by passing one of these to
:attr:`murmur.Agent.max_concurrent_requests` or
:attr:`murmur.Agent.model_concurrency_limiter`. Re-exporting here keeps the
Public API Rule intact (CLAUDE.md §2): user code only ever imports from
``murmur``.

>>> from murmur.models import ConcurrencyLimiter
>>> pool = ConcurrencyLimiter(max_running=10, name="openai-pool")

The convenience int knob (``Agent(max_concurrent_requests=5)``) constructs a
limiter implicitly per agent. Use a shared :class:`ConcurrencyLimiter` when
several agents share one provider key and need to share one cap.

:class:`ConcurrencyLimit` carries an optional ``max_queued`` for backpressure;
:class:`AbstractConcurrencyLimiter` is the base class for custom limiters
(e.g. a Redis-backed cross-process limiter). ``Agent``'s field type accepts
:class:`AbstractConcurrencyLimiter`, so user-defined subclasses plug in
without further integration work.
"""

from __future__ import annotations

from pydantic_ai.concurrency import (
    AbstractConcurrencyLimiter,
    ConcurrencyLimit,
    ConcurrencyLimiter,
)

__all__ = [
    "AbstractConcurrencyLimiter",
    "ConcurrencyLimit",
    "ConcurrencyLimiter",
]
