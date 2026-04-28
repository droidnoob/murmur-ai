"""Tests for the default ``AlwaysSingleRouter``."""

from __future__ import annotations

from murmur.core.protocols.router import RouteDecision
from murmur.routing.always_single import AlwaysSingleRouter
from murmur.types import TaskSpec


async def test_classify_returns_single_for_any_task() -> None:
    router = AlwaysSingleRouter()
    decision = await router.classify(TaskSpec(input="anything"))
    assert decision is RouteDecision.SINGLE
