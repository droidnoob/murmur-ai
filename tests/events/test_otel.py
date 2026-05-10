"""Unit tests for :class:`OTelMetricsEmitter`.

Each test builds its own :class:`MeterProvider` wired to an
:class:`InMemoryMetricReader`, passes that provider to the emitter, then
asserts on the recorded data points. No global state, no exporter, no
network calls — entirely in-process.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from murmur.events import OTelMetricsEmitter
from murmur.events.types import EventType, RuntimeEvent


def _provider() -> tuple[MeterProvider, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]), reader


def _completed(
    *,
    agent: str = "researcher",
    tokens: int = 120,
    duration_ms: int = 250,
    model: str = "anthropic:claude-sonnet-4-6",
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={
            "tokens_used": tokens,
            "duration_ms": duration_ms,
            "backend": "thread",
            "model": model,
        },
    )


def _failed(
    *,
    agent: str = "researcher",
    duration_ms: int = 80,
    error: str = "BudgetExceededError: ran out of tokens",
    reason: str = "budget",
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.AGENT_FAILED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={
            "duration_ms": duration_ms,
            "error": error,
            "reason": reason,
            "backend": "thread",
        },
    )


def _tool_completed(
    *, agent: str = "researcher", tool: str = "search", duration_ms: int = 35
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.TOOL_CALL_COMPLETED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={"tool_name": tool, "duration_ms": duration_ms, "tokens_used": 0},
    )


def _tool_failed(
    *, agent: str = "researcher", tool: str = "search", duration_ms: int = 12
) -> RuntimeEvent:
    return RuntimeEvent(
        event_type=EventType.TOOL_CALL_FAILED,
        agent_name=agent,
        trace_id="t",
        timestamp=datetime.now(tz=UTC),
        payload={"tool_name": tool, "duration_ms": duration_ms, "error": "boom"},
    )


def _gather_metrics(reader: InMemoryMetricReader) -> dict[str, Any]:
    """Flatten the reader's data into ``{metric_name: [(value, attrs), ...]}``."""
    data = reader.get_metrics_data()
    out: dict[str, list[tuple[Any, dict[str, str]]]] = {}
    if data is None:
        return out
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                rows = out.setdefault(metric.name, [])
                for point in metric.data.data_points:
                    raw_attrs = point.attributes or {}
                    attrs = {str(k): str(v) for k, v in raw_attrs.items()}
                    if hasattr(point, "sum"):
                        rows.append((point.sum, attrs))
                    else:
                        rows.append((point.value, attrs))
    return out


# ---------------------------------------------------------------------------
# AGENT_COMPLETED → token usage + operation duration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_completed_records_token_usage_histogram() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_completed(tokens=120))

    metrics = _gather_metrics(reader)
    assert "gen_ai.client.token.usage" in metrics
    [(total, attrs)] = metrics["gen_ai.client.token.usage"]
    assert total == 120
    assert attrs["gen_ai.operation.name"] == "invoke_agent"
    assert attrs["gen_ai.provider.name"] == "anthropic"
    assert attrs["gen_ai.request.model"] == "claude-sonnet-4-6"
    assert attrs["gen_ai.token.type"] == "total"
    assert attrs["agent"] == "researcher"


@pytest.mark.asyncio
async def test_agent_completed_records_operation_duration_histogram() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_completed(duration_ms=2500))

    metrics = _gather_metrics(reader)
    [(total_seconds, attrs)] = metrics["gen_ai.client.operation.duration"]
    assert total_seconds == pytest.approx(2.5)
    assert attrs["gen_ai.provider.name"] == "anthropic"
    assert "error.type" not in attrs


@pytest.mark.asyncio
async def test_agent_completed_zero_tokens_does_not_record_usage() -> None:
    """Histogram should stay empty when token count is 0 — keeps the
    cardinality contract clean. Duration still records."""
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_completed(tokens=0))

    metrics = _gather_metrics(reader)
    assert metrics.get("gen_ai.client.token.usage", []) == []
    assert "gen_ai.client.operation.duration" in metrics


@pytest.mark.asyncio
async def test_agent_completed_unknown_model_falls_back_safely() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    bare = RuntimeEvent(
        event_type=EventType.AGENT_COMPLETED,
        agent_name="a",
        trace_id="t",
        payload={"tokens_used": 10, "duration_ms": 5, "backend": "thread"},
    )
    await emitter.emit(bare)

    [(_, attrs)] = _gather_metrics(reader)["gen_ai.client.token.usage"]
    assert attrs["gen_ai.provider.name"] == "unknown"
    assert attrs["gen_ai.request.model"] == "unknown"


# ---------------------------------------------------------------------------
# AGENT_FAILED → operation duration with error.type + rejection counter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_failed_tags_error_type_and_records_duration() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_failed(error="DepthLimitError: too deep", reason="depth"))

    metrics = _gather_metrics(reader)
    [(_, dur_attrs)] = metrics["gen_ai.client.operation.duration"]
    assert dur_attrs["error.type"] == "DepthLimitError"
    [(rej_total, rej_attrs)] = metrics["murmur.rejections"]
    assert rej_total == 1
    assert rej_attrs["reason"] == "depth"


@pytest.mark.asyncio
async def test_agent_failed_without_reason_payload_falls_back_to_unknown() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    ev = RuntimeEvent(
        event_type=EventType.AGENT_FAILED,
        agent_name="a",
        trace_id="t",
        payload={"duration_ms": 10, "error": "RuntimeError: boom", "backend": "thread"},
    )
    await emitter.emit(ev)

    [(_, attrs)] = _gather_metrics(reader)["murmur.rejections"]
    assert attrs["reason"] == "unknown"


# ---------------------------------------------------------------------------
# TOOL_CALL_* → tool counter + tool duration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_completed_records_counter_and_duration() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_tool_completed(tool="search", duration_ms=42))

    metrics = _gather_metrics(reader)
    [(calls, call_attrs)] = metrics["murmur.tool.calls"]
    assert calls == 1
    assert call_attrs["tool"] == "search"
    assert call_attrs["status"] == "ok"
    [(dur_total, dur_attrs)] = metrics["murmur.tool.duration_ms"]
    assert dur_total == 42
    assert dur_attrs["status"] == "ok"


@pytest.mark.asyncio
async def test_tool_call_failed_uses_error_status() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(_tool_failed(tool="search"))

    metrics = _gather_metrics(reader)
    [(_, attrs)] = metrics["murmur.tool.calls"]
    assert attrs["status"] == "error"


# ---------------------------------------------------------------------------
# Rejection counters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_budget_exceeded_increments_rejection_counter() -> None:
    provider, reader = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)

    await emitter.emit(
        RuntimeEvent(
            event_type=EventType.BUDGET_EXCEEDED,
            agent_name="a",
            trace_id="t",
            payload={"limit": 100, "used": 120, "scope": "task"},
        )
    )
    await emitter.emit(
        RuntimeEvent(
            event_type=EventType.DEPTH_LIMIT_EXCEEDED,
            agent_name="a",
            trace_id="t",
            payload={"limit": 5, "depth": 6},
        )
    )

    rejections = _gather_metrics(reader)["murmur.rejections"]
    by_reason = {a["reason"]: total for total, a in rejections}
    assert by_reason["budget"] == 1
    assert by_reason["depth"] == 1


# ---------------------------------------------------------------------------
# Composability — OTelMetricsEmitter satisfies the EventEmitter Protocol
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_satisfies_event_emitter_protocol_structurally() -> None:
    from murmur.core.protocols.events import EventEmitter

    provider, _ = _provider()
    emitter = OTelMetricsEmitter(meter_provider=provider)
    assert isinstance(emitter, EventEmitter)


@pytest.mark.asyncio
async def test_composes_into_multi_event_emitter() -> None:
    from murmur.events import MultiEventEmitter

    provider, reader = _provider()
    captured: list[RuntimeEvent] = []

    class _Capture:
        async def emit(self, event: RuntimeEvent) -> None:
            captured.append(event)

    multi = MultiEventEmitter([_Capture(), OTelMetricsEmitter(meter_provider=provider)])
    await multi.emit(_completed())

    # Captured AND recorded — both sinks fire from one emit call.
    assert len(captured) == 1
    assert "gen_ai.client.token.usage" in _gather_metrics(reader)


# ---------------------------------------------------------------------------
# Missing-extra path
# ---------------------------------------------------------------------------


def test_construction_without_otel_extra_raises_clear_importerror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the [otel] extra isn't installed, constructing the emitter
    must surface an actionable ImportError pointing the operator at the
    install command — not a cryptic ModuleNotFoundError mid-run."""
    import builtins
    import importlib
    import sys

    # Drop any cached opentelemetry modules so the monkeypatched import
    # fires on next access.
    for mod in list(sys.modules):
        if mod == "opentelemetry" or mod.startswith("opentelemetry."):
            sys.modules.pop(mod, None)

    real_import = builtins.__import__

    def _no_otel(name: str, *args: object, **kwargs: object) -> object:
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ImportError("simulated missing extra")
        # forwarding *args to stdlib __import__ — ty can't see through *object
        return real_import(name, *args, **kwargs)  # ty: ignore[invalid-argument-type]

    monkeypatch.setattr(builtins, "__import__", _no_otel)

    # Force a fresh import of the otel module so the new __import__ shim
    # is in effect when its top-level imports run.
    sys.modules.pop("murmur.events.otel", None)
    otel_mod = importlib.import_module("murmur.events.otel")

    with pytest.raises(ImportError, match=r"murmur-runtime\[otel\]"):
        otel_mod.OTelMetricsEmitter()
