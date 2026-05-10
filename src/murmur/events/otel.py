"""OTel metrics emitter — exports OpenTelemetry GenAI semantic conventions.

Satisfies :class:`murmur.core.protocols.events.EventEmitter` structurally
so it composes into :class:`MultiEventEmitter` alongside the default
:class:`LogEventEmitter`. Wires Murmur's :class:`RuntimeEvent` stream into
the OpenTelemetry GenAI metric histograms (`gen_ai.client.token.usage`,
`gen_ai.client.operation.duration`) plus a small set of Murmur-flavoured
counters for tool calls and typed-error rejections.

Gated behind the ``murmur-runtime[otel]`` extra. Construction without the OTel
SDK installed raises a clear :class:`ImportError` rather than failing
mid-run.

Wire the OTLP exporter externally — Murmur only emits into whichever
:class:`opentelemetry.metrics.MeterProvider` is currently set as global
(or the one the user passes in). That keeps Murmur out of the
exporter / endpoint configuration business: the same provider serves
spans, logs, and metrics to whatever sink the host operator has chosen.

>>> from murmur.events import LogEventEmitter, MultiEventEmitter, OTelMetricsEmitter
>>> from opentelemetry import metrics
>>> from opentelemetry.sdk.metrics import MeterProvider
>>> from opentelemetry.sdk.metrics.export import (
...     PeriodicExportingMetricReader,
... )
>>> from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
...     OTLPMetricExporter,
... )
>>> reader = PeriodicExportingMetricReader(OTLPMetricExporter())
>>> metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
>>> emitter = MultiEventEmitter([LogEventEmitter(), OTelMetricsEmitter()])
>>> runtime = AgentRuntime(event_emitter=emitter)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from murmur.events.types import EventType

if TYPE_CHECKING:
    from murmur.events.types import RuntimeEvent


_INSTALL_HINT = (
    "OTelMetricsEmitter requires the optional 'otel' extra — "
    "install with `pip install murmur-runtime[otel]`."
)


class OTelMetricsEmitter:
    """Emit OTel GenAI metrics from each :class:`RuntimeEvent`.

    Recorded instruments:

    - ``gen_ai.client.token.usage`` — histogram of tokens per agent run,
      attributes ``{operation, provider, request_model, token_type}``.
      ``token_type`` is always ``"total"`` because Murmur sums input +
      output tokens at the runtime level; we don't have the per-side
      breakdown without delving into PydanticAI's ``RunUsage``.
    - ``gen_ai.client.operation.duration`` — seconds per agent run,
      attributes ``{operation, provider, request_model, error_type}``.
      ``error_type`` is unset on success and the typed error-class name on
      failure (``BudgetExceededError``, ``DepthLimitError``, etc.).
    - ``murmur.tool.calls`` — counter of tool invocations; attributes
      ``{tool, agent, status}`` where ``status`` ∈ ``ok|error``.
    - ``murmur.tool.duration_ms`` — histogram of tool-call latency in ms.
    - ``murmur.rejections`` — counter for ``BUDGET_EXCEEDED`` /
      ``DEPTH_LIMIT_EXCEEDED`` / ``AGENT_FAILED`` events with a typed
      reason payload.

    Cardinality discipline: ``agent``, ``tool``, ``operation``, ``provider``,
    ``request_model`` are all expected to be bounded sets. **Do not** label
    by ``trace_id`` / ``task_id`` / prompt content — that's the cardinality
    bomb the planning doc calls out.
    """

    def __init__(
        self,
        *,
        meter_provider: Any | None = None,
    ) -> None:
        """Build the OTel instruments.

        ``meter_provider`` defaults to the globally-configured one
        (``opentelemetry.metrics.get_meter_provider()``). Pass an explicit
        provider to bind this emitter to a non-global one — useful in tests
        where multiple emitters share an :class:`InMemoryMetricReader`.

        Raises :class:`ImportError` with an install hint when the
        ``murmur-runtime[otel]`` extra isn't present.
        """
        try:
            from opentelemetry import metrics
        except ImportError as exc:  # pragma: no cover - exercised by extras test
            raise ImportError(_INSTALL_HINT) from exc

        provider = meter_provider or metrics.get_meter_provider()
        meter = provider.get_meter("murmur", "0")
        self._token_usage = meter.create_histogram(
            name="gen_ai.client.token.usage",
            description="Number of tokens used per GenAI client operation.",
            unit="{token}",
        )
        self._operation_duration = meter.create_histogram(
            name="gen_ai.client.operation.duration",
            description="Wall-clock duration of a GenAI client operation.",
            unit="s",
        )
        self._tool_calls = meter.create_counter(
            name="murmur.tool.calls",
            description="Tool invocations dispatched through Murmur's executor.",
            unit="{call}",
        )
        self._tool_duration = meter.create_histogram(
            name="murmur.tool.duration_ms",
            description="Wall-clock duration of a single tool invocation.",
            unit="ms",
        )
        self._rejections = meter.create_counter(
            name="murmur.rejections",
            description=(
                "Run-level rejections by typed reason "
                "(budget, depth, cycle, cap, timeout, trust, validation)."
            ),
            unit="{rejection}",
        )

    async def emit(self, event: RuntimeEvent) -> None:
        et = event.event_type
        if et is EventType.AGENT_COMPLETED:
            self._record_completed(event)
        elif et is EventType.AGENT_FAILED:
            self._record_failed(event)
        elif et is EventType.TOOL_CALL_COMPLETED:
            self._record_tool(event, status="ok")
        elif et is EventType.TOOL_CALL_FAILED:
            self._record_tool(event, status="error")
        elif et is EventType.BUDGET_EXCEEDED:
            self._rejections.add(1, {"reason": "budget"})
        elif et is EventType.DEPTH_LIMIT_EXCEEDED:
            self._rejections.add(1, {"reason": "depth"})

    # --------------------------------------------------------------- internals

    def _record_completed(self, event: RuntimeEvent) -> None:
        attrs = _gen_ai_attrs(event)
        tokens = _payload_int(event, "tokens_used")
        if tokens > 0:
            self._token_usage.record(tokens, {**attrs, "gen_ai.token.type": "total"})
        duration_ms = _payload_int(event, "duration_ms")
        self._operation_duration.record(duration_ms / 1000.0, attrs)

    def _record_failed(self, event: RuntimeEvent) -> None:
        attrs = _gen_ai_attrs(event)
        attrs["error.type"] = _error_class(event)
        duration_ms = _payload_int(event, "duration_ms")
        self._operation_duration.record(duration_ms / 1000.0, attrs)
        # Typed-reason rejection counter — covers SpawnCycleError,
        # SpawnCapError, TrustViolationError etc. that surface through
        # AGENT_FAILED's payload['reason']. Unknown reasons fall under
        # "unknown" so the counter still ticks, keeping the failure rate
        # observable even when the reason field is absent.
        reason = _payload_str(event, "reason") or "unknown"
        self._rejections.add(1, {"reason": reason})

    def _record_tool(self, event: RuntimeEvent, *, status: str) -> None:
        tool = _payload_str(event, "tool_name") or "unknown"
        attrs = {"tool": tool, "agent": event.agent_name, "status": status}
        self._tool_calls.add(1, attrs)
        duration_ms = _payload_int(event, "duration_ms")
        if duration_ms > 0:
            self._tool_duration.record(duration_ms, attrs)


def _gen_ai_attrs(event: RuntimeEvent) -> dict[str, str]:
    """Build the gen_ai.* attribute dict from an :class:`AGENT_*` event.

    Splits the resolved model identifier (``"provider:name"``) into
    ``gen_ai.provider.name`` + ``gen_ai.request.model``. Falls back to
    ``"unknown"`` when ``model`` is absent — keeps cardinality bounded
    while still emitting a value.
    """
    model_str = _payload_str(event, "model")
    if model_str and ":" in model_str:
        provider, _, request_model = model_str.partition(":")
    else:
        provider = "unknown"
        request_model = model_str or "unknown"
    return {
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.provider.name": provider,
        "gen_ai.request.model": request_model,
        "agent": event.agent_name,
    }


def _error_class(event: RuntimeEvent) -> str:
    err = _payload_str(event, "error")
    if not err:
        return "UnknownError"
    head, _, _ = err.partition(":")
    cleaned = head.strip()
    return cleaned or "UnknownError"


def _payload_int(event: RuntimeEvent, key: str) -> int:
    v = event.payload.get(key)
    if isinstance(v, bool):
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    return 0


def _payload_str(event: RuntimeEvent, key: str) -> str:
    v = event.payload.get(key)
    return v if isinstance(v, str) else ""


__all__ = ["OTelMetricsEmitter"]
