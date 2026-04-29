"""Shared serialization helpers for persistent ``RunStore`` concretes.

The on-wire / on-disk shape is a primitive JSON dict — same envelope idea
as :class:`murmur.messages.ResultMessage`. The original Pydantic class
identity of ``AgentResult.output`` is intentionally **not** preserved:
the stored JSON dict is rehydrated as a generic ``extra="allow"`` model
so callers can keep using ``output.model_dump()`` without the store
knowing about the user's output_type class path.

Only :class:`AgentResult` is serialised here. Value types
(:class:`RunStatus`, :class:`RunProgress`, :class:`RunEvent`) already
have ``model_dump_json`` round-trips via Pydantic.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict

from murmur.types import AgentResult, ResultMetadata


class _RehydratedOutput(BaseModel):
    """Generic envelope used when rehydrating ``AgentResult.output``.

    Stores the original payload as model fields via ``extra="allow"`` so
    ``model_dump()`` returns the verbatim JSON dict the runtime stored.
    """

    model_config = ConfigDict(extra="allow", frozen=True)


def encode_result(result: AgentResult[BaseModel]) -> str:
    """Serialise an :class:`AgentResult` to a JSON string."""
    payload: dict[str, Any] = {
        "agent_name": result.agent_name,
        "task_id": result.task_id,
        "metadata": result.metadata.model_dump(),
        "output": result.output.model_dump() if result.output is not None else None,
        "error": str(result.error) if result.error is not None else None,
    }
    return json.dumps(payload)


def decode_result(blob: str) -> AgentResult[BaseModel]:
    """Inverse of :func:`encode_result` — produces a typed-shape envelope."""
    payload = json.loads(blob)
    output_dict: dict[str, Any] | None = payload.get("output")
    output = (
        _RehydratedOutput.model_validate(output_dict)
        if output_dict is not None
        else None
    )
    error_msg: str | None = payload.get("error")
    error: BaseException | None = RuntimeError(error_msg) if error_msg else None
    metadata = ResultMetadata.model_validate(payload.get("metadata") or {})
    return AgentResult[BaseModel](
        output=output,
        error=error,
        metadata=metadata,
        agent_name=payload["agent_name"],
        task_id=payload["task_id"],
    )


__all__ = ["decode_result", "encode_result"]
