"""Shared serialization helpers for persistent ``RunStore`` concretes.

The on-wire / on-disk shape is a primitive JSON dict — same envelope idea
as :class:`murmur.messages.ResultMessage`. The original Pydantic class
identity of ``AgentResult.output`` is intentionally **not** preserved:
the stored JSON dict is rehydrated as a generic ``extra="allow"`` model
so callers can keep using ``output.model_dump()`` without the store
knowing about the user's output_type class path.

Both :class:`AgentResult` (single) and :class:`GroupResult` (multi-leaf)
round-trip here. The encoded blob carries a ``"kind"`` discriminator —
``"agent"`` for single-leaf, ``"group"`` for multi-leaf — so the decoder
returns the right shape without the caller specifying it. Value types
(:class:`RunStatus`, :class:`RunProgress`, :class:`RunEvent`) already
have ``model_dump_json`` round-trips via Pydantic.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict

from murmur.types import AgentResult, GroupResult, ResultMetadata


class _RehydratedOutput(BaseModel):
    """Generic envelope used when rehydrating ``AgentResult.output``.

    Stores the original payload as model fields via ``extra="allow"`` so
    ``model_dump()`` returns the verbatim JSON dict the runtime stored.
    """

    model_config = ConfigDict(extra="allow", frozen=True)


def _encode_agent_result(result: AgentResult[BaseModel]) -> dict[str, Any]:
    return {
        "kind": "agent",
        "agent_name": result.agent_name,
        "task_id": result.task_id,
        "metadata": result.metadata.model_dump(),
        "output": result.output.model_dump() if result.output is not None else None,
        "error": str(result.error) if result.error is not None else None,
    }


def _decode_agent_result(payload: dict[str, Any]) -> AgentResult[BaseModel]:
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


def encode_result(result: AgentResult[BaseModel] | GroupResult) -> str:
    """Serialise an :class:`AgentResult` or :class:`GroupResult` to JSON."""
    if isinstance(result, GroupResult):
        encoded_outputs = {
            name: _encode_agent_result(leaf) for name, leaf in result.outputs.items()
        }
        payload: dict[str, Any] = {
            "kind": "group",
            "outputs": encoded_outputs,
            "metadata": result.metadata.model_dump(),
        }
        return json.dumps(payload)
    return json.dumps(_encode_agent_result(result))


def decode_result(blob: str) -> AgentResult[BaseModel] | GroupResult:
    """Inverse of :func:`encode_result` — discriminates on the ``"kind"`` field.

    Blobs encoded by older versions (no ``"kind"`` field) are treated as
    ``"agent"`` shape — backward compatible with stores that pre-date
    ``GroupResult`` support.
    """
    payload = json.loads(blob)
    kind = payload.get("kind", "agent")
    if kind == "group":
        outputs = {
            name: _decode_agent_result(leaf)
            for name, leaf in payload["outputs"].items()
        }
        metadata = ResultMetadata.model_validate(payload.get("metadata") or {})
        return GroupResult(outputs=outputs, metadata=metadata)
    return _decode_agent_result(payload)


__all__ = ["decode_result", "encode_result"]
