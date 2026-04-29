"""Wire-format errors and HTTP status mapping.

The server's exception handler converts any :class:`MurmurError` (or
unhandled exception) into an :class:`ErrorResponse` with a deterministic
status code. The client's reverse mapping turns the body back into the
matching typed exception so user code catches the same class whether it
runs locally or against a remote server.

This module is import-safe even without ``fastapi`` installed — the actual
exception handler binding happens in :mod:`murmur.server.app`.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from murmur.core.errors import (
    AllAgentsFailedError,
    BudgetExceededError,
    ContextError,
    DepthLimitError,
    MurmurError,
    RegistryError,
    SpawnError,
    SpecValidationError,
    ToolExecutionError,
    TopologyError,
    TrustViolationError,
)


class ErrorResponse(BaseModel):
    """Wire shape returned by the server on any non-2xx response."""

    model_config = ConfigDict(frozen=True)

    error: str
    """Class name of the raised error — e.g. ``"BudgetExceededError"``."""

    message: str
    """Human-readable detail."""

    agent: str | None = None
    """Which agent failed, if applicable."""

    task_id: str | None = None
    """Which task failed, if applicable."""

    request_id: str
    """Always present for correlation across logs / traces."""


ERROR_STATUS_MAP: dict[type[MurmurError], int] = {
    SpawnError: 500,
    ToolExecutionError: 500,
    ContextError: 500,
    AllAgentsFailedError: 500,
    BudgetExceededError: 429,
    DepthLimitError: 429,
    TrustViolationError: 403,
    SpecValidationError: 400,
    TopologyError: 400,
    RegistryError: 404,
}
"""Map a domain error class to its HTTP status. Defaults to 500 otherwise."""


_NAME_TO_CLASS: dict[str, type[MurmurError]] = {
    cls.__name__: cls for cls in ERROR_STATUS_MAP
}
_NAME_TO_CLASS["MurmurError"] = MurmurError


def status_for(exc: BaseException) -> int:
    """Return the HTTP status code for an exception. ``500`` if unmapped."""
    if isinstance(exc, TimeoutError):
        return 504
    for cls, code in ERROR_STATUS_MAP.items():
        if isinstance(exc, cls):
            return code
    return 500


def error_to_response(exc: BaseException, *, request_id: str) -> ErrorResponse:
    """Build an :class:`ErrorResponse` from any exception."""
    return ErrorResponse(
        error=type(exc).__name__,
        message=str(exc),
        agent=getattr(exc, "agent", None),
        task_id=getattr(exc, "task_id", None),
        request_id=request_id,
    )


def response_to_error(response: ErrorResponse) -> MurmurError:
    """Reverse mapping: recreate the typed exception client-side.

    Unknown error names fall back to :class:`MurmurError`.
    """
    cls = _NAME_TO_CLASS.get(response.error, MurmurError)
    return cls(response.message)


__all__ = [
    "ERROR_STATUS_MAP",
    "ErrorResponse",
    "error_to_response",
    "response_to_error",
    "status_for",
]
