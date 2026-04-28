"""Unit tests for ``murmur.server.errors`` — wire shape + status mapping."""

from __future__ import annotations

import pytest

from murmur.core.errors import (
    BudgetExceededError,
    MurmurError,
    RegistryError,
    SpawnError,
    SpecValidationError,
    TopologyError,
    TrustViolationError,
)
from murmur.server.errors import (
    ErrorResponse,
    error_to_response,
    response_to_error,
    status_for,
)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (SpawnError("x"), 500),
        (BudgetExceededError("x"), 429),
        (TrustViolationError("x"), 403),
        (SpecValidationError("x"), 400),
        (TopologyError("x"), 400),
        (RegistryError("x"), 404),
        (TimeoutError("x"), 504),
        (RuntimeError("unmapped"), 500),
    ],
)
def test_status_for(exc: BaseException, expected: int) -> None:
    assert status_for(exc) == expected


def test_error_to_response_carries_request_id() -> None:
    resp = error_to_response(SpawnError("boom"), request_id="req-1")
    assert isinstance(resp, ErrorResponse)
    assert resp.error == "SpawnError"
    assert resp.message == "boom"
    assert resp.request_id == "req-1"


def test_response_to_error_round_trips_typed_class() -> None:
    resp = ErrorResponse(error="BudgetExceededError", message="over", request_id="r")
    err = response_to_error(resp)
    assert isinstance(err, BudgetExceededError)
    assert str(err) == "over"


def test_response_to_error_unknown_falls_back_to_murmur_error() -> None:
    resp = ErrorResponse(error="WhoKnows", message="?", request_id="r")
    err = response_to_error(resp)
    assert isinstance(err, MurmurError)
    assert type(err) is MurmurError
