"""Domain-error hierarchy tests."""

from __future__ import annotations

import pytest

from murmur.core.errors import (
    BudgetExceededError,
    ContextError,
    DepthLimitError,
    MurmurError,
    RegistryError,
    SpawnError,
    SpecValidationError,
    ToolExecutionError,
    TrustViolationError,
)

ALL_ERRORS = [
    SpawnError,
    ToolExecutionError,
    ContextError,
    BudgetExceededError,
    DepthLimitError,
    SpecValidationError,
    RegistryError,
    TrustViolationError,
]


@pytest.mark.parametrize("err_cls", ALL_ERRORS)
def test_each_error_inherits_murmur_error(err_cls: type[Exception]) -> None:
    assert issubclass(err_cls, MurmurError)


def test_chaining_preserves_cause() -> None:
    src = ValueError("root")
    try:
        try:
            raise src
        except ValueError as e:
            raise SpawnError("wrapped") from e
    except SpawnError as outer:
        assert outer.__cause__ is src
