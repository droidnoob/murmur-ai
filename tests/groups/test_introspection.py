"""Unit tests for ``murmur.groups._introspection.get_fan_out_field``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from murmur.core.errors import SpecValidationError
from murmur.groups._introspection import get_fan_out_field
from murmur.types import FanOut


class _Item(BaseModel):
    x: int


class _OneFanOut(BaseModel):
    items: FanOut[list[_Item]]
    note: str = ""


class _NoFanOut(BaseModel):
    items: list[_Item]


class _TwoFanOut(BaseModel):
    a: FanOut[list[_Item]]
    b: FanOut[list[_Item]]


class _ScalarFanOut(BaseModel):
    x: FanOut[int]  # type: ignore[misc]  # invalid by design — must be list[T]


def test_returns_field_name_and_item_type() -> None:
    found = get_fan_out_field(_OneFanOut)
    assert found is not None
    name, item_type = found
    assert name == "items"
    assert item_type is _Item


def test_no_fan_out_returns_none() -> None:
    assert get_fan_out_field(_NoFanOut) is None


def test_multiple_fan_out_raises() -> None:
    with pytest.raises(SpecValidationError, match="multiple FanOut"):
        get_fan_out_field(_TwoFanOut)


def test_non_list_fan_out_raises() -> None:
    with pytest.raises(SpecValidationError, match="must wrap list"):
        get_fan_out_field(_ScalarFanOut)
