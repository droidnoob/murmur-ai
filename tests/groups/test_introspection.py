"""Unit tests for ``murmur.groups._introspection.get_fan_out_field``."""

from __future__ import annotations

from typing import Union

import pytest
from pydantic import BaseModel

from murmur.core.errors import SpecValidationError
from murmur.groups._introspection import get_fan_out_field
from murmur.types import FanOut


class _Item(BaseModel):
    x: int


class _OtherItem(BaseModel):
    y: str


class _ThirdItem(BaseModel):
    z: float


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


class _UnionPipe(BaseModel):
    """PEP 604 ``A | B`` syntax — the user-facing form for heterogeneous fan-out."""

    items: FanOut[list[_Item | _OtherItem | _ThirdItem]]


class _UnionTyping(BaseModel):
    """``typing.Union[A, B]`` form — equivalent shape, different origin."""

    items: FanOut[list[Union[_Item, _OtherItem]]]  # noqa: UP007 — exercising typing form


class _RedundantUnion(BaseModel):
    """``T | T`` deduplicates to a one-tuple."""

    items: FanOut[list[_Item | _Item]]  # type: ignore[misc]


def test_returns_field_name_and_singleton_tuple_for_single_type() -> None:
    found = get_fan_out_field(_OneFanOut)
    assert found is not None
    name, item_types = found
    assert name == "items"
    assert item_types == (_Item,)


def test_no_fan_out_returns_none() -> None:
    assert get_fan_out_field(_NoFanOut) is None


def test_multiple_fan_out_raises() -> None:
    with pytest.raises(SpecValidationError, match="multiple FanOut"):
        get_fan_out_field(_TwoFanOut)


def test_non_list_fan_out_raises() -> None:
    with pytest.raises(SpecValidationError, match="must wrap list"):
        get_fan_out_field(_ScalarFanOut)


def test_union_pipe_returns_all_members_in_declaration_order() -> None:
    found = get_fan_out_field(_UnionPipe)
    assert found is not None
    name, item_types = found
    assert name == "items"
    assert item_types == (_Item, _OtherItem, _ThirdItem)


def test_union_typing_form_normalises_to_same_shape() -> None:
    """``typing.Union[A, B]`` and ``A | B`` produce identical introspection
    output — the runner doesn't care which syntax the user wrote.
    """
    found = get_fan_out_field(_UnionTyping)
    assert found is not None
    _name, item_types = found
    assert item_types == (_Item, _OtherItem)


def test_redundant_union_deduplicates_to_singleton_tuple() -> None:
    """``T | T`` collapses to a single-type fan-out."""
    found = get_fan_out_field(_RedundantUnion)
    assert found is not None
    _name, item_types = found
    assert item_types == (_Item,)
