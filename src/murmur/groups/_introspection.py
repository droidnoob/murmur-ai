"""Discover the :data:`FanOut`-annotated field on a Pydantic ``output_type``.

The group runner uses this to figure out which list to split over when an
edge has no explicit ``mapper``. Validation rules:

- Exactly one field may carry the marker, or none.
- The annotated type must be ``list[T]`` or ``list[T1 | T2 | ...]``. Not
  tuple, not set, not bare ``T``.
- ``SpecValidationError`` (or its ``TopologyError`` subclass) on violation.

The returned ``item_types`` is always a tuple. Single-type fan-outs come
back as a one-element tuple; union fan-outs come back as a tuple of every
union member in declaration order. Heterogeneous routing (each list item
goes to the downstream whose ``Agent.input_type`` matches its exact type)
relies on the multi-element form.

Internal — leading underscore filename, never re-exported.
"""

from __future__ import annotations

import types as _types
from typing import Annotated, Any, Union, get_args, get_origin, get_type_hints

from murmur.core.errors import SpecValidationError
from murmur.types import _FanOutMarker


def _flatten_union(item_type: type[Any]) -> tuple[type[Any], ...]:
    """Return the union members for ``item_type``, or ``(item_type,)`` for scalars.

    Handles both ``Union[A, B]`` (typing form) and ``A | B`` (PEP 604
    ``types.UnionType``). Preserves declaration order. Removes duplicates
    that ``typing.get_args`` may surface for trivially-collapsing unions
    (``T | T`` → ``(T,)``).
    """
    origin = get_origin(item_type)
    if origin is Union or origin is _types.UnionType:
        seen: list[type[Any]] = []
        for member in get_args(item_type):
            if member not in seen:
                seen.append(member)
        return tuple(seen)
    return (item_type,)


def get_fan_out_field(
    model: type[Any],
) -> tuple[str, tuple[type[Any], ...]] | None:
    """Return ``(field_name, item_types)`` for the model's fan-out field.

    Returns ``None`` if the model has no :data:`FanOut`-annotated field.
    ``item_types`` is always a tuple — length 1 for ``FanOut[list[T]]``
    and length N for ``FanOut[list[T1 | T2 | ... | TN]]``.

    Raises :class:`SpecValidationError` if more than one field carries
    the marker, or if the annotated type is not ``list[T]``.
    """
    hints = get_type_hints(model, include_extras=True)
    matches: list[tuple[str, tuple[type[Any], ...]]] = []
    for name, hint in hints.items():
        if get_origin(hint) is not Annotated:
            continue
        args = get_args(hint)
        if len(args) < 2:
            continue
        inner = args[0]
        metadata = args[1:]
        if not any(isinstance(m, _FanOutMarker) for m in metadata):
            continue
        if get_origin(inner) is not list:
            raise SpecValidationError(
                f"FanOut field {model.__name__}.{name} must wrap list[T]; got {inner!r}"
            )
        list_args = get_args(inner)
        if not list_args:
            raise SpecValidationError(
                f"FanOut field {model.__name__}.{name} must specify a list "
                f"item type, e.g. FanOut[list[SubQuestion]]"
            )
        matches.append((name, _flatten_union(list_args[0])))

    if not matches:
        return None
    if len(matches) > 1:
        names = ", ".join(n for n, _ in matches)
        raise SpecValidationError(
            f"{model.__name__} has multiple FanOut fields ({names}); "
            f"only one is allowed"
        )
    return matches[0]


__all__ = ["get_fan_out_field"]
