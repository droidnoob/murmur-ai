"""Discover the :data:`FanOut`-annotated field on a Pydantic ``output_type``.

The group runner uses this to figure out which list to split over when an
edge has no explicit ``mapper``. Validation rules (Phase 3 spec):

- Exactly one field may carry the marker, or none.
- The annotated type must be ``list[T]``. Not tuple, not set, not bare ``T``.
- ``SpecValidationError`` (or its ``TopologyError`` subclass) on violation.

Internal — leading underscore filename, never re-exported.
"""

from __future__ import annotations

from typing import Annotated, Any, get_args, get_origin, get_type_hints

from murmur.core.errors import SpecValidationError
from murmur.types import _FanOutMarker


def get_fan_out_field(model: type[Any]) -> tuple[str, type[Any]] | None:
    """Return ``(field_name, item_type)`` for the model's fan-out field.

    Returns ``None`` if the model has no :data:`FanOut`-annotated field.
    Raises :class:`SpecValidationError` if more than one field carries the
    marker, or if the annotated type is not ``list[T]``.
    """
    hints = get_type_hints(model, include_extras=True)
    matches: list[tuple[str, type[Any]]] = []
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
        matches.append((name, list_args[0]))

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
