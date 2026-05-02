"""Smoke test — every file in ``examples/`` imports cleanly.

Catches breakage when a refactor changes the public API surface in a way
that an example file's imports or top-level code no longer compile. We
import each file as a module via ``importlib.util.spec_from_file_location``
so missing optional deps (``starlette``, ``uvicorn``, ``httpx`` for the
embedded / dashboard examples) surface as a clear ``pytest.skip`` rather
than a hard failure.

Each example's ``main()`` is **not** invoked — that would dispatch real
LLM calls. We only verify the module loads.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"

# Examples whose top-level imports require optional extras. If the import
# fails with ModuleNotFoundError on one of these names, we skip rather than
# fail — the example is still well-formed, just needs the extra installed.
_OPTIONAL_DEPS_BY_EXAMPLE: dict[str, frozenset[str]] = {
    "embedded.py": frozenset({"httpx", "fastapi", "uvicorn"}),
    "events_dashboard.py": frozenset({"starlette", "uvicorn"}),
}


def _example_files() -> list[Path]:
    return sorted(p for p in _EXAMPLES_DIR.glob("*.py") if not p.name.startswith("_"))


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_imports(path: Path) -> None:
    spec = importlib.util.spec_from_file_location(f"_example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        optional = _OPTIONAL_DEPS_BY_EXAMPLE.get(path.name, frozenset())
        if exc.name in optional:
            pytest.skip(
                f"{path.name} requires optional dep {exc.name!r}; install with extras"
            )
        raise
    finally:
        sys.modules.pop(spec.name, None)


def test_examples_directory_is_not_empty() -> None:
    """If someone deletes the directory, surface that loudly."""
    assert _example_files(), (
        "examples/ has no .py files — directory may have been removed"
    )
