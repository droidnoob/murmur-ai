# Contributing

A `CONTRIBUTING.md` will live at the project root with the canonical
guide. Tracking issue: [`murmur-ai-h5n`][h5n].

[h5n]: https://github.com/murmur-ai/murmur/issues

Until then, the short version.

## Local setup

```bash
git clone https://github.com/murmur-ai/murmur && cd murmur
uv sync --group dev
uv run pre-commit install
uv run pytest -m "not integration" -q     # 556 tests, all green
```

## Quality gates

Every PR must pass:

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest -m "not integration"
uv run pre-commit run --all-files
```

CI runs the same matrix on Python 3.11, 3.12, and 3.13.

## What we look for

- **Protocols-first.** Every pluggable component is a `typing.Protocol`
  in `core/protocols/` first, concrete second. Tests run against the
  Protocol contract, not the concrete. See CLAUDE.md §2a.
- **Public API discipline.** Users `from murmur import …` only. Nothing
  outside `murmur.interop` may import `pydantic_ai` or `faststream`.
  See CLAUDE.md §2.
- **Type-complete.** Every function, method, variable annotation, and
  return type is explicit. `ty` enforces this. No `Any` without an
  inline comment explaining why.
- **Frozen value objects.** Specs and value objects are frozen Pydantic
  models. Update via `model_copy(update=…)`, never mutate.
- **Domain errors.** Never raise raw `Exception` from core code. Catch
  narrow, raise specific. See CLAUDE.md §16.
- **Tests mirror source.** `src/murmur/foo.py` ↔ `tests/test_foo.py`.
  Coverage ≥ 80% on `core/`, 100% on every Protocol method.

## Commits

Conventional Commits, prefix matches directory:

```
feat(core):     add spawn depth limit enforcement
fix(backends):  handle timeout in thread backend
chore(ci):      add ty check to pipeline
docs:           update concepts/runtime.md
test(tools):    add property-based tests for tool resolution
refactor(ctx):  extract empty-context guard
```

Don't reference internal phase / addendum / issue identifiers in commit
messages or code comments — keep those in `.planning/` and `bd`.

## Building the docs

```bash
uv sync --group docs
uv run mkdocs serve
```

Then open `http://localhost:8000`. The site rebuilds on every save.
