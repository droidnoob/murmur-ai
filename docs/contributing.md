# Contributing

The canonical contributor guide lives at the project root —
[`CONTRIBUTING.md`](https://github.com/droidnoob/murmur-ai/blob/main/CONTRIBUTING.md).

## TL;DR

```bash
git clone https://github.com/droidnoob/murmur-ai && cd murmur-ai
uv sync --group dev
uv run pre-commit install
uv run pytest -m "not integration" -q     # 556 unit tests
```

Required:

- Python ≥ 3.11 (CI matrix is 3.11, 3.12, 3.13)
- [`uv`](https://github.com/astral-sh/uv) ≥ 0.11
- Docker (only for the integration test job)

## Quality gates

Every PR must pass:

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check
uv run pytest -m "not integration" -q
uv run pre-commit run --all-files
```

For docs changes, also:

```bash
uv sync --group docs
uv run mkdocs build --strict
uv run mkdocs serve                       # preview at :8000
```

## What we look for

- **Protocols, not ABCs.** New pluggable surface = `typing.Protocol` first,
  concrete second. Tests against the Protocol.
- **Type-complete.** Explicit annotations everywhere; `ty` enforces.
- **Frozen value objects.** Use `model_copy(update=…)`.
- **Domain errors.** Catch narrow, raise specific. No raw `Exception`.
- **Public-API discipline.** Users `from murmur import …` only. Nothing
  outside `murmur.interop` may import `pydantic_ai` or `faststream`.

For the full bar — including commit conventions, what we *don't* take
PRs for, and the issue-tracker (`bd`) workflow — read the canonical
guide on GitHub.

## See also

- [GitHub repository](https://github.com/droidnoob/murmur-ai)
- [Issue tracker (bd)](https://github.com/steveyegge/beads)
- [Architecture](concepts/architecture.md)
