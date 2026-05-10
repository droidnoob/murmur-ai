# Contributing to Murmur

Thanks for your interest. Murmur is **infrastructure** — a runtime for LLM
agents — and the bar for changes is "make the runtime more reliable, more
observable, or more capable without introducing new public surface unless
absolutely necessary." If you're considering a contribution, skim this
file plus [`CLAUDE.md`](./CLAUDE.md) (the canonical conventions doc) before
opening a PR.

---

## Local setup

```bash
git clone https://github.com/murmur-runtime/murmur && cd murmur
uv sync --group dev                      # core deps + dev tools
uv run pre-commit install                # one-time per clone
uv run pytest -m "not integration" -q    # 556 unit tests
```

Required tools:

- **Python ≥ 3.11.** CI matrix is 3.11, 3.12, 3.13.
- **[uv](https://github.com/astral-sh/uv) ≥ 0.11.** Project uses uv
  exclusively for environment + dependency management.
- **Docker** (optional, only for the integration test job).

## Quality gates

Every PR must pass these locally and in CI:

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check
uv run pytest -m "not integration" -q
uv run pre-commit run --all-files
```

CI runs the same matrix on Python 3.11 / 3.12 / 3.13. The integration job
(`run-integration` label or `workflow_dispatch`) spins up real Kafka, NATS,
RabbitMQ, Redis containers via `testcontainers` and runs the
`@pytest.mark.integration` suite.

For docs changes:

```bash
uv sync --group docs
uv run mkdocs build --strict             # CI runs this on every PR
uv run mkdocs serve                      # local preview at :8000
```

## What to read first

In order:

1. [`CLAUDE.md`](./CLAUDE.md) — the project rulebook. Sections to read
   before writing code:
   - **§2 Public API rule** — only `from murmur import …`. Never import
     `pydantic_ai` or `faststream` outside `murmur.interop`.
   - **§2a Protocols-first** — every pluggable component is a
     `typing.Protocol` in `core/protocols/` first, concrete second.
   - **§11 Project structure** — `core/` never imports from sibling
     packages; arrows point inward.
   - **§13 SOLID/DRY/YAGNI** — extract on the third occurrence; no ABCs
     until two concretes ship now; structural typing over inheritance.
   - **§20 Hard rules** — no `print`, no `time.sleep` in async, no
     `**kwargs` in public APIs, no wildcard imports.
   - **§21a Documentation** — when a change touches public API, observable
     behaviour, or a documented decision, update the matching
     `docs/api/`, `docs/concepts/`, or `docs/guides/` page in the same PR.
2. [`docs/concepts/architecture.md`](./docs/concepts/architecture.md) —
   the pipeline + middleware mental model.
3. The Protocol you intend to extend — under `src/murmur/core/protocols/`.

## What we look for

- **Protocols, not ABCs.** Add a `typing.Protocol` first; concretes match
  by shape. No inheritance for code reuse.
- **Type-complete.** Every function, method, return type explicit. `ty`
  enforces this in CI. No `Any` without an inline comment explaining why.
- **Frozen value objects.** Specs and value types use
  `model_config = ConfigDict(frozen=True)`. Update via `model_copy`.
- **Domain errors.** Catch narrow, raise specific. Core code never raises
  raw `Exception` or `ValueError`.
- **Tests mirror source.** `src/murmur/foo.py` ↔ `tests/test_foo.py`.
  Coverage ≥ 80% on `core/`, 100% on every Protocol method. New
  Protocols ship with a shared contract suite under `tests/contracts/`.
- **No public-API leaks.** Adding a field that exposes a `pydantic_ai`
  type? Wrap it. Adding a parameter that takes a `KafkaBroker`? Take a URL
  string instead.

## Commits

Conventional Commits, prefix matches directory:

```
feat(core):     add spawn depth limit enforcement
fix(backends):  handle timeout in thread backend
chore(ci):      add ty check to pipeline
docs(claude):   update §2a wording
test(tools):    add property-based tests for tool resolution
refactor(ctx):  extract empty-context guard
```

**Don't** reference internal phase / addendum / issue identifiers in
commit messages or code comments — keep `Phase N`, `#24b`,
`Addendum 3` etc. in `.planning/` and `bd`. The commit history reads
better when those are out of it.

## Issue tracker

We use [`bd`](https://github.com/steveyegge/beads) (beads) for task
tracking. Run `bd ready` to see what's open and unblocked, `bd show <id>`
for detail. Don't use external task lists — `bd` is the source of truth
for in-flight work.

## Sending a PR

1. Fork & branch from `main`.
2. Make the change. Keep PRs small and focused — one bead, one PR.
3. Run the full quality gate locally (`pre-commit run --all-files` covers
   most of it).
4. Update docs alongside code per CLAUDE.md §21a.
5. Open the PR with a description that links the bead id (if you have
   one) and explains the *why* in 2–3 sentences.
6. CI will run lint + type + unit on Python 3.11/3.12/3.13. Integration
   tests run on demand via the `run-integration` label.

## What we *don't* want PRs for

CLAUDE.md §22 has the canonical list. The short version:

- Re-implementing things PydanticAI / FastStream already do.
- Auth / user management on the server.
- A custom logging framework (use `structlog`).
- Plugin systems / dynamic loading (Python imports are sufficient).
- Agent memory / RAG / vector store integration (out of Murmur's role).
- Anything in a future phase (Container backend, smart context passers,
  workflow engine) before the prior phase's foundation is shipped.

When in doubt, open an issue first — a few sentences in `bd` save a wasted
PR.

## License

`TBD` until the project picks one before v0.1. Until then, contributions
are made under the same terms as the eventual licence; we'll re-confirm
with contributors before the first tagged release.
