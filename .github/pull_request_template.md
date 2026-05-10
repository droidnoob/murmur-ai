<!--
Thanks for opening a PR. Fill in the sections that apply, delete those
that don't. The CI suite (lint + type + unit on py3.11/3.12/3.13 plus
the docs build) runs automatically.
-->

## Summary

<!-- One paragraph: what changes, and why. Reviewers read this first. -->

## Type of change

- [ ] Bug fix
- [ ] Feature
- [ ] Refactor (no behaviour change)
- [ ] Docs only
- [ ] Build / CI / dependency update
- [ ] Breaking change

## Touched surfaces

- [ ] Public API (`from murmur import ...`) — covered in `src/murmur/__init__.py`
- [ ] Wire format (`AgentServer` HTTP routes, broker envelopes, MCP)
- [ ] Protocols in `src/murmur/core/protocols/` — adding or changing one
- [ ] Concrete that implements a Protocol — runs the shared contract suite
- [ ] Examples in `examples/`
- [ ] Docs in `docs/`

## Verification

- [ ] `uv run pytest -m "not integration" -q` — all green
- [ ] `uv run ruff check .` and `uv run ty check` — clean
- [ ] `uv run mkdocs build --strict` — clean (only required if docs touched)

## Notes for reviewers

<!-- Anything tricky? Tradeoffs you weighed? Existing tests you intentionally
changed? Mention it here. -->
