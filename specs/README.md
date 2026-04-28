# specs/

YAML definitions for agents, groups, and (Phase 3) workflows. Loaded by `YamlRegistry` and validated by `murmur validate specs/`.

YAML and the Python SDK are two representations of the **same canonical spec** — every YAML file round-trips through Python and back. Use whichever is more convenient; the runtime does not care.

## Layout

```
specs/
    agents/        # one file per Agent — referenced by name in code or other specs
    groups/        # AgentGroup definitions (Phase 3)
    workflows/     # YAML workflows with stages + Jinja templating (Phase 3)
```

## File naming

The filename (without extension) **must match** the spec's `name` field. `specs/agents/researcher.yaml` defines an Agent with `name: researcher`. `murmur validate` rejects mismatches.

## Example: an agent

```yaml
# specs/agents/researcher.yaml
name: researcher
model: anthropic:claude-sonnet-4-6
trust_level: medium
context_passer: "null"
backend: auto

instructions: |
  You are a research agent. Given a topic, return a structured
  summary with sources.

output_schema:
  type: object
  required: [summary, sources, confidence]
  properties:
    summary:    { type: string }
    sources:    { type: array, items: { type: string } }
    confidence: { type: number, minimum: 0.0, maximum: 1.0 }

tools:
  - web_search
```

Resolve and run from Python:

```python
from murmur import AgentRuntime, TaskSpec
from murmur.registry import YamlRegistry

registry = YamlRegistry(root="specs")
runtime = AgentRuntime(registry=registry)

result = await runtime.run("researcher", TaskSpec(input="..."))
```

## Validation

```bash
uv run murmur validate specs/
```

Prints `[ok]` / `[error]` per file and exits non-zero on any error. Wire this into pre-commit if your team relies on YAML specs.

## Phase status

- **Phase 1:** `agents/` only. The Phase 1 `YamlRegistry` is a stub — wiring lands as part of phase-1-mvp work item #5 (canonical YAML schema + round-trip).
- **Phase 3:** `groups/` and `workflows/` come online with `AgentGroup`, coordination strategies, and the workflow runner.
