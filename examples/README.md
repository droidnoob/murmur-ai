# Murmur examples

Each file is a self-contained, runnable program. Set `ANTHROPIC_API_KEY`
(or the appropriate provider key) and run with `python examples/<name>.py`.

| File | One-liner | Concept |
|---|---|---|
| [`quickstart.py`](quickstart.py) | Single agent, thread mode, structured output | [`runtime.md`](../docs/concepts/runtime.md), [`agents.md`](../docs/concepts/agents.md) |
| [`distributed.py`](distributed.py) | Same agent over an in-process broker + Worker | [`backends.md`](../docs/concepts/backends.md), [distributed guide](../docs/guides/distributed.md) |
| [`embedded.py`](embedded.py) | Murmur mounted into a FastAPI app via `AgentRouter` | [embedded guide](../docs/guides/embedded.md), [`server.md`](../docs/api/server.md) |
| [`cost_budget.py`](cost_budget.py) | `TokenBudget` exhaustion + `BUDGET_EXCEEDED` event | [`cost.md`](../docs/concepts/cost.md) |
| [`events_dashboard.py`](events_dashboard.py) | `SSEEventEmitter` wired to a Starlette `/events/stream` | [`events.md`](../docs/concepts/events.md) |
| [`mcp.py`](mcp.py) | Agent consuming an MCP server via `mcp_stdio` + `allow=` | [`mcp.md`](../docs/concepts/mcp.md) |
| [`mcp_server.py`](mcp_server.py) | Expose a Murmur agent as an MCP tool via `AgentServer.serve_mcp` (stdio or HTTP) | [`mcp.md`](../docs/concepts/mcp.md) |
| [`agent_team.py`](agent_team.py) | Coordinator + 2 typed delegates via `AgentTeam` and the auto-generated `delegate(...)` tool | [`coordination.md` — AgentTeam](../docs/concepts/coordination.md) |
| [`worker_concurrency.py`](worker_concurrency.py) | Two competing `Worker`s tuned with `concurrency=` / `prefetch=` splitting a 32-task burst | [`backends.md`](../docs/concepts/backends.md), [distributed guide](../docs/guides/distributed.md) |
| [`spawn_agents.py`](spawn_agents.py) | Orchestrator delegates to children via `spawn_agents` | [`agents.md` — Templates + fan-out](../docs/concepts/agents.md#templates--shared-config-across-a-fleet) |
| [`memory_via_tool.py`](memory_via_tool.py) | Cross-run memory pattern: persistence via two tools | [`coordination.md`](../docs/concepts/coordination.md) |

## Running

```bash
# from the repo root
uv sync
export ANTHROPIC_API_KEY=...
uv run python examples/quickstart.py
```

`quickstart.py` and `distributed.py` use the cheap Haiku model
(`anthropic:claude-haiku-4-5-20251001`) by default so first-run cost
stays small. Swap `model="..."` for any
[PydanticAI-supported model string](https://ai.pydantic.dev/models/).

## Tests

`tests/test_examples_smoke.py` imports every file in this directory at
collection time, so an example that breaks against the current public
API surfaces in CI as a failed import.
