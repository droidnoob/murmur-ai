# Embedded mode

Mount Murmur inside a user-supplied FastAPI app. No separate process.
The same agents, the same observability, but routed through your
application's middleware stack and lifecycle.

## Bootstrap

```bash
uv init my-fastapi-app
cd my-fastapi-app
uv add 'murmur-ai[server]' fastapi httpx uvicorn
export ANTHROPIC_API_KEY=...
```

A working end-to-end script — same `Agent` as the
[Quickstart](../getting-started/quickstart.md), mounted via
`AgentRouter`, exercised both over HTTP (httpx against the in-process
ASGI app) and via the in-process `LocalClient` — lives at
[`examples/embedded.py`](https://github.com/murmur-ai/murmur/blob/main/examples/embedded.py).

## Why embed

- You already run a FastAPI service and want agent endpoints alongside.
- You want shared auth, logging, request IDs across application + agent
  routes.
- You want to control the lifespan (e.g. share a DB connection pool).

Standalone (`murmur serve`) is the right answer when Murmur owns the
server. Embedded is the right answer when your application does.

## `AgentRouter`

```python
from fastapi import FastAPI
from murmur import Agent, AgentRuntime
from murmur.server import AgentRouter
from murmur_client import LocalClient

runtime = AgentRuntime()
runtime.register(researcher)
runtime.register(reviewer)

router = AgentRouter(runtime=runtime)

app = FastAPI(lifespan=router.lifespan)
AgentRouter.install_exception_handlers(app)
app.include_router(router, prefix="/agents")
```

`AgentRouter` is an `APIRouter` subclass — you mount it on your app like
any other router. The lifespan calls `runtime.shutdown()` automatically
on exit, releasing MCP subprocesses and broker connections.

`AgentRouter.install_exception_handlers(app)` is a classmethod that
wires Murmur's domain errors to the HTTP status codes in
`server/errors.py` (each error type maps to a stable HTTP status). It's
a separate one-liner because it modifies the host app, not the router.

## Routes the router adds

| Method | Path | Body | Returns |
|---|---|---|---|
| `GET` | `/agents` | – | List of registered agent names |
| `GET` | `/agents/{name}/schema` | – | JSON schema for the agent's input/output |
| `POST` | `/agents/{name}/run` | `TaskSpec` JSON | `AgentResult` JSON |
| `POST` | `/agents/{name}/gather` | `{tasks: [TaskSpec, ...], max_concurrency: int}` | `list[AgentResult]` JSON |
| `GET` | `/groups` | – | List of registered group names |
| `GET` | `/groups/{name}/topology` | – | Group topology metadata |
| `POST` | `/groups/{name}/run` | `TaskSpec` JSON | `AgentResult` or `GroupResult` JSON (see below) |
| `POST` | `/submit` | `SubmitRequest` JSON | `{run_id}` |
| `GET` | `/runs/{run_id}/status` | – | `RunStatus` |
| `GET` | `/runs/{run_id}/result` | – | `AgentResult` or `GroupResult` JSON |
| `GET` | `/runs/{run_id}/stream` | – | SSE stream of run events |
| `POST` | `/runs/{run_id}/cancel` | – | `204` |
| `GET` | `/events/stream` | – | SSE stream of all runtime events (when `sse_emitter=` is wired) |
| `GET` | `/healthz` | – | `200` if alive |
| `GET` | `/readyz` | – | `200` if broker connected, registry loaded |
| `GET` | `/health` | – | Legacy alias for `/healthz` |

`POST /groups/{name}/run` returns one of two shapes depending on how
many terminal nodes fired at runtime:

- **Single-leaf** (typical pipeline, branch routing where one
  predicate fires) → standard `AgentResult` envelope
  (`{agent_name, task_id, success, output, error, metadata}`).
- **Multi-leaf** (moderator-and-specialists, parallel branches whose
  conditions both fire) → `GroupResult` envelope
  (`{group: true, outputs: {leaf_name: AgentResult, ...}, success,
  metadata}`).

The same shape comes back from `GET /runs/{run_id}/result` for
async-submitted group runs — the run-store carries the original
`AgentResult | GroupResult` and the result endpoint serialises it
verbatim. Clients can discriminate on the `"group"` key.

`/healthz` and `/readyz` are split per the conventional pattern —
`/healthz` checks the process is alive; `/readyz` checks it can accept
traffic.

## SSE event stream — embedded

Pass the same `sse_emitter=` to the router:

```python
from murmur.events import LogEventEmitter, MultiEventEmitter, SSEEventEmitter

sse = SSEEventEmitter(heartbeat_interval=15.0)
runtime = AgentRuntime(
    event_emitter=MultiEventEmitter([LogEventEmitter(), sse]),
)

router = AgentRouter(runtime=runtime, sse_emitter=sse)
```

The router exposes `GET /events/stream` (mount-relative) when
`sse_emitter` is set. Note: `sse_emitter` and `server=` are mutually
exclusive — use one or the other, not both.

## `LocalClient` — in-process API

For Python callers in the same process, skip HTTP entirely:

```python
from murmur_client import LocalClient

client = LocalClient(server=app)        # or server=router

run = await client.submit("researcher", TaskSpec(input="..."))
async for event in run.events():
    print(event)
result = await run.result()
```

`LocalClient` and `MurmurClient` (HTTP) both satisfy a shared
`_RunBackend` Protocol — same call surface, different transport.

## Auth, rate limiting, request IDs

Murmur ships none of these. They're application concerns and compose
cleanly:

- **Auth**: standard FastAPI dependencies on the routes you mount.
  `app.include_router(router, prefix="/agents", dependencies=[Depends(verify_token)])`.
- **Rate limiting**: a third-party middleware (slowapi, fastapi-limiter)
  in front of the router.
- **Request IDs**: any standard middleware that sets `X-Request-Id`.
  Murmur's runtime promotes `request_id` to `trace_id` on every
  `RuntimeEvent`.

Auth and rate limiting are deliberately out of scope for Murmur — the
embedded pattern is how you compose them in.

## Where to next

- **Run a fleet behind your app** — [Distributed deployments](distributed.md).
- **Live event stream over your own SSE route** — [`SSEEventEmitter` setup](../concepts/events.md#sseeventemitter)
  and the [`events_dashboard.py` example](https://github.com/murmur-ai/murmur/blob/main/examples/events_dashboard.py).
- **Cap costs per request** — [`TokenBudget`](../concepts/cost.md).
- **Decompose work via the LLM inside an HTTP handler** — [Agents — LLM-driven fan-out](../concepts/agents.md#llm-driven-fan-out-with-spawn_agents).
