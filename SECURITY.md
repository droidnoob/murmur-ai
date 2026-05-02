# Security

This document describes Murmur's threat model: what the runtime
defends against, what it deliberately does not, and the composition
pattern operators are expected to use for inbound auth, sanitisation,
and isolation.

Murmur is **infrastructure** for orchestrating LLM agents. It is not
an agent framework, an auth framework, or a containment system. The
boundaries below are deliberate; pretending otherwise would expose
operators to surprises.

---

## Reporting a vulnerability

Report security issues privately rather than as public GitHub issues.
Send a description (steps to reproduce, affected versions, suspected
impact) to the project maintainers via the contact listed on the
[repository][repo]. We aim to acknowledge within five business days
and to ship a fix or mitigation within thirty days for non-critical
issues; critical issues (remote code execution, credential exposure,
unbounded resource consumption with no operator opt-in) get an
out-of-band release.

[repo]: https://github.com/murmur-ai/murmur

---

## What Murmur defends against

### Tool-policy bypass

Agents cannot directly invoke tools the runtime has not gated. The
[`ToolExecutor`][tools] is a single chokepoint: every native tool
call, every MCP-discovered tool call, every `spawn_agents` invocation
flows through it. The agent's `trust_level` controls the gate:

- `LOW` requires an explicit `allow=[…]` for both native tools (per
  the runtime's allow-list) and each MCP server (`mcp_stdio(...,
  allow=[...])`).
- `SANDBOX` skips MCP servers entirely and rejects any native tool.
- `MEDIUM` and `HIGH` expose the registered set unless `allow=`
  narrows.

Same gate is used by `_PolicyMCPToolset` for MCP calls and by
`make_spawn_agents_tool` for child dispatch — there is no second
code path that bypasses the executor.

[tools]: https://github.com/murmur-ai/murmur/blob/main/docs/concepts/tools.md

### Cascading-spawn budget exhaustion

[`TokenBudget`][cost] enforces a per-runtime cumulative token
ceiling. The `CostTrackingMiddleware`'s pre-check rejects new
dispatch when `remaining <= 0` (emits `BUDGET_EXCEEDED` then raises
`BudgetExceededError`). The post-charge tolerates one over-spend
per saturation event — the next call hard-stops.

This is a **soft cap in distributed mode** (multiple cross-process
consumers race the same publisher-side counter). For hard
cross-machine caps, operators shape upstream task supply with
`gather(max_concurrency=N)` plus broker-level rate limiting.

[cost]: https://github.com/murmur-ai/murmur/blob/main/docs/concepts/cost.md

### Spawn-depth runaway (partial)

`DepthLimitMiddleware` rejects runs whose `AgentContext.depth`
exceeds the configured cap (default 4). It only fires when the
caller threads `depth` through `AgentContext` — direct
`runtime.run` from inside a tool body always builds a fresh context.
Full cascading-spawn graph tracking (parent → child → grandchild
linkage with cycle rejection) is queued as future work; today the
operator is responsible for not registering `spawn_agents` on a
template that becomes a child agent.

### MCP exposure isolation

The MCP expose side is opt-in at two tiers:

1. **Surface.** `AgentServer.serve_mcp()` must be called explicitly.
   Constructing `AgentServer` does not start an MCP server.
2. **Per-agent.** Only agents enrolled via `register_mcp(agent, ...)`
   appear as MCP tools. `register()` is HTTP-only; an agent
   registered for HTTP is **invisible** to MCP clients unless the
   operator explicitly enrolls it.

Re-running `serve_mcp()` builds a fresh `FastMCP`, so re-enrollment
on the same `tool_name` replaces; an empty-enrollment serve raises
`RegistryError` rather than silently starting an empty server.

See [MCP — expose side][mcp-expose].

[mcp-expose]: https://github.com/murmur-ai/murmur/blob/main/docs/concepts/mcp.md

### Wire-format spoofing

Broker `TaskMessage` / `ResultMessage` envelopes carry primitive
fields (`success: bool`, `output_payload: dict`, `error_message:
str`) and rehydrate against the agent's declared `output_type` after
receipt. Arbitrary `BaseModel` injection over the wire is not
possible — the receiver picks the validator class by agent name from
its registry; the wire payload only supplies the dict.

---

## What Murmur explicitly does NOT defend against

### Model jailbreaks

Persuading an LLM to ignore its system prompt, leak its instructions,
or claim affordances it does not have is a property of the
**underlying model**, not the orchestration layer. Murmur passes
your `instructions` through to PydanticAI verbatim and validates the
returned output against `output_type`. Resistance to jailbreak
attempts is the model's responsibility; output validation is
Murmur's only contribution to the surface area.

### Prompt injection from tainted tool / MCP output

When an agent calls `web_search`, an MCP server's `read_file`, or
any tool that returns content from an untrusted source, that content
becomes part of the agent's next prompt. A malicious source can
embed instructions designed to subvert the agent's behaviour. Murmur
**does not sanitise tool / MCP output today**. Until the
`SelectiveContextPasser` work lands (currently queued), agents that
ingest external content should:

- Run at `TrustLevel.LOW` with a minimal `allow=[…]` so a subverted
  agent cannot escalate by calling additional tools.
- Use `output_type` schemas that constrain the response to a small,
  validated shape (the LLM cannot return arbitrary instructions
  through a `confidence: float, sources: list[str]` schema).
- Operate inside a process boundary the operator already trusts —
  this is what the future `ContainerBackend` will provide; until
  then, treat agents that ingest external content as having the
  same blast radius as the surrounding process.

### Side-channel exfiltration via tool arguments

A compromised LLM can encode data into legitimate tool calls
(extra-long search queries, suspicious file paths, etc.). Murmur
**logs every tool call** (see [Events][events]) but does not
inspect arguments for exfiltration patterns or pattern-match against
DLP rules. Operators who need exfiltration prevention layer that on
at the tool implementation (the body of the registered tool) or
upstream (e.g. a proxy intercepting outbound network calls).

[events]: https://github.com/murmur-ai/murmur/blob/main/docs/concepts/events.md

### CPU / memory / crash isolation within a process

`AsyncBackend` (the default, asyncio-task-based) shares one event
loop, one Python interpreter, one memory space, and one crash
domain. A CPU-bound malicious tool body blocks the loop; an OOM in
one tool kills every concurrent agent. Murmur today does not
isolate at the OS level. Mitigations:

- Trust-level + tool allow-list keeps unknown tools off the agent's
  surface. The default `LOW` allow-list is the load-bearing
  defence.
- `JobBackend` with a real broker + multiple `Worker` processes
  provides crash isolation at the worker boundary today (broker
  re-delivers on worker death).
- The future `ContainerBackend` will provide per-run process
  isolation. Until then, agents processing untrusted input that
  could trigger CPU-bound work should run in a `Worker` process the
  operator is willing to lose.

### Inbound authentication of callers

Murmur ships **no auth framework**. Every surface that reaches
agents must be auth'd by the operator at the boundary:

| Surface | Composition pattern |
|---|---|
| HTTP `AgentServer` (`murmur serve` or embedded `AgentRouter`) | FastAPI `Depends(...)` on `app.include_router(router, prefix=..., dependencies=[Depends(verify_token)])`. Third-party middleware (slowapi, fastapi-limiter) for rate limiting. |
| MCP HTTP transport (`serve_mcp(transport="http")`) | Reverse proxy doing token / mTLS verification. Murmur does no auth on the MCP route. |
| MCP stdio transport (`serve_mcp(transport="stdio")`) | OS-level — only callers with FS access to launch the subprocess can talk. The `mcpServers` config on the calling client is the trust boundary. |
| Broker (Kafka / NATS / RabbitMQ / Redis) | Broker-native auth: TLS, SASL/SCRAM, ACLs. Credentials pass through the broker URL or broker arguments; Murmur does not intercept. |
| Worker auto-discovery | The broker IS the trust boundary — anything that can publish to the agent's task topic invokes the agent. Treat the broker like an internal RPC bus. |

Auth is deliberately out of Murmur's scope because every operator's
auth story is already solved at the layer Murmur sits on top of, and
shipping a bespoke auth framework would create the wrong incentives
(operators bypassing it, or worse, trusting it past its design).

---

## Trust level semantics

| Level | Native tools | MCP servers | When to use |
|---|---|---|---|
| `HIGH` | All registered names allowed | All exposed unless `allow=` narrows | Code you wrote, in your trust boundary. |
| `MEDIUM` (default) | All registered names allowed | All exposed unless `allow=` narrows | Internal automation against agents you control. |
| `LOW` | Read-only allowlist required | Requires explicit `allow=[…]` per server | Agents that ingest **any** external content (user input, MCP results, web search). |
| `SANDBOX` | None — all calls rejected | All servers skipped | Pure reasoning; no I/O of any kind. |

`SANDBOX` is currently enforced at the tool layer only — backend
isolation lands with `ContainerBackend`. Until then, treat
`SANDBOX` as "no tools" rather than "no side effects" and assume
the agent shares the runtime's process resources.

---

## Known limitations

These are deliberate — documented so operators can plan around them
rather than be surprised.

1. **No `parent_trace_id` linkage.** Children spawned via
   `spawn_agents`, `runtime.run_group`, or recursive `runtime.run`
   appear as independent top-level runs in the event stream.
   Correlate by timing + agent name until the cascading-spawn graph
   work ships.

2. **`spawn_agents` cycle detection is operator-enforced.** Don't
   include `spawn_agents` in an `AgentTemplate.tools` set used to
   materialise children — recursive spawn would loop with no cap
   beyond the per-runtime budget.

3. **Distributed `TokenBudget` is best-effort.** Cross-process
   consumers race the same counter. Use it as a soft ceiling for
   alerting; pair with broker-level rate limiting for hard caps.

4. **`AsyncBackend` (default) is single-process, no isolation.** A
   blocking call or memory leak in one tool affects every concurrent
   agent. Move to `JobBackend` + multiple `Worker` processes for
   isolation today.

5. **MCP expose side: any enrolled agent is callable by any
   connecting MCP client.** Auth lives at the transport layer
   (reverse proxy for HTTP, OS for stdio).

6. **Tool / MCP output is not sanitised.** Agents that ingest
   external content should run at `LOW`, use a tight `output_type`,
   and live in a process boundary the operator already trusts.

7. **Agent definitions are not signed.** YAML registry files are
   loaded verbatim. An operator who lets untrusted code edit the
   spec directory has handed over the runtime; integrity is a
   filesystem / source-control concern.

---

## Sanitisation contract (forward-looking)

When `SelectiveContextPasser` ships, the contract on the
result-return path will be:

- **External-source content** (MCP responses, untrusted tool output,
  user input) entering the next agent's context will be marked and
  optionally summarised through a separate cheap-model call before
  inclusion.
- **Sanitised text will be passed alongside, not in place of**, the
  original — agents can opt to use either, but the default
  `SelectiveContextPasser` policy will prefer the sanitised view for
  agents at `TrustLevel.LOW`.
- **The sanitisation step is itself an agent**, so its
  `output_type` constrains what can be produced; it cannot inject
  arbitrary instructions because it must validate against a
  schema.

Documented now so authors building today on `NullContextPasser` /
`FullContextPasser` can structure agents to slot the sanitiser in
without reshaping their context flow.

---

## Recommendations by deployment shape

### Internal automation, single-host

- Default config (`AgentRuntime()`, `MEDIUM`) is fine.
- Wire `LogEventEmitter` (default) to your log aggregation.
- Set `RuntimeOptions(token_budget=TokenBudget(limit=…))` early —
  cheaper than discovering a runaway agent in the bill.

### Internal automation, multi-host fleet

- `AgentRuntime(broker=...)` + multiple `Worker` processes — gives
  crash isolation today.
- Broker auth (SASL on Kafka, NKEYS on NATS, ACLs on RabbitMQ /
  Redis) is non-negotiable. Treat the broker as a trust boundary,
  not a transport.
- `BrokerEventBridge` for centralised event observability — one
  `murmur serve --broker URL --publish-events` is the SSE dashboard
  for the fleet.

### Agents ingesting user input or web search results

- `TrustLevel.LOW`. Always.
- Tight `output_type` — small Pydantic schema with bounded list
  lengths. The schema is your post-condition.
- No MCP servers unless they're inspected and on a fixed
  `allow=[…]`.
- Run in a `Worker` process you're willing to lose if the agent
  misbehaves.

### Agents exposed via MCP to third-party clients (Claude Desktop, etc.)

- Default to `register_mcp` only on agents you've audited the tool
  surface of. Internal admin agents stay HTTP-only.
- For `transport="http"`: terminate auth at a reverse proxy
  upstream of `AgentServer`. Murmur doesn't see auth headers.
- For `transport="stdio"`: the launching client config IS the
  trust boundary. If a desktop user runs your stdio MCP server,
  they have whatever access the agent's tools provide.
- Pair with `TokenBudget` so a chatty client can't bleed your
  provider keys.
- Log every tool call and MCP `tool.call` — `LogEventEmitter` does
  this by default.

### Agents that delegate via `spawn_agents`

- Keep `spawn_agents` on the **orchestrator's** per-agent `tools`
  set; **never** on the `AgentTemplate.tools` for the children.
- Bind the factory to the runtime you actually want children to
  dispatch on — see [Backends — runtime-binding gotcha][rb].
- Set `max_concurrency` on the factory; the per-call cap is your
  hard ceiling on simultaneous in-flight children.

[rb]: https://github.com/murmur-ai/murmur/blob/main/docs/concepts/backends.md

---

## Hardening checklist

A short pre-deployment list. None of these are mandatory; all of
them are cheap.

- [ ] `RuntimeOptions(token_budget=TokenBudget(limit=…))` set with a
  real cap.
- [ ] `RuntimeOptions(timeout_seconds=…)` set lower than the
  default 300s if your workload allows.
- [ ] Every agent has an `output_type` that constrains the response
  shape — no free-string outputs reaching downstream consumers.
- [ ] Trust level is `LOW` for any agent ingesting external
  content; allow-list is the smallest set that works.
- [ ] MCP expose side: no internal agent is in `register_mcp`'s
  enrollment unless explicitly intended.
- [ ] HTTP server: FastAPI auth dependency on the agent route
  prefix (`Depends(verify_token)` or equivalent).
- [ ] Broker mode: TLS + SASL on the broker connection. Never
  unauthenticated brokers.
- [ ] Token budget alerting wired — `BUDGET_EXCEEDED` event
  surfaces in your on-call channel.
- [ ] `runtime.shutdown()` called on process exit (or use
  `AgentRouter` / `AgentServer` lifespans, which call it for you)
  so MCP subprocesses and broker connections release cleanly.

---

## Versioning

Murmur is pre-1.0. Breaking changes to the trust model, the tool
gating semantics, or the wire envelope will be called out in
[CHANGELOG.md](https://github.com/murmur-ai/murmur/blob/main/CHANGELOG.md)
under an explicit `Security` section.
Operators who rely on a specific guarantee should pin to a minor
version (`murmur-ai>=0.x.y,<0.x+1.0`) until the project reaches 1.0.
