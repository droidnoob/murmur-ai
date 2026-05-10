# @murmur/client

TypeScript client for [Murmur](../../README.md) agent servers. Browser + Node compatible (fetch + ReadableStream — no `EventSource`, so custom `Authorization` headers work).

Mirrors the Python client at [`packages/murmur-client`](../murmur-client) — same wire envelope, same auth model, same surface.

## Install

```bash
npm install @murmur/client
# or
pnpm add @murmur/client
```

## Quickstart

```ts
import { MurmurClient } from "@murmur/client";

const client = new MurmurClient("http://localhost:8421", {
  authToken: process.env.MURMUR_TOKEN, // omit if the server has no auth_token set
});

interface CapitalLookup {
  country: string;
  capital: string;
  confidence: number;
}

const result = await client.run<CapitalLookup>("geographer", {
  input: "Capital of Japan?",
});

if (result.output != null) {
  console.log(result.output.capital); // "Tokyo"
} else {
  console.error(result.error);
}
```

## Surface

| Method | Wire | Notes |
|---|---|---|
| `health()` | `GET /health` | bypasses auth |
| `listAgents()` | `GET /agents` | |
| `getAgentSchema(name)` | `GET /agents/{name}/schema` | |
| `listGroups()` | `GET /groups` | |
| `getGroupTopology(name)` | `GET /groups/{name}/topology` | |
| `listTools()` | `GET /tools` | |
| `usage({ groupBy? })` | `GET /usage` | optional `groupBy="model"` |
| `runtimeStats()` | `GET /runtime/stats` | |
| `run<T>(name, task)` | `POST /agents/{name}/run` | sync — returns `AgentResult<T>` |
| `gather<T>(name, tasks)` | `POST /agents/{name}/gather` | sync fan-out |
| `runGroup(name, task)` | `POST /groups/{name}/run` | returns `AgentResult \| GroupResult` |
| `submit(target, task)` | `POST /submit` | returns a `Run` handle |
| `Run.status()` / `.result()` / `.cancel()` / `.stream()` | per-run polling + SSE | |
| `streamEvents(signal?)` | `GET /events/stream` | fleet-wide SSE |

## Auth

Static bearer token only. Set `authToken` on the client; the server enforces it via `AgentServer(auth_token=...)`. Wrong / missing token → `UnauthorizedError` (401).

## Errors

All HTTP errors raise a subclass of `MurmurError`:

- `UnauthorizedError` — 401, missing or wrong token
- `RegistryError` — 404 / unknown agent or group
- `MurmurError` — generic catch-all for everything else

Each carries `.type`, `.status`, and `.requestId` for log correlation against the server's structured `request_id` field.

## Running the tests

```bash
cd packages/js-client
npm install
npm test
```

The unit tests use a programmable fake `fetch` to verify wire shape and auth wiring without a live Python server. End-to-end coverage lives in the Python suite (`tests/client/`, `tests/server/test_auth.py`) and exercises the same routes.
