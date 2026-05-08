// REST + SSE client for the AgentServer endpoints (`/runs`, `/runs/{id}/tree`,
// `/events`, `/events/stream`, `/usage`). Every call returns `null` on a 404
// or network failure so the UI can fall through to demo data.

export interface WireEvent {
  event_type: string;
  timestamp: string;
  agent_name: string;
  task_id: string | null;
  trace_id: string;
  parent_trace_id: string | null;
  payload: Record<string, unknown>;
}

export interface UsageGroup {
  key: string;
  tokens_used: number;
  events: number;
}

export interface UsageReport {
  group_by: "agent" | "trace" | "none";
  totals: { tokens_used: number; events: number };
  groups: UsageGroup[];
}

export interface WireRuntimeStats {
  id: string;
  broker: string | null;
  broker_status: string;
  sse_status: string;
  spawn_count: number;
  max_total_spawns: number | null;
  tokens_used: number;
  token_budget: number | null;
  workers_count: number;
  mcp_count: number;
}

export interface WireWorker {
  id: string;
  broker: string;
  subscribed: string[];
  concurrency: number;
  in_flight: number;
  last_hb: string;
  status: string;
}

export interface WireMCPServer {
  name: string;
  mode: string;
  tools_count: number;
  tools: string[];
  eager: boolean;
  status: string;
}

export interface WireErrorGroup {
  error_class: string;
  count_24h: number;
  last: string;
  top_agent: string;
}

export interface WireRejectionCounts {
  budget: number;
  cycle: number;
  depth: number;
  cap: number;
  timeout: number;
}

export interface WireRunSummary {
  trace_id: string;
  agent_name: string;
  status: "spawned" | "running" | "completed" | "failed" | "rejected";
  started_at: string | null;
  duration_ms: number;
  tokens_used: number;
  backend: string;
  trust_level: "high" | "medium" | "low" | "sandbox";
  depth: number;
  parent_agent: string | null;
}

export interface RunsPage {
  rows: WireRunSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface StatsResponse {
  runtime: WireRuntimeStats;
  burn_rate: number[];
  rejection_counts: WireRejectionCounts;
  error_groups: WireErrorGroup[];
  workers: WireWorker[];
  mcp_servers: WireMCPServer[];
}

// Same-origin by default — the dashboard is served at /dashboard/ off the same host.
// Override via VITE_API_BASE for cross-origin dev.
const RAW_BASE = import.meta.env.VITE_API_BASE ?? "";
const API_BASE: string = RAW_BASE.replace(/\/+$/, "");

async function safeJson<T>(path: string): Promise<T | null> {
  try {
    const r = await fetch(`${API_BASE}${path}`, { headers: { Accept: "application/json" } });
    if (!r.ok) return null;
    return (await r.json()) as T;
  } catch {
    return null;
  }
}

export const api = {
  runsPage: (params: {
    limit?: number;
    offset?: number;
    status?: string;
  } = {}): Promise<RunsPage | null> => {
    const q = new URLSearchParams();
    if (params.limit !== undefined) q.set("limit", String(params.limit));
    if (params.offset !== undefined) q.set("offset", String(params.offset));
    if (params.status !== undefined) q.set("status", params.status);
    const qs = q.toString();
    return safeJson(`/runs${qs ? `?${qs}` : ""}`);
  },
  runTree: (traceId: string): Promise<WireEvent[] | null> =>
    safeJson(`/runs/${encodeURIComponent(traceId)}/tree`),
  events: (limit = 200): Promise<WireEvent[] | null> => safeJson(`/events?limit=${limit}`),
  usage: (groupBy: "agent" | "trace" | "none" = "agent"): Promise<UsageReport | null> =>
    safeJson(`/usage?group_by=${groupBy}`),
  stats: (): Promise<StatsResponse | null> => safeJson(`/runtime/stats`),
};

export type Unsubscribe = () => void;

// Subscribe to the live RuntimeEvent firehose. Returns an unsubscribe function.
// Returns null if the EventSource fails to open (no /events/stream endpoint, or
// the host doesn't speak SSE).
export function subscribeEvents(
  onEvent: (event: WireEvent) => void,
  onError?: (err: Event) => void,
): Unsubscribe | null {
  if (typeof window === "undefined" || typeof EventSource === "undefined") return null;
  let source: EventSource;
  try {
    source = new EventSource(`${API_BASE}/events/stream`);
  } catch {
    return null;
  }
  source.onmessage = (msg) => {
    try {
      const payload = JSON.parse(msg.data) as WireEvent;
      onEvent(payload);
    } catch {
      // Malformed frame — ignore. The server's emitter is fire-and-forget.
    }
  };
  if (onError) source.onerror = onError;
  return () => source.close();
}
