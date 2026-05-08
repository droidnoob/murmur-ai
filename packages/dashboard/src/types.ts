export type RunStatus = "spawned" | "running" | "completed" | "failed" | "rejected";
export type TrustLevel = "high" | "medium" | "low" | "sandbox";
export type RejectReason = "cycle" | "depth" | "cap" | "budget" | "timeout";
export type Severity = "info" | "warn" | "error";
export type Backend = "thread" | "job" | "group" | string;
export type Theme = "dark" | "light";

export interface Run {
  trace_id: string;
  agent_name: string;
  parent_trace_id: string | null;
  depth: number;
  status: RunStatus;
  trust_level: TrustLevel;
  backend: Backend;
  tokens_used: number;
  duration_ms: number;
  cascade_label?: string;
  rejection_reason?: RejectReason | null;
  error?: string | null;
  task_input?: string;
  task_metadata?: Record<string, unknown>;
  started_at?: string;
  children?: Run[];
  parent_agent?: string | null;
}

export interface FlatRun extends Run {
  parent_agent: string | null;
}

export type EventType =
  | "agent_spawned"
  | "agent_completed"
  | "agent_failed"
  | "spawn_rejected"
  | "tool_call_started"
  | "tool_call_completed"
  | "tool_call_failed"
  | "worker_started";

export interface EventEntry {
  ts: string;
  type: EventType;
  agent: string;
  trace: string;
  severity: Severity;
  preview: string;
}

export interface Worker {
  id: string;
  broker: string;
  subscribed: string[];
  concurrency: number;
  in_flight: number;
  last_hb: string;
  status: "healthy" | "stale" | "down";
}

export interface MCPServer {
  name: string;
  mode: "stdio" | "http" | "sse" | string;
  tools_count: number;
  tools: string[];
  eager: boolean;
  status: "connected" | "degraded" | "disconnected" | string;
}

export interface ErrorGroup {
  error_class: string;
  count_24h: number;
  last: string;
  top_agent: string;
}

export interface RejectionCounts {
  budget: number;
  cycle: number;
  depth: number;
  cap: number;
  timeout: number;
}

export type ConnectionStatus = "connected" | "reconnecting" | "failed" | string;

export interface RuntimeInfo {
  id: string;
  broker: string;
  broker_status: ConnectionStatus;
  sse_status: ConnectionStatus;
  spawn_count: number;
  max_total_spawns: number;
  tokens_used: number;
  token_budget: number;
  workers_count: number;
  mcp_count: number;
}
