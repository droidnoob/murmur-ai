/**
 * Wire types — mirror the Pydantic models the AgentServer serialises.
 * Kept narrow on purpose: only the fields the server actually emits, so
 * additions on the Python side don't silently break the JS client.
 */

export interface TaskSpec {
  /** Per-task UUID. The client generates this if you don't. */
  id?: string;
  /** Per-call UUID for log correlation across runtime + tools. */
  request_id?: string;
  /** The user's prompt. Free-form string by contract; structured input
   *  goes through the agent's input_type validator on the server side. */
  input: string;
  /** Free-form propagated metadata — no semantic meaning to the runtime. */
  metadata?: Record<string, string>;
}

export interface ResultMetadata {
  duration_ms: number;
  tokens_used: number;
  cost_usd: number;
  backend: string;
  trace_id: string | null;
}

/**
 * Outcome of a single agent dispatch. ``output`` is present iff the run
 * succeeded; ``error`` is the wire-form of the exception otherwise. Use
 * {@link AgentResult.is_ok} on the helper or check ``output != null``.
 */
export interface AgentResult<T = unknown> {
  output: T | null;
  /** When set, the run failed. ``type`` is the Python error class name. */
  error: { type: string; message: string } | null;
  metadata: ResultMetadata;
  agent_name: string;
  task_id: string;
}

export interface GroupResult {
  outputs: Record<string, AgentResult>;
  metadata: ResultMetadata;
  group_name: string;
  task_id: string;
}

/** Lifecycle phases of a submitted run. Mirrors ``murmur.runs.RunStatus``. */
export type RunPhase =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface RunStatus {
  run_id: string;
  phase: RunPhase;
  target: string;
  is_group: boolean;
  created_at: string;
  finished_at: string | null;
}

/** One server-sent event off the run-stream or fleet-stream endpoint. */
export interface RunEvent {
  event_type: string;
  agent_name: string;
  task_id: string | null;
  trace_id: string;
  parent_trace_id: string | null;
  timestamp: string;
  payload: Record<string, unknown>;
}
