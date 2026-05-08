// Reduce a flat list of WireEvents into the dashboard's Run-tree shape.
//
// The server emits one event per state transition (AGENT_SPAWNED →
// AGENT_COMPLETED / AGENT_FAILED, plus tool-call/budget events). The dashboard
// renders runs as nodes with derived status, tokens_used, duration_ms. This
// module folds the events into Run records, then assembles parent → child
// trees from `parent_trace_id`.

import type { WireEvent } from "./api";
import type {
  EventEntry,
  EventType,
  RejectReason,
  Run,
  RunStatus,
  Severity,
  TrustLevel,
} from "./types";

const STATUS_BY_TYPE: Record<string, RunStatus> = {
  agent_dispatched: "spawned",
  agent_spawned: "running",
  agent_completed: "completed",
  agent_failed: "failed",
  spawn_rejected: "rejected",
  budget_exceeded: "rejected",
  depth_limit_exceeded: "rejected",
};

const REJECT_BY_TYPE: Record<string, RejectReason | undefined> = {
  budget_exceeded: "budget",
  depth_limit_exceeded: "depth",
};

interface Accumulator {
  trace_id: string;
  parent_trace_id: string | null;
  agent_name: string;
  status: RunStatus;
  trust_level: TrustLevel;
  backend: string;
  tokens_used: number;
  duration_ms: number;
  started_at: string | null;
  rejection_reason: RejectReason | null;
  error: string | null;
  task_input?: string;
}

function asString(v: unknown, fallback = ""): string {
  return typeof v === "string" ? v : fallback;
}

function asNumber(v: unknown, fallback = 0): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }
  return fallback;
}

function isTrustLevel(v: unknown): v is TrustLevel {
  return v === "high" || v === "medium" || v === "low" || v === "sandbox";
}

function isRejectReason(v: unknown): v is RejectReason {
  return v === "cycle" || v === "depth" || v === "cap" || v === "budget" || v === "timeout";
}

function fmtClockTime(iso: string): string {
  // Render in the browser's local timezone — server emits UTC.
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function fmtClockTimeWithMs(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const base = fmtClockTime(iso);
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${base}.${ms}`;
}

// Fold the event list into per-trace accumulators.
function accumulate(events: WireEvent[]): Map<string, Accumulator> {
  const acc = new Map<string, Accumulator>();
  for (const ev of events) {
    let a = acc.get(ev.trace_id);
    if (!a) {
      a = {
        trace_id: ev.trace_id,
        parent_trace_id: ev.parent_trace_id,
        agent_name: ev.agent_name,
        status: "spawned",
        trust_level: "high",
        backend: "thread",
        tokens_used: 0,
        duration_ms: 0,
        started_at: null,
        rejection_reason: null,
        error: null,
      };
      acc.set(ev.trace_id, a);
    }
    // Latest non-empty agent_name wins; events should agree but be defensive.
    if (ev.agent_name) a.agent_name = ev.agent_name;
    if (ev.parent_trace_id !== null) a.parent_trace_id = ev.parent_trace_id;

    const next = STATUS_BY_TYPE[ev.event_type];
    if (next) a.status = next;

    const trust = ev.payload.trust_level;
    if (isTrustLevel(trust)) a.trust_level = trust;

    const backend = asString(ev.payload.backend);
    if (backend) a.backend = backend;

    if (ev.event_type === "agent_spawned" || ev.event_type === "agent_dispatched") {
      a.started_at = fmtClockTime(ev.timestamp);
    }
    if (ev.event_type === "agent_completed" || ev.event_type === "agent_failed") {
      a.duration_ms = asNumber(ev.payload.duration_ms, a.duration_ms);
      a.tokens_used = asNumber(ev.payload.tokens_used, a.tokens_used);
    }
    if (ev.event_type === "agent_failed") {
      a.error = asString(ev.payload.error, a.error ?? "");
    }
    const reject = REJECT_BY_TYPE[ev.event_type];
    if (reject) a.rejection_reason = reject;
    if (ev.event_type === "spawn_rejected") {
      const reason = ev.payload.reason;
      a.rejection_reason = isRejectReason(reason) ? reason : "cycle";
    }
  }
  return acc;
}

// Build the Run forest from accumulators.
export function eventsToRuns(events: WireEvent[]): Run[] {
  if (events.length === 0) return [];
  const acc = accumulate(events);
  // Track each trace's most recent event timestamp so we can sort roots
  // newest-first below. Iterates in event order, so the last assignment
  // wins.
  const lastSeenTs = new Map<string, string>();
  for (const ev of events) {
    lastSeenTs.set(ev.trace_id, ev.timestamp);
  }
  const nodes = new Map<string, Run>();
  for (const a of acc.values()) {
    nodes.set(a.trace_id, {
      trace_id: a.trace_id,
      agent_name: a.agent_name,
      parent_trace_id: a.parent_trace_id,
      depth: 0,
      status: a.status,
      trust_level: a.trust_level,
      backend: a.backend,
      tokens_used: a.tokens_used,
      duration_ms: a.duration_ms,
      started_at: a.started_at ?? "—",
      rejection_reason: a.rejection_reason,
      error: a.error,
      children: [],
    });
  }
  // Wire children + compute depth.
  const roots: Run[] = [];
  for (const node of nodes.values()) {
    if (node.parent_trace_id && nodes.has(node.parent_trace_id)) {
      const parent = nodes.get(node.parent_trace_id)!;
      parent.children!.push(node);
    } else {
      roots.push(node);
    }
  }
  // Sort roots newest-first so consumers (Live tab, History) see the
  // freshest activity at the top without an extra sort step.
  roots.sort((a, b) => {
    const ta = lastSeenTs.get(a.trace_id) ?? "";
    const tb = lastSeenTs.get(b.trace_id) ?? "";
    return tb.localeCompare(ta);
  });
  function setDepth(node: Run, depth: number) {
    node.depth = depth;
    for (const c of node.children ?? []) setDepth(c, depth + 1);
  }
  roots.forEach((r) => setDepth(r, 0));
  return roots;
}

// Translate a single event into the dashboard's event-stream row shape.
const SEVERITY_BY_TYPE: Record<string, Severity> = {
  agent_failed: "error",
  tool_call_failed: "error",
  spawn_rejected: "warn",
  budget_exceeded: "error",
  depth_limit_exceeded: "warn",
};

const SUPPORTED_EVENT_TYPES: ReadonlySet<EventType> = new Set<EventType>([
  "agent_spawned",
  "agent_completed",
  "agent_failed",
  "spawn_rejected",
  "tool_call_started",
  "tool_call_completed",
  "tool_call_failed",
  "worker_started",
]);

export function toEventEntry(ev: WireEvent): EventEntry {
  const previewParts: string[] = [];
  for (const [k, v] of Object.entries(ev.payload)) {
    previewParts.push(`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`);
  }
  const trace = ev.trace_id.length > 8 ? `${ev.trace_id.slice(0, 4)}...${ev.trace_id.slice(-4)}` : ev.trace_id;
  const type: EventType = SUPPORTED_EVENT_TYPES.has(ev.event_type as EventType)
    ? (ev.event_type as EventType)
    : "agent_spawned";
  return {
    ts: fmtClockTimeWithMs(ev.timestamp),
    type,
    agent: ev.agent_name,
    trace,
    severity: SEVERITY_BY_TYPE[ev.event_type] ?? "info",
    preview: previewParts.join(" "),
  };
}
