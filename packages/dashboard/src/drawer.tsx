import { Fragment, useState } from "react";
import type { ReactNode } from "react";
import { Icons } from "./icons";
import { StatusDot, StatusPill, TrustTag, BackendTag, IdBadge, fmt } from "./primitives";
import type { Run } from "./types";
import type { WireEvent } from "./api";

const TIMELINE_COLOR_BY_TYPE: Record<string, string> = {
  agent_dispatched: "var(--text-tertiary)",
  agent_spawned: "var(--accent)",
  agent_completed: "var(--status-completed)",
  agent_failed: "var(--status-failed)",
  tool_call_started: "var(--text-tertiary)",
  tool_call_completed: "var(--status-completed)",
  tool_call_failed: "var(--status-failed)",
  spawn_rejected: "var(--status-rejected)",
  budget_exceeded: "var(--reject-budget)",
  depth_limit_exceeded: "var(--reject-depth)",
  batch_started: "var(--text-tertiary)",
  batch_completed: "var(--status-completed)",
  group_started: "var(--text-tertiary)",
  group_completed: "var(--status-completed)",
};

function fmtTimelineTs(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms}`;
}

interface TimelineRow {
  ts: string;
  type: string;
  label: string;
  detail?: string;
  color: string;
}

function eventToTimelineRow(ev: WireEvent): TimelineRow {
  const type = ev.event_type;
  const color = TIMELINE_COLOR_BY_TYPE[type] ?? "var(--text-tertiary)";
  let label = ev.agent_name;
  let detail: string | undefined;

  switch (type) {
    case "agent_spawned":
    case "agent_dispatched": {
      const backend = ev.payload.backend;
      const trust = ev.payload.trust_level;
      label = `${ev.agent_name} (backend=${backend ?? "?"}, trust=${trust ?? "?"})`;
      if (ev.parent_trace_id) detail = `parent_trace=${ev.parent_trace_id}`;
      break;
    }
    case "agent_completed": {
      const ms = ev.payload.duration_ms;
      const tokens = ev.payload.tokens_used;
      label = ev.agent_name;
      detail = `duration=${typeof ms === "number" ? ms : "?"}ms, tokens=${typeof tokens === "number" ? tokens : "?"}`;
      break;
    }
    case "agent_failed": {
      const err = ev.payload.error;
      label = ev.agent_name;
      detail = typeof err === "string" ? err : JSON.stringify(ev.payload);
      break;
    }
    case "tool_call_started":
    case "tool_call_completed":
    case "tool_call_failed": {
      const tool = ev.payload.tool_name;
      label = typeof tool === "string" ? tool : ev.agent_name;
      const err = ev.payload.error;
      if (typeof err === "string") detail = err;
      break;
    }
    case "budget_exceeded": {
      const limit = ev.payload.limit;
      const used = ev.payload.used;
      label = "budget exceeded";
      detail = `used=${used} / limit=${limit}`;
      break;
    }
    case "depth_limit_exceeded": {
      const limit = ev.payload.limit;
      const depth = ev.payload.depth;
      label = "depth limit exceeded";
      detail = `depth=${depth} / limit=${limit}`;
      break;
    }
    default: {
      const previewParts: string[] = [];
      for (const [k, v] of Object.entries(ev.payload)) {
        previewParts.push(`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`);
      }
      detail = previewParts.join(" ");
    }
  }

  return { ts: fmtTimelineTs(ev.timestamp), type, label, detail, color };
}

function DrawerSection({
  title,
  children,
  action,
  defaultOpen = true,
}: {
  title: ReactNode;
  children: ReactNode;
  action?: ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section style={{ borderBottom: "1px solid var(--border-subtle)" }}>
      <header
        onClick={() => setOpen(!open)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          padding: "10px 16px",
          cursor: "pointer",
          minHeight: 36,
        }}
      >
        {open ? (
          <Icons.ChevronDown size={12} stroke="var(--text-tertiary)" />
        ) : (
          <Icons.ChevronRight size={12} stroke="var(--text-tertiary)" />
        )}
        <span
          style={{
            fontSize: 11,
            fontWeight: 600,
            letterSpacing: 0.4,
            textTransform: "uppercase",
            color: "var(--text-secondary)",
            flex: 1,
          }}
        >
          {title}
        </span>
        {action}
      </header>
      {open && <div style={{ padding: "0 16px 14px" }}>{children}</div>}
    </section>
  );
}

function KVTable({ data }: { data: Record<string, unknown> }) {
  return (
    <table
      style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-mono)", fontSize: 11.5 }}
    >
      <tbody>
        {Object.entries(data).map(([k, v]) => (
          <tr key={k} style={{ borderBottom: "1px solid var(--border-subtle)" }}>
            <td
              style={{
                padding: "4px 8px 4px 0",
                color: "var(--text-tertiary)",
                verticalAlign: "top",
                whiteSpace: "nowrap",
              }}
            >
              {k}
            </td>
            <td style={{ padding: "4px 0", color: "var(--text-primary)", wordBreak: "break-all" }}>
              {String(v)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TimelineEntry({
  ts,
  type,
  label,
  detail,
  color,
}: {
  ts: string;
  type: string;
  label: string;
  detail?: string;
  color: string;
}) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "64px 14px 1fr",
        gap: 8,
        padding: "5px 0",
        fontSize: 11.5,
        lineHeight: 1.45,
      }}
    >
      <div style={{ fontFamily: "var(--font-mono)", color: "var(--text-muted)", fontSize: 10.5 }}>{ts}</div>
      <div style={{ display: "flex", justifyContent: "center", position: "relative" }}>
        <span
          style={{ width: 6, height: 6, borderRadius: "50%", background: color, marginTop: 6 }}
        />
      </div>
      <div style={{ minWidth: 0 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span
            style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}
          >
            {type}
          </span>
          <span style={{ color: "var(--text-primary)" }}>{label}</span>
        </div>
        {detail && (
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 10.5,
              color: "var(--text-muted)",
              marginTop: 1,
            }}
          >
            {detail}
          </div>
        )}
      </div>
    </div>
  );
}

export function RunDetailDrawer({
  run,
  onClose,
  lineage,
  events = [],
}: {
  run: Run | null;
  onClose: () => void;
  lineage: Run[];
  events?: WireEvent[];
}) {
  if (!run) return null;
  const isReject = run.status === "rejected";
  const isFail = run.status === "failed";

  // Build the timeline from real events for this trace_id, oldest-first.
  const traceEvents = events
    .filter((e) => e.trace_id === run.trace_id)
    .slice()
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp));
  const timeline: TimelineRow[] = traceEvents.map(eventToTimelineRow);

  return (
    <aside
      style={{
        position: "absolute",
        top: 0,
        right: 0,
        bottom: 0,
        width: "40%",
        minWidth: 480,
        maxWidth: 640,
        background: "var(--bg-surface)",
        borderLeft: "1px solid var(--border-default)",
        boxShadow: "var(--shadow-drawer)",
        overflowY: "auto",
        zIndex: 10,
        animation: "drawer-in 240ms ease",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div
        style={{
          position: "sticky",
          top: 0,
          zIndex: 2,
          background: "var(--bg-surface)",
          borderBottom: "1px solid var(--border-default)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 16px" }}>
          <button
            onClick={onClose}
            style={{
              width: 24,
              height: 24,
              background: "transparent",
              border: "1px solid var(--border-default)",
              borderRadius: "var(--r-sm)",
              color: "var(--text-secondary)",
              cursor: "pointer",
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <Icons.X size={12} />
          </button>
          <span
            style={{
              fontSize: 11,
              color: "var(--text-muted)",
              textTransform: "uppercase",
              letterSpacing: 0.4,
            }}
          >
            Run detail
          </span>
          <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
            <button
              style={{
                padding: "4px 8px",
                height: 24,
                background: "transparent",
                border: "1px solid var(--border-default)",
                borderRadius: "var(--r-sm)",
                color: "var(--text-secondary)",
                fontSize: 11,
                cursor: "pointer",
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
              }}
            >
              <Icons.ExternalLink size={11} /> Open
            </button>
          </div>
        </div>
        <div style={{ padding: "0 16px 14px" }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 6 }}>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: 18,
                fontWeight: 600,
                color: "var(--text-primary)",
                letterSpacing: -0.3,
              }}
            >
              {run.agent_name}
            </span>
            <StatusPill status={run.status} />
            <TrustTag trust={run.trust_level} />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <IdBadge id={run.trace_id} length={16} label="trace" />
            <span style={{ fontSize: 11, color: "var(--text-muted)" }}>·</span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--text-secondary)" }}>
              <span style={{ color: "var(--text-muted)" }}>dur</span> {fmt.ms(run.duration_ms)}
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--text-secondary)" }}>
              <span style={{ color: "var(--text-muted)" }}>tok</span> {fmt.tokens(run.tokens_used)}
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--text-secondary)" }}>
              <span style={{ color: "var(--text-muted)" }}>depth</span> {run.depth}
            </span>
            <BackendTag backend={run.backend} />
          </div>
        </div>
      </div>

      <DrawerSection title="Lineage">
        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 4 }}>
          {lineage.map((step, i) => (
            <Fragment key={i}>
              <button
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 4,
                  padding: "2px 8px",
                  height: 22,
                  background: i === lineage.length - 1 ? "var(--accent-bg)" : "var(--bg-input)",
                  color: i === lineage.length - 1 ? "var(--accent)" : "var(--text-secondary)",
                  border: `1px solid ${i === lineage.length - 1 ? "var(--accent-border)" : "var(--border-default)"}`,
                  borderRadius: "var(--r-sm)",
                  fontFamily: "var(--font-mono)",
                  fontSize: 11.5,
                  cursor: "pointer",
                }}
              >
                <StatusDot status={step.status} pulse={step.status === "running"} />
                {step.agent_name}
                <span style={{ color: "var(--text-muted)", fontSize: 10 }}>d{step.depth}</span>
              </button>
              {i < lineage.length - 1 && (
                <Icons.ChevronRight size={11} stroke="var(--text-muted)" />
              )}
            </Fragment>
          ))}
        </div>
      </DrawerSection>

      {(isReject || isFail) && (
        <div
          style={{
            margin: "12px 16px",
            padding: "10px 12px",
            background: isReject ? "var(--status-rejected-bg)" : "var(--status-failed-bg)",
            border: `1px solid ${isReject ? "var(--status-rejected)" : "var(--status-failed)"}`,
            borderRadius: "var(--r-md)",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
            {isReject ? (
              <Icons.AlertTriangle size={13} stroke="var(--status-rejected)" />
            ) : (
              <Icons.XCircle size={13} stroke="var(--status-failed)" />
            )}
            <span
              style={{
                fontSize: 11,
                fontWeight: 600,
                letterSpacing: 0.3,
                textTransform: "uppercase",
                color: isReject ? "var(--status-rejected)" : "var(--status-failed)",
              }}
            >
              {isReject ? `Rejected — ${run.rejection_reason}` : "Failed"}
            </span>
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 11.5,
              color: "var(--text-primary)",
              lineHeight: 1.5,
            }}
          >
            {run.error || "—"}
          </div>
        </div>
      )}

      <DrawerSection title="Task">
        <div
          style={{
            padding: 10,
            background: "var(--bg-input)",
            border: "1px solid var(--border-subtle)",
            borderRadius: "var(--r-sm)",
            fontFamily: "var(--font-mono)",
            fontSize: 11.5,
            color: "var(--text-primary)",
            lineHeight: 1.5,
            marginBottom: 10,
          }}
        >
          {run.task_input || "—"}
        </div>
        {run.task_metadata && Object.keys(run.task_metadata).length > 0 && (
          <KVTable data={run.task_metadata} />
        )}
      </DrawerSection>

      <DrawerSection title={`Timeline · ${timeline.length} events`}>
        {timeline.length === 0 ? (
          <div
            style={{
              padding: 12,
              fontSize: 11.5,
              color: "var(--text-tertiary)",
              fontStyle: "italic",
            }}
          >
            No events in the store for this trace yet.
          </div>
        ) : (
          <div>
            {timeline.map((ev, i) => (
              <TimelineEntry key={i} {...ev} />
            ))}
          </div>
        )}
      </DrawerSection>

      <DrawerSection title={isFail ? "Error trace" : "Output"} defaultOpen={false}>
        <pre
          style={{
            margin: 0,
            padding: 10,
            background: "var(--bg-input)",
            border: "1px solid var(--border-subtle)",
            borderRadius: "var(--r-sm)",
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            color: "var(--text-primary)",
            overflowX: "auto",
            lineHeight: 1.5,
          }}
        >
          {isFail && run.error
            ? run.error
            : JSON.stringify(
                {
                  status: run.status,
                  trace_id: run.trace_id,
                  agent: run.agent_name,
                  duration_ms: run.duration_ms,
                  tokens_used: run.tokens_used,
                },
                null,
                2,
              )}
        </pre>
        {!isFail && (
          <div
            style={{
              marginTop: 6,
              fontSize: 10.5,
              color: "var(--text-muted)",
              fontStyle: "italic",
            }}
          >
            Output payload is not captured in the event stream — only
            metadata. Wire a custom emitter or tool to record full output
            if you need it.
          </div>
        )}
      </DrawerSection>

      <DrawerSection title="Cost estimate" defaultOpen={false}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, fontSize: 12 }}>
          <div>
            <div
              style={{
                fontSize: 10.5,
                color: "var(--text-muted)",
                textTransform: "uppercase",
                letterSpacing: 0.4,
                marginBottom: 4,
              }}
            >
              Tokens
            </div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 16, color: "var(--text-primary)" }}>
              {fmt.tokens(run.tokens_used)}
            </div>
          </div>
          <div>
            <div
              style={{
                fontSize: 10.5,
                color: "var(--text-muted)",
                textTransform: "uppercase",
                letterSpacing: 0.4,
                marginBottom: 4,
              }}
            >
              Estimate
            </div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 16, color: "var(--text-primary)" }}>
              ${(run.tokens_used * 0.000015).toFixed(3)}
            </div>
          </div>
        </div>
        <div
          style={{
            fontSize: 10.5,
            color: "var(--text-muted)",
            marginTop: 8,
            fontStyle: "italic",
          }}
        >
          Indicative only — applies a flat $0.000015/token rate. The
          server doesn't know which model produced these tokens, so any
          number here is a rough upper bound. Use the cost-tracking
          middleware on the runtime if you need exact figures.
        </div>
      </DrawerSection>
    </aside>
  );
}
