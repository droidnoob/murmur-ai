import { useEffect, useState } from "react";
import type { CSSProperties, ReactNode } from "react";
import { Icons } from "./icons";
import type { RunStatus, TrustLevel, RejectReason, Severity, EventType } from "./types";

interface Meta {
  color: string;
  bg?: string;
  label: string;
}

export const STATUS_META: Record<RunStatus, Meta> = {
  spawned: { color: "var(--status-spawned)", bg: "var(--status-spawned-bg)", label: "Spawned" },
  running: { color: "var(--status-running)", bg: "var(--status-running-bg)", label: "Running" },
  completed: { color: "var(--status-completed)", bg: "var(--status-completed-bg)", label: "Completed" },
  failed: { color: "var(--status-failed)", bg: "var(--status-failed-bg)", label: "Failed" },
  rejected: { color: "var(--status-rejected)", bg: "var(--status-rejected-bg)", label: "Rejected" },
};

export const TRUST_META: Record<TrustLevel, Meta> = {
  high: { color: "var(--trust-high)", bg: "var(--trust-high-bg)", label: "HIGH" },
  medium: { color: "var(--trust-medium)", bg: "var(--trust-medium-bg)", label: "MEDIUM" },
  low: { color: "var(--trust-low)", bg: "var(--trust-low-bg)", label: "LOW" },
  sandbox: { color: "var(--trust-sandbox)", bg: "var(--trust-sandbox-bg)", label: "SANDBOX" },
};

export const REJECT_META: Record<RejectReason, Meta> = {
  cycle: { color: "var(--reject-cycle)", label: "cycle" },
  depth: { color: "var(--reject-depth)", label: "depth" },
  cap: { color: "var(--reject-cap)", label: "cap" },
  budget: { color: "var(--reject-budget)", label: "budget" },
  timeout: { color: "var(--reject-timeout)", label: "timeout" },
};

export const SEVERITY_META: Record<Severity, Meta> = {
  info: { color: "var(--sev-info)", label: "INFO" },
  warn: { color: "var(--sev-warn)", label: "WARN" },
  error: { color: "var(--sev-error)", label: "ERROR" },
};

export function StatusDot({ status, pulse }: { status: RunStatus; pulse?: boolean }) {
  const m = STATUS_META[status] || STATUS_META.spawned;
  return (
    <span
      style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        background: m.color,
        boxShadow: pulse ? `0 0 0 0 ${m.color}` : "none",
        animation: pulse ? "pulse-dot 1.6s infinite" : "none",
        flexShrink: 0,
      }}
    />
  );
}

export function StatusPill({ status, size = "md" }: { status: RunStatus; size?: "sm" | "md" }) {
  const m = STATUS_META[status] || STATUS_META.spawned;
  const isPulse = status === "running";
  const padY = size === "sm" ? 1 : 2;
  const padX = size === "sm" ? 6 : 8;
  const fs = size === "sm" ? 11 : 11.5;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: `${padY}px ${padX}px`,
        background: m.bg,
        color: m.color,
        borderRadius: "var(--r-sm)",
        fontSize: fs,
        fontWeight: 500,
        lineHeight: 1.4,
        letterSpacing: 0.1,
        textTransform: "lowercase",
        whiteSpace: "nowrap",
      }}
    >
      <StatusDot status={status} pulse={isPulse} />
      {m.label.toLowerCase()}
    </span>
  );
}

export function TrustTag({ trust, size = "md" }: { trust: TrustLevel; size?: "sm" | "md" }) {
  const m = TRUST_META[trust] || TRUST_META.low;
  const fs = size === "sm" ? 9.5 : 10;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "1px 5px",
        background: m.bg,
        color: m.color,
        border: "1px solid transparent",
        borderRadius: "var(--r-sm)",
        fontFamily: "var(--font-mono)",
        fontSize: fs,
        fontWeight: 500,
        letterSpacing: 0.4,
        whiteSpace: "nowrap",
      }}
    >
      {m.label}
    </span>
  );
}

export function SeverityPill({ severity }: { severity: Severity }) {
  const m = SEVERITY_META[severity] || SEVERITY_META.info;
  return (
    <span
      style={{
        display: "inline-block",
        padding: "0 4px",
        color: m.color,
        borderLeft: `2px solid ${m.color}`,
        fontFamily: "var(--font-mono)",
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: 0.4,
        lineHeight: "14px",
      }}
    >
      {m.label}
    </span>
  );
}

export function RejectTag({ reason }: { reason: RejectReason }) {
  const m = REJECT_META[reason];
  if (!m) return null;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "1px 6px",
        background: "transparent",
        color: m.color,
        border: `1px solid ${m.color}`,
        borderRadius: "var(--r-sm)",
        fontSize: 10.5,
        fontWeight: 600,
        letterSpacing: 0.3,
        textTransform: "uppercase",
        fontFamily: "var(--font-mono)",
      }}
    >
      {m.label}
    </span>
  );
}

export function BackendTag({ backend }: { backend: string }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 3,
        padding: "1px 5px",
        color: "var(--text-tertiary)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--r-sm)",
        fontFamily: "var(--font-mono)",
        fontSize: 10,
        fontWeight: 500,
        letterSpacing: 0.3,
      }}
    >
      {backend}
    </span>
  );
}

const EVENT_TYPE_MAP: Record<string, { c: string; label: string }> = {
  agent_spawned: { c: "var(--accent)", label: "spawn" },
  agent_completed: { c: "var(--status-completed)", label: "done" },
  agent_failed: { c: "var(--status-failed)", label: "fail" },
  spawn_rejected: { c: "var(--status-rejected)", label: "rejct" },
  tool_call_started: { c: "var(--text-tertiary)", label: "tool↗" },
  tool_call_completed: { c: "var(--text-secondary)", label: "tool✓" },
  tool_call_failed: { c: "var(--status-failed)", label: "tool✕" },
  worker_started: { c: "var(--trust-medium)", label: "wkr+" },
};

export function EventTypePill({ type }: { type: EventType | string }) {
  const m = EVENT_TYPE_MAP[type] || { c: "var(--text-tertiary)", label: type };
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        minWidth: 48,
        height: 16,
        padding: "0 5px",
        background: "transparent",
        color: m.c,
        border: "1px solid currentColor",
        borderRadius: "var(--r-sm)",
        fontFamily: "var(--font-mono)",
        fontSize: 10,
        fontWeight: 500,
        letterSpacing: 0.2,
        whiteSpace: "nowrap",
      }}
    >
      {m.label}
    </span>
  );
}

export function IdBadge({
  id,
  length = 8,
  mono = true,
  label,
}: {
  id: string;
  length?: number;
  mono?: boolean;
  label?: string;
}) {
  const [copied, setCopied] = useState(false);
  const display = id.length > length + 3 ? `${id.slice(0, length)}…` : id;
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        navigator.clipboard?.writeText(id);
        setCopied(true);
        setTimeout(() => setCopied(false), 900);
      }}
      title={id}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "0 6px",
        height: 18,
        background: "var(--bg-input)",
        color: "var(--text-secondary)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--r-sm)",
        fontFamily: mono ? "var(--font-mono)" : "var(--font-sans)",
        fontSize: 10.5,
        cursor: "pointer",
        lineHeight: 1,
      }}
    >
      {label && <span style={{ color: "var(--text-muted)" }}>{label}</span>}
      <span>{display}</span>
      {copied ? (
        <Icons.Check size={10} stroke="var(--status-completed)" />
      ) : (
        <Icons.Copy size={10} stroke="var(--text-muted)" />
      )}
    </button>
  );
}

export function Gauge({
  value,
  max,
  size = 80,
  thickness = 6,
  label,
  sublabel,
  danger = 0.85,
  warn = 0.65,
}: {
  value: number;
  max: number;
  size?: number;
  thickness?: number;
  label?: string;
  sublabel?: string;
  danger?: number;
  warn?: number;
}) {
  const pct = Math.min(1, value / max);
  const r = (size - thickness) / 2;
  const circ = 2 * Math.PI * r;
  const dash = circ * 0.75;
  const offset = dash * (1 - pct);
  const color =
    pct >= danger ? "var(--status-failed)" : pct >= warn ? "var(--status-rejected)" : "var(--status-completed)";
  return (
    <div style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
      <div style={{ position: "relative", width: size, height: size * 0.75 }}>
        <svg
          width={size}
          height={size}
          style={{ transform: "rotate(135deg)", position: "absolute", top: 0, left: 0 }}
        >
          <circle
            cx={size / 2}
            cy={size / 2}
            r={r}
            fill="none"
            stroke="var(--border-default)"
            strokeWidth={thickness}
            strokeDasharray={`${dash} ${circ}`}
            strokeLinecap="round"
          />
          <circle
            cx={size / 2}
            cy={size / 2}
            r={r}
            fill="none"
            stroke={color}
            strokeWidth={thickness}
            strokeDasharray={`${dash} ${circ}`}
            strokeDashoffset={offset}
            strokeLinecap="round"
            style={{ transition: "stroke-dashoffset 600ms ease, stroke 200ms" }}
          />
        </svg>
        <div
          style={{
            position: "absolute",
            inset: 0,
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            paddingTop: size * 0.1,
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: size * 0.18,
              fontWeight: 600,
              color: "var(--text-primary)",
            }}
          >
            {Math.round(pct * 100)}
            <span style={{ fontSize: size * 0.11, color: "var(--text-tertiary)" }}>%</span>
          </div>
          {label && (
            <div style={{ fontSize: 10, color: "var(--text-tertiary)", marginTop: 1 }}>{label}</div>
          )}
        </div>
      </div>
      {sublabel && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--text-tertiary)" }}>
          {sublabel}
        </div>
      )}
    </div>
  );
}

export function GaugeBar({
  value,
  max,
  width = 80,
  danger = 0.85,
  warn = 0.65,
}: {
  value: number;
  max: number;
  width?: number;
  danger?: number;
  warn?: number;
}) {
  const pct = Math.min(1, value / max);
  const color =
    pct >= danger ? "var(--status-failed)" : pct >= warn ? "var(--status-rejected)" : "var(--accent)";
  return (
    <div style={{ width, height: 4, background: "var(--bg-input)", borderRadius: 2, overflow: "hidden" }}>
      <div style={{ width: `${pct * 100}%`, height: "100%", background: color, transition: "width 400ms" }} />
    </div>
  );
}

export function Sparkline({
  data,
  width = 240,
  height = 48,
  color = "var(--accent)",
  area = true,
  showAxis = false,
}: {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
  area?: boolean;
  showAxis?: boolean;
}) {
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width;
    const y = height - ((v - min) / range) * (height - 4) - 2;
    return [x, y] as const;
  });
  const path = points.map((p, i) => (i === 0 ? `M${p[0]},${p[1]}` : `L${p[0]},${p[1]}`)).join(" ");
  const areaPath = `${path} L${width},${height} L0,${height} Z`;
  return (
    <svg width={width} height={height} style={{ display: "block" }}>
      {area && <path d={areaPath} fill="var(--chart-area)" />}
      <path d={path} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
      {showAxis && (
        <>
          <text x={2} y={10} fill="var(--chart-axis)" fontSize={9} fontFamily="var(--font-mono)">
            {max}
          </text>
          <text x={2} y={height - 2} fill="var(--chart-axis)" fontSize={9} fontFamily="var(--font-mono)">
            {min}
          </text>
        </>
      )}
    </svg>
  );
}

export interface HBarItem {
  label: string;
  value: number;
  color: string;
}

export function HBar({ items }: { items: HBarItem[] }) {
  const max = Math.max(...items.map((i) => i.value));
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {items.map((it, idx) => (
        <div
          key={idx}
          style={{
            display: "grid",
            gridTemplateColumns: "64px 1fr 32px",
            alignItems: "center",
            gap: 8,
            fontSize: 12,
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 5,
              color: "var(--text-secondary)",
              fontFamily: "var(--font-mono)",
              fontSize: 11,
            }}
          >
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: it.color }} />
            {it.label}
          </div>
          <div style={{ height: 6, background: "var(--bg-input)", borderRadius: 2, overflow: "hidden" }}>
            <div style={{ width: `${(it.value / max) * 100}%`, height: "100%", background: it.color, opacity: 0.7 }} />
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 11,
              color: "var(--text-secondary)",
              textAlign: "right",
            }}
          >
            {it.value}
          </div>
        </div>
      ))}
    </div>
  );
}

export function FilterChip({
  label,
  count,
  active,
  onClick,
  color,
}: {
  label: string;
  count?: number;
  active?: boolean;
  onClick?: () => void;
  color?: string;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "3px 8px",
        height: 22,
        background: active ? "var(--accent-bg)" : "var(--bg-surface)",
        color: active ? "var(--accent)" : "var(--text-secondary)",
        border: `1px solid ${active ? "var(--accent-border)" : "var(--border-default)"}`,
        borderRadius: "var(--r-sm)",
        fontSize: 11.5,
        cursor: "pointer",
        whiteSpace: "nowrap",
        transition: "background 120ms, border-color 120ms, color 120ms",
      }}
    >
      {color && <span style={{ width: 6, height: 6, borderRadius: "50%", background: color }} />}
      {label}
      {count != null && (
        <span
          style={{
            color: active ? "var(--accent)" : "var(--text-tertiary)",
            fontFamily: "var(--font-mono)",
            fontSize: 10.5,
          }}
        >
          {count}
        </span>
      )}
    </button>
  );
}

export function Card({
  title,
  action,
  children,
  style,
  padded = true,
}: {
  title?: ReactNode;
  action?: ReactNode;
  children?: ReactNode;
  style?: CSSProperties;
  padded?: boolean;
}) {
  return (
    <section
      style={{
        background: "var(--bg-surface)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--r-md)",
        ...style,
      }}
    >
      {title && (
        <header
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "8px 12px",
            borderBottom: "1px solid var(--border-subtle)",
            minHeight: 36,
          }}
        >
          <h3
            style={{
              margin: 0,
              fontSize: 12,
              fontWeight: 600,
              letterSpacing: 0.3,
              textTransform: "uppercase",
              color: "var(--text-secondary)",
            }}
          >
            {title}
          </h3>
          {action}
        </header>
      )}
      <div style={{ padding: padded ? 12 : 0 }}>{children}</div>
    </section>
  );
}

export const fmt = {
  ms: (ms: number | null | undefined): string => {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
    return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  },
  tokens: (n: number | null | undefined): string => {
    if (n == null) return "—";
    if (n < 1000) return `${n}`;
    if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
    return `${(n / 1_000_000).toFixed(2)}M`;
  },
  trace: (id: string | null | undefined, n = 8): string =>
    id ? `${id.slice(0, n)}…${id.slice(-4)}` : "—",
};

export function useKeyframes() {
  useEffect(() => {
    if (document.getElementById("murmur-keyframes")) return;
    const s = document.createElement("style");
    s.id = "murmur-keyframes";
    s.textContent = `
      @keyframes pulse-dot {
        0%   { box-shadow: 0 0 0 0   rgba(59,130,246,0.6); }
        70%  { box-shadow: 0 0 0 5px rgba(59,130,246,0); }
        100% { box-shadow: 0 0 0 0   rgba(59,130,246,0); }
      }
      @keyframes node-enter {
        from { opacity: 0; transform: translateY(-4px); }
        to   { opacity: 1; transform: translateY(0); }
      }
      @keyframes drawer-in {
        from { transform: translateX(20px); opacity: 0; }
        to   { transform: translateX(0); opacity: 1; }
      }
    `;
    document.head.appendChild(s);
  }, []);
}
