import { useMemo, useState } from "react";
import { Icons } from "./icons";
import { Card, FilterChip, EventTypePill, SEVERITY_META } from "./primitives";
import type { EventEntry, EventType, Severity } from "./types";

function EventRow({ ev }: { ev: EventEntry }) {
  const sevColor = SEVERITY_META[ev.severity].color;
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "70px 60px 1fr",
        gap: 6,
        padding: "4px 10px",
        borderBottom: "1px solid var(--border-subtle)",
        cursor: "pointer",
        fontSize: 11.5,
        lineHeight: 1.45,
        borderLeft: `2px solid ${ev.severity === "info" ? "transparent" : sevColor}`,
        transition: "background 80ms",
      }}
      onMouseEnter={(e) => (e.currentTarget.style.background = "var(--bg-hover)")}
      onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 10.5,
          color: "var(--text-muted)",
          whiteSpace: "nowrap",
        }}
      >
        {ev.ts.split(" ")[0]}
      </div>
      <div>
        <EventTypePill type={ev.type} />
      </div>
      <div style={{ minWidth: 0 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 11.5,
              color: "var(--text-primary)",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {ev.agent}
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--text-muted)" }}>
            {ev.trace}
          </span>
        </div>
        <div
          style={{
            color: "var(--text-tertiary)",
            fontSize: 11,
            fontFamily: "var(--font-mono)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {ev.preview}
        </div>
      </div>
    </div>
  );
}

const TYPE_OPTIONS: { v: EventType; label: string; c: string }[] = [
  { v: "agent_spawned", label: "spawn", c: "var(--accent)" },
  { v: "agent_completed", label: "done", c: "var(--status-completed)" },
  { v: "spawn_rejected", label: "reject", c: "var(--status-rejected)" },
  { v: "tool_call_started", label: "tool", c: "var(--text-tertiary)" },
  { v: "tool_call_failed", label: "fail", c: "var(--status-failed)" },
];

const SEV_OPTIONS: { v: Severity; label: string; c: string }[] = [
  { v: "info", label: "info", c: "var(--sev-info)" },
  { v: "warn", label: "warn", c: "var(--sev-warn)" },
  { v: "error", label: "error", c: "var(--sev-error)" },
];

export function EventStream({ events, height }: { events: EventEntry[]; height?: string | number }) {
  const [paused, setPaused] = useState(false);
  const [typeFilter, setTypeFilter] = useState<Set<EventType>>(new Set());
  const [sevFilter, setSevFilter] = useState<Set<Severity>>(new Set());

  function toggle<T>(set: Set<T>, setter: (s: Set<T>) => void, v: T) {
    const next = new Set(set);
    next.has(v) ? next.delete(v) : next.add(v);
    setter(next);
  }

  const filtered = useMemo(() => {
    return events.filter((e) => {
      if (typeFilter.size > 0 && !typeFilter.has(e.type)) return false;
      if (sevFilter.size > 0 && !sevFilter.has(e.severity)) return false;
      return true;
    });
  }, [events, typeFilter, sevFilter]);

  return (
    <Card
      padded={false}
      style={{ display: "flex", flexDirection: "column", height }}
      title={
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
          <Icons.Activity size={11} stroke="currentColor" />
          Live event stream
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 10.5,
              fontWeight: 500,
              color: "var(--text-muted)",
              textTransform: "none",
              letterSpacing: 0,
              marginLeft: 4,
            }}
          >
            {filtered.length} / {events.length}
          </span>
        </span>
      }
      action={
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <button
            onClick={() => setPaused(!paused)}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              padding: "2px 8px",
              height: 22,
              background: paused ? "var(--status-rejected-bg)" : "transparent",
              color: paused ? "var(--status-rejected)" : "var(--text-secondary)",
              border: "1px solid var(--border-default)",
              borderRadius: "var(--r-sm)",
              fontSize: 11,
              cursor: "pointer",
            }}
          >
            {paused ? <Icons.Play size={10} /> : <Icons.Pause size={10} />}
            {paused ? "Paused" : "Live"}
          </button>
        </div>
      }
    >
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 4,
          padding: "8px 10px",
          borderBottom: "1px solid var(--border-subtle)",
          background: "var(--bg-raised)",
        }}
      >
        <span
          style={{
            fontSize: 10,
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: 0.4,
            marginRight: 4,
            alignSelf: "center",
          }}
        >
          type
        </span>
        {TYPE_OPTIONS.map((o) => (
          <FilterChip
            key={o.v}
            label={o.label}
            color={o.c}
            active={typeFilter.has(o.v)}
            onClick={() => toggle(typeFilter, setTypeFilter, o.v)}
          />
        ))}
      </div>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 4,
          padding: "6px 10px",
          borderBottom: "1px solid var(--border-subtle)",
          background: "var(--bg-raised)",
        }}
      >
        <span
          style={{
            fontSize: 10,
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: 0.4,
            marginRight: 4,
            alignSelf: "center",
          }}
        >
          severity
        </span>
        {SEV_OPTIONS.map((o) => (
          <FilterChip
            key={o.v}
            label={o.label}
            color={o.c}
            active={sevFilter.has(o.v)}
            onClick={() => toggle(sevFilter, setSevFilter, o.v)}
          />
        ))}
      </div>

      <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
        {filtered.map((ev, i) => (
          <EventRow key={i} ev={ev} />
        ))}
      </div>
    </Card>
  );
}
