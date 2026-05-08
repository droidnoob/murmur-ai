import type { ReactNode } from "react";
import { Icons } from "./icons";
import { GaugeBar, fmt } from "./primitives";
import type { ConnectionStatus, RuntimeInfo, Theme } from "./types";

export function ConnectionDot({ status }: { status: ConnectionStatus }) {
  const map: Record<string, { c: string; label: string; pulse: boolean }> = {
    connected: { c: "var(--status-completed)", label: "connected", pulse: false },
    reconnecting: { c: "var(--status-rejected)", label: "reconnecting…", pulse: true },
    failed: { c: "var(--status-failed)", label: "disconnected", pulse: false },
  };
  const m = map[status] || map.connected;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        fontSize: 11.5,
        color: "var(--text-secondary)",
      }}
    >
      <span
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: m.c,
          boxShadow: m.pulse ? `0 0 6px ${m.c}` : "none",
        }}
      />
      {m.label}
    </span>
  );
}

function HeaderMeter({
  icon,
  label,
  value,
  max,
  danger = 0.85,
  warn = 0.65,
}: {
  icon: ReactNode;
  label: string;
  value: number;
  max: number;
  danger?: number;
  warn?: number;
}) {
  const pct = Math.min(1, value / max);
  const color =
    pct >= danger ? "var(--status-failed)" : pct >= warn ? "var(--status-rejected)" : "var(--text-secondary)";
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "4px 10px",
        height: 28,
        background: "var(--bg-surface)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--r-md)",
      }}
    >
      {icon}
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: 5 }}>
          <span
            style={{
              fontSize: 10.5,
              color: "var(--text-tertiary)",
              textTransform: "uppercase",
              letterSpacing: 0.4,
            }}
          >
            {label}
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color, fontWeight: 600 }}>
            {fmt.tokens(value)}
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 10.5, color: "var(--text-muted)" }}>
            / {fmt.tokens(max)}
          </span>
        </div>
        <GaugeBar value={value} max={max} width={110} danger={danger} warn={warn} />
      </div>
    </div>
  );
}

export type Tab = "Live" | "History" | "Health";

export function GlobalHeader({
  runtime,
  theme,
  onThemeToggle,
  activeTab,
  onTabChange,
}: {
  runtime: RuntimeInfo;
  theme: Theme;
  onThemeToggle: () => void;
  activeTab: Tab;
  onTabChange: (t: Tab) => void;
}) {
  const tabs: Tab[] = ["Live", "History", "Health"];
  return (
    <header
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        height: "var(--header-h)",
        padding: "0 16px",
        background: "var(--bg-raised)",
        borderBottom: "1px solid var(--border-default)",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <div
          style={{
            width: 22,
            height: 22,
            background: "var(--accent)",
            borderRadius: 5,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "#fff",
            fontFamily: "var(--font-mono)",
            fontWeight: 700,
            fontSize: 13,
          }}
        >
          ~
        </div>
        <span
          style={{
            fontSize: 14,
            fontWeight: 600,
            letterSpacing: -0.2,
            color: "var(--text-primary)",
          }}
        >
          murmur
        </span>
        <span style={{ fontSize: 11, color: "var(--text-muted)", fontFamily: "var(--font-mono)" }}>
          v0.4.2
        </span>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          paddingLeft: 12,
          marginLeft: 4,
          borderLeft: "1px solid var(--border-default)",
        }}
      >
        <Icons.Cpu size={13} stroke="var(--text-tertiary)" />
        <span style={{ fontSize: 11, color: "var(--text-tertiary)" }}>runtime</span>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--text-primary)" }}>
          {runtime.id}
        </span>
        <ConnectionDot status={runtime.broker_status} />
      </div>

      <nav style={{ display: "flex", alignItems: "center", gap: 0, marginLeft: "auto" }}>
        {tabs.map((t) => {
          const active = t === activeTab;
          return (
            <button
              key={t}
              onClick={() => onTabChange(t)}
              style={{
                position: "relative",
                padding: "0 14px",
                height: "var(--header-h)",
                background: "transparent",
                color: active ? "var(--text-primary)" : "var(--text-tertiary)",
                border: "none",
                fontSize: 13,
                fontWeight: active ? 500 : 400,
                cursor: "pointer",
                letterSpacing: -0.1,
              }}
            >
              {t}
              {active && (
                <span
                  style={{
                    position: "absolute",
                    left: 12,
                    right: 12,
                    bottom: 0,
                    height: 2,
                    background: "var(--accent)",
                    borderRadius: 1,
                  }}
                />
              )}
            </button>
          );
        })}
      </nav>

      <div style={{ display: "flex", alignItems: "center", gap: 8, marginLeft: "auto" }}>
        <HeaderMeter
          icon={<Icons.Coins size={13} stroke="var(--text-tertiary)" />}
          label="tokens"
          value={runtime.tokens_used}
          max={runtime.token_budget}
        />
        <HeaderMeter
          icon={<Icons.Spawn size={13} stroke="var(--text-tertiary)" />}
          label="spawns"
          value={runtime.spawn_count}
          max={runtime.max_total_spawns}
        />

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "0 10px",
            height: 28,
            background: "var(--bg-surface)",
            border: "1px solid var(--border-default)",
            borderRadius: "var(--r-md)",
          }}
        >
          <Icons.Activity size={13} stroke="var(--text-tertiary)" />
          <ConnectionDot status={runtime.sse_status} />
        </div>

        <button
          onClick={onThemeToggle}
          title={theme === "dark" ? "Switch to light" : "Switch to dark"}
          style={{
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            width: 28,
            height: 28,
            background: "var(--bg-surface)",
            border: "1px solid var(--border-default)",
            borderRadius: "var(--r-md)",
            color: "var(--text-secondary)",
            cursor: "pointer",
          }}
        >
          {theme === "dark" ? <Icons.Moon size={13} /> : <Icons.Sun size={13} />}
        </button>
      </div>
    </header>
  );
}
