import { useState } from "react";
import { GlobalHeader } from "./header";
import type { Tab } from "./header";
import { LiveView, HistoryView, HealthView } from "./views";
import { RunDetailDrawer } from "./drawer";
import { useKeyframes } from "./primitives";
import { useLiveData, useRunDetail } from "./hooks";
import {
  RUNS,
  EVENT_STREAM,
  WORKERS,
  MCP_SERVERS,
  ERROR_GROUPS,
  BURN_RATE,
  REJECTION_COUNTS,
  RUNTIME,
} from "./data";
import type {
  ConnectionStatus,
  ErrorGroup,
  MCPServer,
  RejectionCounts,
  Run,
  RuntimeInfo,
  Theme,
  Worker,
} from "./types";
import type { StatsResponse } from "./api";

function asConn(value: string): ConnectionStatus {
  return value === "connected" || value === "reconnecting" || value === "failed"
    ? value
    : "failed";
}

function statsToRuntimeInfo(stats: StatsResponse): RuntimeInfo {
  const r = stats.runtime;
  return {
    id: r.id,
    broker: r.broker ?? "(in-process)",
    broker_status: asConn(r.broker_status),
    sse_status: asConn(r.sse_status),
    spawn_count: r.spawn_count,
    max_total_spawns: r.max_total_spawns ?? 0,
    tokens_used: r.tokens_used,
    token_budget: r.token_budget ?? 0,
    workers_count: r.workers_count,
    mcp_count: r.mcp_count,
  };
}

function statsToWorkers(stats: StatsResponse): Worker[] {
  return stats.workers.map((w) => ({
    id: w.id,
    broker: w.broker,
    subscribed: w.subscribed,
    concurrency: w.concurrency,
    in_flight: w.in_flight,
    last_hb: w.last_hb,
    status: w.status === "stale" || w.status === "down" ? w.status : "healthy",
  }));
}

function statsToMcp(stats: StatsResponse): MCPServer[] {
  return stats.mcp_servers.map((m) => ({
    name: m.name,
    mode: m.mode,
    tools_count: m.tools_count,
    tools: m.tools,
    eager: m.eager,
    status: m.status,
  }));
}

function statsToErrorGroups(stats: StatsResponse): ErrorGroup[] {
  return stats.error_groups.map((g) => ({
    error_class: g.error_class,
    count_24h: g.count_24h,
    last: g.last,
    top_agent: g.top_agent,
  }));
}

function statsToRejection(stats: StatsResponse): RejectionCounts {
  return stats.rejection_counts;
}

function findRunByTraceId(roots: Run[], traceId: string): Run | null {
  function search(node: Run): Run | null {
    if (node.trace_id === traceId) return node;
    for (const c of node.children || []) {
      const r = search(c);
      if (r) return r;
    }
    return null;
  }
  for (const r of roots) {
    const f = search(r);
    if (f) return f;
  }
  return null;
}

function buildLineage(roots: Run[], traceId: string): Run[] {
  function search(node: Run, chain: Run[]): Run[] | null {
    const next = [...chain, node];
    if (node.trace_id === traceId) return next;
    for (const c of node.children || []) {
      const r = search(c, next);
      if (r) return r;
    }
    return null;
  }
  for (const r of roots) {
    const f = search(r, []);
    if (f) return f;
  }
  return [];
}

export function MurmurDashboard({
  initialTab = "Live",
  initialTraceId = null,
  theme: themeProp,
}: {
  initialTab?: Tab;
  initialTraceId?: string | null;
  theme?: Theme;
} = {}) {
  useKeyframes();
  const [theme, setTheme] = useState<Theme>(themeProp || "dark");
  const [activeTab, setActiveTab] = useState<Tab>(initialTab);
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(initialTraceId);

  const live = useLiveData();
  // Live mode = the server is reachable (we've seen events OR stats).
  // Demo mode keeps the design-reference mocks visible when the server is
  // unreachable.
  const isLive =
    live.mode === "live" && (live.runs.length > 0 || live.stats !== null);

  const sourceRuns: Run[] = isLive ? live.runs : RUNS;
  const events = isLive && live.events.length > 0 ? live.events : EVENT_STREAM;
  // ``eventsToRuns`` returns roots newest-first; show the top 12 so a
  // multi-stage group cascade stays visible alongside single-agent runs.
  const liveRuns = sourceRuns.slice(0, 12);

  const runtimeInfo: RuntimeInfo = live.stats !== null ? statsToRuntimeInfo(live.stats) : RUNTIME;
  const workers: Worker[] = live.stats !== null ? statsToWorkers(live.stats) : WORKERS;
  const mcpServers: MCPServer[] = live.stats !== null ? statsToMcp(live.stats) : MCP_SERVERS;
  const errorGroups: ErrorGroup[] =
    live.stats !== null ? statsToErrorGroups(live.stats) : ERROR_GROUPS;
  const rejectionCounts: RejectionCounts =
    live.stats !== null ? statsToRejection(live.stats) : REJECTION_COUNTS;
  const burnRate: number[] =
    live.stats !== null && live.stats.burn_rate.some((v) => v > 0)
      ? live.stats.burn_rate
      : BURN_RATE;

  // Try the in-memory ring first (covers Live-tab clicks instantly).
  // Fall back to /runs/{trace_id}/tree for History clicks whose events
  // have aged out of the SSE ring.
  const detail = useRunDetail(selectedTraceId, live.rawEvents);
  const ringRun = selectedTraceId
    ? findRunByTraceId(sourceRuns, selectedTraceId)
    : null;
  const selectedRun = ringRun ?? detail.run;
  const lineage = selectedTraceId
    ? ringRun
      ? buildLineage(sourceRuns, selectedTraceId)
      : selectedRun
        ? [selectedRun]
        : []
    : [];
  const drawerEvents =
    detail.events.length > 0
      ? detail.events
      : live.rawEvents.filter((e) => e.trace_id === selectedTraceId);

  function onSelect(run: Run) {
    setSelectedTraceId(run.trace_id);
  }

  return (
    <div
      className={`dashboard-root theme-${theme}`}
      style={{
        position: "relative",
        display: "flex",
        flexDirection: "column",
        height: "100%",
        width: "100%",
        background: "var(--bg-base)",
        color: "var(--text-primary)",
        overflow: "hidden",
      }}
    >
      <GlobalHeader
        runtime={runtimeInfo}
        theme={theme}
        onThemeToggle={() => setTheme(theme === "dark" ? "light" : "dark")}
        activeTab={activeTab}
        onTabChange={(t) => {
          setActiveTab(t);
          setSelectedTraceId(null);
        }}
      />
      {!isLive && (
        <div
          style={{
            padding: "4px 16px",
            background: "var(--accent-bg)",
            color: "var(--accent)",
            fontFamily: "var(--font-mono)",
            fontSize: 11,
            borderBottom: "1px solid var(--accent-border)",
          }}
        >
          {live.mode === "loading"
            ? "connecting to /events…"
            : "demo mode — server unreachable, showing mocked runs"}
        </div>
      )}

      <div style={{ flex: 1, minHeight: 0, position: "relative", display: "flex", flexDirection: "column" }}>
        {activeTab === "Live" && (
          <LiveView
            runs={liveRuns}
            events={events}
            selectedTraceId={selectedTraceId}
            onSelect={onSelect}
          />
        )}
        {activeTab === "History" && (
          <HistoryView selectedTraceId={selectedTraceId} onSelect={onSelect} />
        )}
        {activeTab === "Health" && (
          <HealthView
            runtime={runtimeInfo}
            workers={workers}
            mcpServers={mcpServers}
            errorGroups={errorGroups}
            burnRate={burnRate}
            rejectionCounts={rejectionCounts}
          />
        )}

        {selectedRun && (
          <RunDetailDrawer
            run={selectedRun}
            lineage={lineage}
            events={drawerEvents}
            onClose={() => setSelectedTraceId(null)}
          />
        )}
      </div>
    </div>
  );
}
