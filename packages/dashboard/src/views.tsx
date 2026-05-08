import { useEffect, useMemo, useState } from "react";
import { Icons } from "./icons";
import { useRunsPage } from "./hooks";
import type { RunsStatusFilter } from "./hooks";
import type { WireRunSummary } from "./api";
import {
  Card,
  FilterChip,
  StatusDot,
  Gauge,
  Sparkline,
  HBar,
  BackendTag,
  fmt,
} from "./primitives";
import { CascadeTree } from "./tree";
import { EventStream } from "./event-stream";
import { RunTable } from "./run-table";
import type {
  Run,
  FlatRun,
  EventEntry,
  RunStatus,
  RuntimeInfo,
  Worker,
  MCPServer,
  ErrorGroup,
  RejectionCounts,
} from "./types";

type StatusFilter = "all" | RunStatus;

export function LiveView({
  runs,
  events,
  selectedTraceId,
  onSelect,
}: {
  runs: Run[];
  events: EventEntry[];
  selectedTraceId: string | null;
  onSelect: (r: Run) => void;
}) {
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [activeRoot, setActiveRoot] = useState<string | undefined>(runs[0]?.trace_id);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");

  function toggle(id: string) {
    const next = new Set(collapsed);
    next.has(id) ? next.delete(id) : next.add(id);
    setCollapsed(next);
  }

  const root = runs.find((r) => r.trace_id === activeRoot) || runs[0];
  const liveRunsFlat = useMemo<FlatRun[]>(() => {
    const out: FlatRun[] = [];
    function walk(node: Run, parent: string | null) {
      out.push({ ...node, parent_agent: parent, started_at: node.started_at || "—" });
      (node.children || []).forEach((c) => walk(c, node.agent_name));
    }
    runs.forEach((r) => walk(r, null));
    return out;
  }, [runs]);

  const visibleRuns = useMemo<FlatRun[]>(
    () =>
      statusFilter === "all"
        ? liveRunsFlat
        : liveRunsFlat.filter((r) => r.status === statusFilter),
    [liveRunsFlat, statusFilter],
  );

  // Client-side pagination — keeps the recent-runs card a fixed height
  // so it never elbows the cascading-spawn tree or the event stream.
  const RECENT_PAGE_SIZES = [10, 25, 50] as const;
  const [recentPageSize, setRecentPageSize] = useState<number>(10);
  const [recentPage, setRecentPage] = useState(0);
  useEffect(() => {
    setRecentPage(0);
  }, [statusFilter, visibleRuns.length]);
  const totalRecent = visibleRuns.length;
  const totalRecentPages = Math.max(1, Math.ceil(totalRecent / recentPageSize));
  const safeRecentPage = Math.min(recentPage, totalRecentPages - 1);
  const recentRangeStart =
    totalRecent === 0 ? 0 : safeRecentPage * recentPageSize + 1;
  const recentRangeEnd = Math.min(
    recentRangeStart + recentPageSize - 1,
    totalRecent,
  );
  const paginatedRuns = visibleRuns.slice(
    safeRecentPage * recentPageSize,
    safeRecentPage * recentPageSize + recentPageSize,
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 360px",
          gap: 12,
          padding: 12,
          minHeight: 0,
          flex: 1,
        }}
      >
        <Card
          padded={false}
          title={
            <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              <Icons.GitBranch size={11} stroke="currentColor" />
              Cascading-spawn tree
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
                {root?.agent_name}
              </span>
            </span>
          }
          action={
            <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
              {runs.map((r) => (
                <button
                  key={r.trace_id}
                  onClick={() => setActiveRoot(r.trace_id)}
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 4,
                    padding: "2px 8px",
                    height: 22,
                    background: activeRoot === r.trace_id ? "var(--accent-bg)" : "transparent",
                    color: activeRoot === r.trace_id ? "var(--accent)" : "var(--text-tertiary)",
                    border: `1px solid ${activeRoot === r.trace_id ? "var(--accent-border)" : "var(--border-default)"}`,
                    borderRadius: "var(--r-sm)",
                    fontFamily: "var(--font-mono)",
                    fontSize: 10.5,
                    cursor: "pointer",
                    whiteSpace: "nowrap",
                  }}
                  title={r.trace_id}
                >
                  <StatusDot status={r.status} pulse={r.status === "running"} />
                  {r.agent_name}
                </button>
              ))}
            </div>
          }
          style={{ overflow: "hidden", display: "flex", flexDirection: "column" }}
        >
          <div
            style={{
              flex: 1,
              overflow: "auto",
              background:
                "radial-gradient(circle at 1px 1px, var(--border-subtle) 1px, transparent 0)",
              backgroundSize: "24px 24px",
              backgroundColor: "var(--bg-base)",
              minHeight: 0,
              position: "relative",
            }}
          >
            {root && (
              <CascadeTree
                root={root}
                selectedId={selectedTraceId}
                onSelect={onSelect}
                collapsedSet={collapsed}
                onToggleCollapse={toggle}
              />
            )}
          </div>
        </Card>

        <EventStream events={events} height="100%" />
      </div>

      <div
        style={{
          padding: "0 12px 12px",
          flex: "0 0 auto",
          minHeight: 0,
        }}
      >
        <Card
          padded={false}
          style={{
            height: 320,
            display: "flex",
            flexDirection: "column",
            overflow: "hidden",
          }}
          title={
            <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              <Icons.Layers size={11} stroke="currentColor" />
              Recent runs
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
                last 1h · {liveRunsFlat.filter((r) => r.depth === 0).length} top-level
              </span>
            </span>
          }
          action={
            <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
              <FilterChip
                label="all"
                active={statusFilter === "all"}
                count={liveRunsFlat.length}
                onClick={() => setStatusFilter("all")}
              />
              <FilterChip
                label="running"
                color="var(--status-running)"
                active={statusFilter === "running"}
                count={liveRunsFlat.filter((r) => r.status === "running").length}
                onClick={() => setStatusFilter("running")}
              />
              <FilterChip
                label="completed"
                color="var(--status-completed)"
                active={statusFilter === "completed"}
                count={liveRunsFlat.filter((r) => r.status === "completed").length}
                onClick={() => setStatusFilter("completed")}
              />
              <FilterChip
                label="rejected"
                color="var(--status-rejected)"
                active={statusFilter === "rejected"}
                count={liveRunsFlat.filter((r) => r.status === "rejected").length}
                onClick={() => setStatusFilter("rejected")}
              />
              <FilterChip
                label="failed"
                color="var(--status-failed)"
                active={statusFilter === "failed"}
                count={liveRunsFlat.filter((r) => r.status === "failed").length}
                onClick={() => setStatusFilter("failed")}
              />
            </div>
          }
        >
          <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
            <RunTable
              rows={paginatedRuns}
              onRowClick={onSelect}
              selectedTraceId={selectedTraceId}
              density="comfortable"
              columns={[
                "started_at",
                "agent_name",
                "status",
                "depth",
                "parent_agent",
                "backend",
                "trust_level",
                "duration_ms",
                "tokens_used",
                "trace_id",
              ]}
            />
          </div>
          <div
            style={{
              flex: "0 0 auto",
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              gap: 8,
              padding: "6px 12px",
              borderTop: "1px solid var(--border-subtle)",
              fontSize: 11,
              color: "var(--text-tertiary)",
              background: "var(--bg-raised)",
            }}
          >
            <label
              style={{ display: "inline-flex", alignItems: "center", gap: 4 }}
            >
              <span style={{ color: "var(--text-muted)" }}>per page</span>
              <select
                value={recentPageSize}
                onChange={(e) => setRecentPageSize(Number(e.target.value))}
                style={{
                  background: "var(--bg-input)",
                  color: "var(--text-secondary)",
                  border: "1px solid var(--border-default)",
                  borderRadius: "var(--r-sm)",
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                  padding: "1px 4px",
                }}
              >
                {RECENT_PAGE_SIZES.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
            <span style={{ fontFamily: "var(--font-mono)" }}>
              {totalRecent === 0
                ? "0 of 0"
                : `${recentRangeStart}–${recentRangeEnd} of ${totalRecent}`}
            </span>
            <button
              onClick={() => setRecentPage(Math.max(0, safeRecentPage - 1))}
              disabled={safeRecentPage === 0}
              style={{
                width: 22,
                height: 22,
                background: "transparent",
                color:
                  safeRecentPage === 0
                    ? "var(--text-muted)"
                    : "var(--text-secondary)",
                border: "1px solid var(--border-default)",
                borderRadius: "var(--r-sm)",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: safeRecentPage === 0 ? "not-allowed" : "pointer",
                opacity: safeRecentPage === 0 ? 0.5 : 1,
              }}
            >
              <Icons.ChevronLeft size={11} />
            </button>
            <span style={{ fontFamily: "var(--font-mono)" }}>
              {safeRecentPage + 1} / {totalRecentPages}
            </span>
            <button
              onClick={() =>
                setRecentPage(Math.min(totalRecentPages - 1, safeRecentPage + 1))
              }
              disabled={safeRecentPage >= totalRecentPages - 1}
              style={{
                width: 22,
                height: 22,
                background: "transparent",
                color:
                  safeRecentPage >= totalRecentPages - 1
                    ? "var(--text-muted)"
                    : "var(--text-secondary)",
                border: "1px solid var(--border-default)",
                borderRadius: "var(--r-sm)",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor:
                  safeRecentPage >= totalRecentPages - 1
                    ? "not-allowed"
                    : "pointer",
                opacity: safeRecentPage >= totalRecentPages - 1 ? 0.5 : 1,
              }}
            >
              <Icons.ChevronRight size={11} />
            </button>
          </div>
        </Card>
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// History tab — backend-paginated /runs.
// ---------------------------------------------------------------------------

const HISTORY_PAGE_SIZES = [25, 50, 100, 200] as const;

function summaryToFlatRun(s: WireRunSummary): FlatRun {
  return {
    trace_id: s.trace_id,
    agent_name: s.agent_name,
    parent_trace_id: null,
    depth: s.depth,
    status: s.status,
    trust_level: s.trust_level,
    backend: s.backend,
    tokens_used: s.tokens_used,
    duration_ms: s.duration_ms,
    started_at: s.started_at
      ? new Date(s.started_at).toLocaleTimeString("en-GB", { hour12: false })
      : "—",
    parent_agent: s.parent_agent,
    children: [],
  };
}

export function HistoryView({
  selectedTraceId,
  onSelect,
}: {
  selectedTraceId: string | null;
  onSelect: (r: Run) => void;
}) {
  const {
    page,
    loading,
    error,
    pageSize,
    pageIndex,
    status,
    setPageSize,
    setPageIndex,
    setStatus,
  } = useRunsPage();

  const totalRows = page?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const safePage = Math.min(pageIndex, totalPages - 1);
  const rangeStart = totalRows === 0 ? 0 : safePage * pageSize + 1;
  const rangeEnd = Math.min(rangeStart + (page?.rows.length ?? 0) - 1, totalRows);
  const flatRows: FlatRun[] = (page?.rows ?? []).map(summaryToFlatRun);

  function chip(label: string, value: RunsStatusFilter, color?: string) {
    return (
      <FilterChip
        label={label}
        color={color}
        active={status === value}
        onClick={() => setStatus(value)}
      />
    );
  }

  return (
    <div style={{ padding: 12, height: "100%", overflowY: "auto" }}>
      <Card
        padded
        style={{ marginBottom: 12 }}
        title={
          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <Icons.Filter size={11} /> Filters
          </span>
        }
      >
        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
          {chip("all", "all")}
          {chip("running", "running", "var(--status-running)")}
          {chip("completed", "completed", "var(--status-completed)")}
          {chip("failed", "failed", "var(--status-failed)")}
          {chip("rejected", "rejected", "var(--status-rejected)")}
          {chip("spawned", "spawned", "var(--status-spawned)")}
        </div>
      </Card>

      <Card
        padded={false}
        title={
          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <Icons.Database size={11} /> Runs
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
              {error
                ? `error: ${error}`
                : loading && page === null
                ? "loading…"
                : `${totalRows} match${totalRows === 1 ? "" : "es"}`}
            </span>
          </span>
        }
        action={
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontSize: 11,
              color: "var(--text-tertiary)",
            }}
          >
            <label style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              <span style={{ color: "var(--text-muted)" }}>per page</span>
              <select
                value={pageSize}
                onChange={(e) => setPageSize(Number(e.target.value))}
                style={{
                  background: "var(--bg-input)",
                  color: "var(--text-secondary)",
                  border: "1px solid var(--border-default)",
                  borderRadius: "var(--r-sm)",
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                  padding: "1px 4px",
                }}
              >
                {HISTORY_PAGE_SIZES.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
            <span style={{ fontFamily: "var(--font-mono)" }}>
              {totalRows === 0
                ? "0 of 0"
                : `${rangeStart}–${rangeEnd} of ${totalRows}`}
            </span>
            <button
              onClick={() => setPageIndex(Math.max(0, safePage - 1))}
              disabled={safePage === 0}
              style={{
                width: 22,
                height: 22,
                background: "transparent",
                color: safePage === 0 ? "var(--text-muted)" : "var(--text-secondary)",
                border: "1px solid var(--border-default)",
                borderRadius: "var(--r-sm)",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: safePage === 0 ? "not-allowed" : "pointer",
                opacity: safePage === 0 ? 0.5 : 1,
              }}
            >
              <Icons.ChevronLeft size={11} />
            </button>
            <span style={{ fontFamily: "var(--font-mono)" }}>
              {safePage + 1} / {totalPages}
            </span>
            <button
              onClick={() => setPageIndex(Math.min(totalPages - 1, safePage + 1))}
              disabled={safePage >= totalPages - 1}
              style={{
                width: 22,
                height: 22,
                background: "transparent",
                color:
                  safePage >= totalPages - 1
                    ? "var(--text-muted)"
                    : "var(--text-secondary)",
                border: "1px solid var(--border-default)",
                borderRadius: "var(--r-sm)",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: safePage >= totalPages - 1 ? "not-allowed" : "pointer",
                opacity: safePage >= totalPages - 1 ? 0.5 : 1,
              }}
            >
              <Icons.ChevronRight size={11} />
            </button>
          </div>
        }
      >
        <RunTable
          rows={flatRows}
          onRowClick={onSelect}
          selectedTraceId={selectedTraceId}
          density="compact"
        />
      </Card>
    </div>
  );
}


export function HealthView({
  runtime,
  workers,
  mcpServers,
  errorGroups,
  burnRate,
  rejectionCounts,
}: {
  runtime: RuntimeInfo;
  workers: Worker[];
  mcpServers: MCPServer[];
  errorGroups: ErrorGroup[];
  burnRate: number[];
  rejectionCounts: RejectionCounts;
}) {
  const burnPerMin = burnRate[burnRate.length - 1];
  const tokensRemaining = runtime.token_budget - runtime.tokens_used;
  const minsToEmpty = Math.round(tokensRemaining / burnPerMin);

  const rejectItems = [
    { label: "budget", value: rejectionCounts.budget, color: "var(--reject-budget)" },
    { label: "cycle", value: rejectionCounts.cycle, color: "var(--reject-cycle)" },
    { label: "timeout", value: rejectionCounts.timeout, color: "var(--reject-timeout)" },
    { label: "depth", value: rejectionCounts.depth, color: "var(--reject-depth)" },
    { label: "cap", value: rejectionCounts.cap, color: "var(--reject-cap)" },
  ];

  return (
    <div style={{ padding: 12, height: "100%", overflowY: "auto" }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
        <Card
          title={
            <span>
              <Icons.Coins size={11} style={{ verticalAlign: "-1px", marginRight: 4 }} /> Token budget
            </span>
          }
        >
          <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: 16, alignItems: "center" }}>
            <Gauge
              value={runtime.tokens_used}
              max={runtime.token_budget}
              size={120}
              thickness={10}
              label="used"
              sublabel={`${fmt.tokens(runtime.tokens_used)} / ${fmt.tokens(runtime.token_budget)}`}
            />
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
                Burn rate · last 60min
              </div>
              <div style={{ display: "flex", alignItems: "baseline", gap: 6, marginBottom: 4 }}>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 22, fontWeight: 600, color: "var(--text-primary)" }}>
                  {burnPerMin.toLocaleString()}
                </span>
                <span style={{ fontSize: 11, color: "var(--text-tertiary)" }}>tok/min</span>
                <span
                  style={{
                    marginLeft: "auto",
                    fontSize: 11,
                    color: "var(--status-rejected)",
                    fontFamily: "var(--font-mono)",
                  }}
                >
                  ↑ trending up
                </span>
              </div>
              <Sparkline data={burnRate} width={300} height={56} />
              <div
                style={{
                  marginTop: 8,
                  padding: "6px 10px",
                  background: "var(--status-rejected-bg)",
                  border: "1px solid var(--status-rejected)",
                  borderRadius: "var(--r-sm)",
                }}
              >
                <span style={{ fontSize: 11, color: "var(--text-primary)" }}>
                  <span style={{ color: "var(--status-rejected)", fontWeight: 600 }}>Projection:</span> hits limit in{" "}
                  <span style={{ fontFamily: "var(--font-mono)" }}>~{minsToEmpty}m</span> at current rate
                </span>
              </div>
            </div>
          </div>
        </Card>

        <Card
          title={
            <span>
              <Icons.Spawn size={11} style={{ verticalAlign: "-1px", marginRight: 4 }} /> Spawn cap
            </span>
          }
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div>
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 4 }}>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 600, color: "var(--text-primary)" }}>
                  {runtime.spawn_count.toLocaleString()}
                  <span style={{ color: "var(--text-muted)", fontSize: 13 }}>
                    {" "}
                    / {runtime.max_total_spawns.toLocaleString()}
                  </span>
                </span>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-tertiary)" }}>
                  {Math.round((runtime.spawn_count / runtime.max_total_spawns) * 100)}%
                </span>
              </div>
              <div style={{ height: 6, background: "var(--bg-input)", borderRadius: 3, overflow: "hidden" }}>
                <div
                  style={{
                    width: `${(runtime.spawn_count / runtime.max_total_spawns) * 100}%`,
                    height: "100%",
                    background: "var(--accent)",
                  }}
                />
              </div>
            </div>
            <div>
              <div
                style={{
                  fontSize: 10.5,
                  color: "var(--text-muted)",
                  textTransform: "uppercase",
                  letterSpacing: 0.4,
                  marginBottom: 6,
                }}
              >
                Recent rejections by reason · 24h
              </div>
              <HBar items={rejectItems} />
            </div>
          </div>
        </Card>
      </div>

      <Card
        padded={false}
        style={{ marginBottom: 12 }}
        title={
          <span>
            <Icons.Server size={11} style={{ verticalAlign: "-1px", marginRight: 4 }} /> Worker fleet · {workers.length}
          </span>
        }
        action={
          <span style={{ fontSize: 11, color: "var(--text-tertiary)" }}>
            {workers.filter((w) => w.status === "healthy").length} healthy ·{" "}
            {workers.filter((w) => w.status === "stale").length} stale
          </span>
        }
      >
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ background: "var(--bg-raised)", borderBottom: "1px solid var(--border-default)" }}>
              {["Worker ID", "Broker", "Subscribed agents", "Concurrency", "In-flight", "Last heartbeat", ""].map((h) => (
                <th
                  key={h}
                  style={{
                    padding: "6px 12px",
                    textAlign: "left",
                    fontSize: 10.5,
                    fontWeight: 600,
                    letterSpacing: 0.4,
                    textTransform: "uppercase",
                    color: "var(--text-tertiary)",
                  }}
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {workers.map((w) => {
              const cap = w.in_flight / w.concurrency;
              const capColor =
                cap > 0.85 ? "var(--status-failed)" : cap > 0.6 ? "var(--status-rejected)" : "var(--status-completed)";
              return (
                <tr key={w.id} style={{ height: 36, borderBottom: "1px solid var(--border-subtle)" }}>
                  <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--text-primary)" }}>
                    {w.id}
                  </td>
                  <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-tertiary)" }}>
                    {w.broker}
                  </td>
                  <td style={{ padding: "0 12px" }}>
                    <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                      {w.subscribed.map((a) => (
                        <span
                          key={a}
                          style={{
                            padding: "0 6px",
                            height: 18,
                            background: "var(--bg-input)",
                            border: "1px solid var(--border-default)",
                            borderRadius: "var(--r-sm)",
                            fontFamily: "var(--font-mono)",
                            fontSize: 10.5,
                            color: "var(--text-secondary)",
                            display: "inline-flex",
                            alignItems: "center",
                          }}
                        >
                          {a}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}>
                    {w.concurrency}
                  </td>
                  <td style={{ padding: "0 12px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: capColor, fontWeight: 600 }}>
                        {w.in_flight}
                      </span>
                      <div style={{ width: 64, height: 4, background: "var(--bg-input)", borderRadius: 2, overflow: "hidden" }}>
                        <div style={{ width: `${cap * 100}%`, height: "100%", background: capColor }} />
                      </div>
                    </div>
                  </td>
                  <td style={{ padding: "0 12px" }}>
                    <span
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: 4,
                        fontSize: 11.5,
                        color: w.status === "stale" ? "var(--status-rejected)" : "var(--text-secondary)",
                      }}
                    >
                      <span
                        style={{
                          width: 6,
                          height: 6,
                          borderRadius: "50%",
                          background: w.status === "stale" ? "var(--status-rejected)" : "var(--status-completed)",
                        }}
                      />
                      {w.last_hb}
                    </span>
                  </td>
                  <td style={{ padding: "0 12px", textAlign: "right" }}>
                    <Icons.ChevronRight size={12} stroke="var(--text-muted)" />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card
          padded={false}
          title={
            <span>
              <Icons.Wrench size={11} style={{ verticalAlign: "-1px", marginRight: 4 }} /> MCP servers · {mcpServers.length}
            </span>
          }
        >
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--bg-raised)", borderBottom: "1px solid var(--border-default)" }}>
                {["Server", "Mode", "Tools", "Eager", "Status"].map((h) => (
                  <th
                    key={h}
                    style={{
                      padding: "6px 12px",
                      textAlign: "left",
                      fontSize: 10.5,
                      fontWeight: 600,
                      letterSpacing: 0.4,
                      textTransform: "uppercase",
                      color: "var(--text-tertiary)",
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {mcpServers.map((s) => {
                const sColor =
                  s.status === "connected"
                    ? "var(--status-completed)"
                    : s.status === "degraded"
                    ? "var(--status-rejected)"
                    : "var(--status-failed)";
                return (
                  <tr key={s.name} style={{ height: 32, borderBottom: "1px solid var(--border-subtle)" }}>
                    <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--text-primary)" }}>
                      {s.name}
                    </td>
                    <td style={{ padding: "0 12px" }}>
                      <BackendTag backend={s.mode} />
                    </td>
                    <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}>
                      {s.tools_count}
                    </td>
                    <td style={{ padding: "0 12px" }}>
                      {s.eager ? (
                        <Icons.Check size={12} stroke="var(--status-completed)" />
                      ) : (
                        <Icons.Minus size={12} stroke="var(--text-muted)" />
                      )}
                    </td>
                    <td style={{ padding: "0 12px" }}>
                      <span
                        style={{
                          display: "inline-flex",
                          alignItems: "center",
                          gap: 5,
                          fontSize: 11.5,
                          color: sColor,
                        }}
                      >
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: sColor }} />
                        {s.status}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Card>

        <Card
          padded={false}
          title={
            <span>
              <Icons.AlertOctagon size={11} style={{ verticalAlign: "-1px", marginRight: 4 }} /> Errors · 24h
            </span>
          }
        >
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--bg-raised)", borderBottom: "1px solid var(--border-default)" }}>
                {["Error class", "Count", "Most recent", "Top agent"].map((h) => (
                  <th
                    key={h}
                    style={{
                      padding: "6px 12px",
                      textAlign: "left",
                      fontSize: 10.5,
                      fontWeight: 600,
                      letterSpacing: 0.4,
                      textTransform: "uppercase",
                      color: "var(--text-tertiary)",
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {errorGroups.map((g) => (
                <tr key={g.error_class} style={{ height: 32, borderBottom: "1px solid var(--border-subtle)" }}>
                  <td style={{ padding: "0 12px" }}>
                    <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--status-failed)" }}>
                      {g.error_class}
                    </span>
                  </td>
                  <td
                    style={{
                      padding: "0 12px",
                      fontFamily: "var(--font-mono)",
                      fontSize: 11.5,
                      color: "var(--text-primary)",
                      fontWeight: 600,
                    }}
                  >
                    {g.count_24h}
                  </td>
                  <td style={{ padding: "0 12px", fontSize: 11.5, color: "var(--text-tertiary)" }}>{g.last}</td>
                  <td style={{ padding: "0 12px", fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}>
                    {g.top_agent}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </div>
    </div>
  );
}
