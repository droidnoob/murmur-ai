import { useMemo, useState } from "react";
import { Icons } from "./icons";
import { StatusPill, TrustTag, BackendTag, fmt } from "./primitives";
import type { FlatRun, Run } from "./types";

type SortKey = "started_at" | "agent_name" | "status" | "depth" | "duration_ms" | "tokens_used";

interface Column {
  key: keyof FlatRun | "trace_id" | "parent_agent";
  label: string;
  width: number;
  sortable: boolean;
  align: "left" | "right";
  mono?: boolean;
}

const ALL_COLUMNS: Column[] = [
  { key: "started_at", label: "Started", width: 84, sortable: true, align: "left", mono: true },
  { key: "agent_name", label: "Agent", width: 160, sortable: true, align: "left", mono: true },
  { key: "status", label: "Status", width: 100, sortable: true, align: "left" },
  { key: "depth", label: "Depth", width: 56, sortable: true, align: "right", mono: true },
  { key: "parent_agent", label: "Parent", width: 130, sortable: false, align: "left", mono: true },
  { key: "backend", label: "Backend", width: 70, sortable: false, align: "left" },
  { key: "trust_level", label: "Trust", width: 76, sortable: false, align: "left" },
  { key: "duration_ms", label: "Duration", width: 80, sortable: true, align: "right", mono: true },
  { key: "tokens_used", label: "Tokens", width: 72, sortable: true, align: "right", mono: true },
  { key: "trace_id", label: "Trace ID", width: 130, sortable: false, align: "left", mono: true },
];

export function RunTable({
  rows,
  onRowClick,
  selectedTraceId,
  density = "comfortable",
  columns: cols,
}: {
  rows: FlatRun[];
  onRowClick?: (r: Run) => void;
  selectedTraceId: string | null;
  density?: "comfortable" | "compact";
  columns?: string[];
}) {
  const [sortBy, setSortBy] = useState<string>("started_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const columns = cols ? ALL_COLUMNS.filter((c) => cols.includes(c.key as string)) : ALL_COLUMNS;
  const rowH = density === "compact" ? 28 : 32;

  const sorted = useMemo(() => {
    const arr = [...rows];
    arr.sort((a, b) => {
      const av = (a as unknown as Record<string, unknown>)[sortBy];
      const bv = (b as unknown as Record<string, unknown>)[sortBy];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number")
        return sortDir === "asc" ? av - bv : bv - av;
      return sortDir === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [rows, sortBy, sortDir]);

  function toggleSort(k: string) {
    if (sortBy === k) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else {
      setSortBy(k);
      setSortDir("desc");
    }
  }

  function renderCell(col: Column, row: FlatRun) {
    const v = (row as unknown as Record<string, unknown>)[col.key as string];
    switch (col.key) {
      case "status":
        return <StatusPill status={row.status} size="sm" />;
      case "trust_level":
        return <TrustTag trust={row.trust_level} size="sm" />;
      case "backend":
        return <BackendTag backend={row.backend} />;
      case "duration_ms":
        return fmt.ms(row.duration_ms);
      case "tokens_used":
        return fmt.tokens(row.tokens_used);
      case "trace_id":
        return fmt.trace(row.trace_id);
      case "depth":
        return <span style={{ color: "var(--text-tertiary)" }}>{row.depth}</span>;
      case "parent_agent":
        return row.parent_agent ? (
          <span style={{ color: "var(--text-secondary)" }}>{row.parent_agent}</span>
        ) : (
          <span style={{ color: "var(--text-muted)" }}>—</span>
        );
      case "agent_name":
        return (
          <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{row.agent_name}</span>
        );
      default:
        return v != null ? (String(v) as string) : <span style={{ color: "var(--text-muted)" }}>—</span>;
    }
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12.5, tableLayout: "auto" }}>
        <thead>
          <tr style={{ background: "var(--bg-raised)", borderBottom: "1px solid var(--border-default)" }}>
            {columns.map((c) => (
              <th
                key={c.key}
                onClick={() => c.sortable && toggleSort(c.key as SortKey)}
                style={{
                  width: c.width,
                  minWidth: c.width,
                  padding: "6px 12px",
                  textAlign: c.align,
                  fontSize: 10.5,
                  fontWeight: 600,
                  letterSpacing: 0.4,
                  textTransform: "uppercase",
                  color: "var(--text-tertiary)",
                  cursor: c.sortable ? "pointer" : "default",
                  whiteSpace: "nowrap",
                  userSelect: "none",
                }}
              >
                <span
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 3,
                    justifyContent: c.align === "right" ? "flex-end" : "flex-start",
                  }}
                >
                  {c.label}
                  {c.sortable && sortBy === c.key && (sortDir === "asc" ? <Icons.ArrowUp size={10} /> : <Icons.ArrowDown size={10} />)}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => {
            const isSelected = row.trace_id === selectedTraceId;
            return (
              <tr
                key={row.trace_id + "_" + i}
                onClick={() => onRowClick && onRowClick(row)}
                style={{
                  height: rowH,
                  background: isSelected ? "var(--bg-active)" : "transparent",
                  borderBottom: "1px solid var(--border-subtle)",
                  cursor: "pointer",
                  borderLeft: isSelected ? "2px solid var(--accent)" : "2px solid transparent",
                }}
                onMouseEnter={(e) => {
                  if (!isSelected) e.currentTarget.style.background = "var(--bg-hover)";
                }}
                onMouseLeave={(e) => {
                  if (!isSelected) e.currentTarget.style.background = "transparent";
                }}
              >
                {columns.map((c) => (
                  <td
                    key={c.key}
                    style={{
                      padding: "0 12px",
                      textAlign: c.align,
                      color: "var(--text-secondary)",
                      fontFamily: c.mono ? "var(--font-mono)" : "var(--font-sans)",
                      fontSize: c.mono ? 11.5 : 12.5,
                      whiteSpace: "nowrap",
                      verticalAlign: "middle",
                    }}
                  >
                    {renderCell(c, row)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
