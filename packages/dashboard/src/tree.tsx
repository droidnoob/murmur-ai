import { Icons } from "./icons";
import { StatusDot, TrustTag, RejectTag, STATUS_META, fmt } from "./primitives";
import type { Run } from "./types";

function NodeCard({
  node,
  selected,
  onClick,
}: {
  node: Run;
  selected: boolean;
  onClick: (n: Run) => void;
}) {
  const status = STATUS_META[node.status];
  const isReject = node.status === "rejected";
  const isRunning = node.status === "running";

  const borderColor = selected ? "var(--accent)" : "var(--border-default)";

  return (
    <button
      onClick={() => onClick(node)}
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 4,
        minWidth: 188,
        maxWidth: 220,
        padding: "8px 10px",
        background: selected ? "var(--bg-active)" : "var(--bg-surface)",
        border: `1px solid ${borderColor}`,
        borderLeft: `3px solid ${status.color}`,
        borderRadius: "var(--r-md)",
        textAlign: "left",
        cursor: "pointer",
        boxShadow: selected ? "0 0 0 3px var(--accent-bg)" : "none",
        animation: "node-enter 320ms ease",
        position: "relative",
        transition: "background 120ms, border-color 120ms",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 6, width: "100%" }}>
        <StatusDot status={node.status} pulse={isRunning} />
        <span
          style={{
            flex: 1,
            fontSize: 12.5,
            fontWeight: 600,
            color: "var(--text-primary)",
            fontFamily: "var(--font-mono)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            letterSpacing: -0.1,
          }}
        >
          {node.agent_name}
        </span>
        <span
          style={{
            fontSize: 9.5,
            fontWeight: 600,
            color: "var(--text-muted)",
            fontFamily: "var(--font-mono)",
            padding: "0 4px",
            border: "1px solid var(--border-default)",
            borderRadius: "var(--r-sm)",
            letterSpacing: 0.3,
          }}
        >
          d{node.depth}
        </span>
      </div>

      {isReject && node.rejection_reason && (
        <div style={{ display: "flex", alignItems: "center", gap: 5, marginTop: 1 }}>
          <RejectTag reason={node.rejection_reason} />
          <span
            style={{
              fontSize: 10.5,
              color: "var(--text-tertiary)",
              fontStyle: "italic",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {node.rejection_reason === "cycle"
              ? "ancestor loop"
              : node.rejection_reason === "budget"
              ? "over token budget"
              : node.rejection_reason === "depth"
              ? "past depth limit"
              : node.rejection_reason === "cap"
              ? "spawn cap hit"
              : node.rejection_reason}
          </span>
        </div>
      )}

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          width: "100%",
          justifyContent: "space-between",
        }}
      >
        <TrustTag trust={node.trust_level} size="sm" />
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            fontFamily: "var(--font-mono)",
            fontSize: 10.5,
            color: "var(--text-tertiary)",
          }}
        >
          <span title="tokens">
            <span style={{ color: "var(--text-muted)" }}>⊙ </span>
            {fmt.tokens(node.tokens_used)}
          </span>
          <span title="duration">
            <span style={{ color: "var(--text-muted)" }}>◴ </span>
            {fmt.ms(node.duration_ms)}
          </span>
        </div>
      </div>
    </button>
  );
}

interface Group {
  label: string;
  nodes: Run[];
}

function groupChildren(children: Run[]): Group[] {
  const groups: Group[] = [];
  let current: Group | null = null;
  children.forEach((c) => {
    const lbl = c.cascade_label || "single";
    if (lbl.startsWith("gather:") && current && current.label === lbl) {
      current.nodes.push(c);
    } else {
      current = { label: lbl, nodes: [c] };
      groups.push(current);
    }
  });
  return groups;
}

function countDescendants(node: Run): number {
  return (node.children || []).reduce((t, c) => t + 1 + countDescendants(c), 0);
}

export function CascadeTree({
  root,
  selectedId,
  onSelect,
  collapsedSet,
  onToggleCollapse,
}: {
  root: Run;
  selectedId: string | null;
  onSelect: (n: Run) => void;
  collapsedSet: Set<string>;
  onToggleCollapse: (id: string) => void;
}) {
  function NodeAndChildren({ node }: { node: Run }) {
    const isCollapsed = collapsedSet.has(node.trace_id);
    const hasChildren = !!(node.children && node.children.length > 0);
    const groups = hasChildren ? groupChildren(node.children!) : [];

    return (
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center", position: "relative" }}>
        <div style={{ position: "relative", display: "flex", alignItems: "center" }}>
          <NodeCard node={node} selected={selectedId === node.trace_id} onClick={onSelect} />
          {hasChildren && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onToggleCollapse(node.trace_id);
              }}
              style={{
                position: "absolute",
                right: -10,
                top: "50%",
                transform: "translateY(-50%)",
                width: 18,
                height: 18,
                background: "var(--bg-raised)",
                border: "1px solid var(--border-default)",
                borderRadius: "50%",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: "pointer",
                color: "var(--text-tertiary)",
                zIndex: 2,
              }}
              title={isCollapsed ? `Expand ${node.children!.length}` : "Collapse"}
            >
              {isCollapsed ? <Icons.Plus size={11} /> : <Icons.Minus size={11} />}
            </button>
          )}
        </div>

        {hasChildren && !isCollapsed && (
          <div style={{ width: 1, height: 16, background: "var(--border-strong)" }} />
        )}

        {hasChildren && isCollapsed && (
          <div
            style={{
              marginTop: 6,
              padding: "2px 8px",
              background: "var(--bg-input)",
              border: "1px dashed var(--border-default)",
              borderRadius: "var(--r-sm)",
              fontSize: 10.5,
              color: "var(--text-tertiary)",
              fontFamily: "var(--font-mono)",
            }}
          >
            +{countDescendants(node)} descendants
          </div>
        )}

        {hasChildren && !isCollapsed && (
          <div style={{ display: "flex", alignItems: "flex-start", gap: 24, position: "relative" }}>
            {groups.map((g, gi) => (
              <div
                key={gi}
                style={{ display: "flex", flexDirection: "column", alignItems: "center", position: "relative" }}
              >
                {g.label.startsWith("gather:") && g.nodes.length > 1 && (
                  <div
                    style={{
                      position: "relative",
                      padding: "2px 8px",
                      marginBottom: 6,
                      background: "var(--bg-input)",
                      border: "1px solid var(--border-default)",
                      borderRadius: "var(--r-sm)",
                      fontSize: 10,
                      color: "var(--text-secondary)",
                      fontFamily: "var(--font-mono)",
                      textTransform: "uppercase",
                      letterSpacing: 0.4,
                      fontWeight: 600,
                    }}
                  >
                    gather × {g.nodes.length}
                  </div>
                )}

                {g.nodes.length > 1 && (
                  <div style={{ position: "relative", width: "100%", height: 16, marginBottom: -1 }}>
                    <div
                      style={{
                        position: "absolute",
                        top: 0,
                        left: "8%",
                        right: "8%",
                        height: 1,
                        background: "var(--border-strong)",
                      }}
                    />
                    {g.nodes.map((_, i) => {
                      const pct = g.nodes.length === 1 ? 50 : (i / (g.nodes.length - 1)) * 84 + 8;
                      return (
                        <div
                          key={i}
                          style={{
                            position: "absolute",
                            top: 0,
                            left: `${pct}%`,
                            width: 1,
                            height: 16,
                            background: "var(--border-strong)",
                          }}
                        />
                      );
                    })}
                  </div>
                )}
                {g.nodes.length === 1 && g.label !== "single" && (
                  <div style={{ width: 1, height: 12, background: "var(--border-strong)" }} />
                )}

                <div style={{ display: "flex", alignItems: "flex-start", gap: 16 }}>
                  {g.nodes.map((c) => (
                    <NodeAndChildren key={c.trace_id} node={c} />
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        padding: "24px 24px 32px",
        minWidth: "100%",
        width: "fit-content",
      }}
    >
      <NodeAndChildren node={root} />
    </div>
  );
}
