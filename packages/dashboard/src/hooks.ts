// Live-data hook for the dashboard.
//
// Fetches the initial run list + their event trees from the AgentServer,
// subscribes to /events/stream for incremental updates, and exposes a single
// `LiveData` object the UI can render. When the API isn't reachable
// (standalone bundle, server without the EventStore wired) the hook flips
// `mode` to `"demo"` and the caller falls through to mocked data.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, subscribeEvents } from "./api";
import type {
  RunsPage,
  StatsResponse,
  ToolsReport,
  UsageReport,
  WireEvent,
} from "./api";
import { eventsToRuns, toEventEntry } from "./transform";
import type { EventEntry, Run } from "./types";

export type LiveMode = "loading" | "live" | "demo";

export interface LiveData {
  mode: LiveMode;
  runs: Run[];
  events: EventEntry[];
  // Raw wire events keyed by trace_id — feeds the run-detail drawer.
  rawEvents: WireEvent[];
  stats: StatsResponse | null;
  costByModel: UsageReport | null;
  toolsReport: ToolsReport | null;
}

const EVENT_RING_CAP = 500;

const STATS_POLL_MS = 5000;

export function useLiveData(): LiveData {
  const [mode, setMode] = useState<LiveMode>("loading");
  const [rawEvents, setRawEvents] = useState<WireEvent[]>([]);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [costByModel, setCostByModel] = useState<UsageReport | null>(null);
  const [toolsReport, setToolsReport] = useState<ToolsReport | null>(null);
  const seenIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    let cancelled = false;

    (async () => {
      // Pull the recent /events backlog. If the endpoint isn't there, the
      // server isn't running an event store — drop into demo mode.
      const seed = await api.events(EVENT_RING_CAP);
      if (cancelled) return;
      if (seed === null) {
        setMode("demo");
        return;
      }
      const ordered = [...seed].reverse();
      seenIds.current = new Set(
        ordered.map((e) => `${e.trace_id}:${e.event_type}:${e.timestamp}`),
      );
      setRawEvents(ordered);
      setMode("live");
    })();

    const unsubscribe = subscribeEvents((ev) => {
      const id = `${ev.trace_id}:${ev.event_type}:${ev.timestamp}`;
      if (seenIds.current.has(id)) return;
      seenIds.current.add(id);
      setRawEvents((prev) => {
        const next = [...prev, ev];
        if (next.length > EVENT_RING_CAP) next.splice(0, next.length - EVENT_RING_CAP);
        return next;
      });
      setMode("live");
    });

    // Stats / cost / tools poll — refresh header meters + Health tab on a tick.
    const fetchAll = async (): Promise<void> => {
      const [nextStats, nextCost, nextTools] = await Promise.all([
        api.stats(),
        api.usage("model"),
        api.tools("tool"),
      ]);
      if (cancelled) return;
      if (nextStats !== null) setStats(nextStats);
      if (nextCost !== null) setCostByModel(nextCost);
      if (nextTools !== null) setToolsReport(nextTools);
    };
    void fetchAll();
    const interval = window.setInterval(() => void fetchAll(), STATS_POLL_MS);

    return () => {
      cancelled = true;
      unsubscribe?.();
      window.clearInterval(interval);
    };
  }, []);

  const runs = useMemo(() => eventsToRuns(rawEvents), [rawEvents]);
  const events = useMemo(
    () => [...rawEvents].reverse().map(toEventEntry),
    [rawEvents],
  );

  return { mode, runs, events, rawEvents, stats, costByModel, toolsReport };
}


// ---------------------------------------------------------------------------
// Backend-paginated /runs hook for the History tab.
// ---------------------------------------------------------------------------

export type RunsStatusFilter = "all" | "spawned" | "running" | "completed" | "failed" | "rejected";

export interface RunsPageState {
  page: RunsPage | null;
  loading: boolean;
  error: string | null;
  pageSize: number;
  pageIndex: number;
  status: RunsStatusFilter;
  setPageSize: (n: number) => void;
  setPageIndex: (i: number) => void;
  setStatus: (s: RunsStatusFilter) => void;
  refresh: () => void;
}

// ---------------------------------------------------------------------------
// Per-trace detail hook for the run drawer.
// ---------------------------------------------------------------------------

export interface RunDetailState {
  run: Run | null;
  events: WireEvent[];
  loading: boolean;
}

export function useRunDetail(
  traceId: string | null,
  fallbackEvents: WireEvent[],
): RunDetailState {
  const [events, setEvents] = useState<WireEvent[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    if (traceId === null) {
      setEvents([]);
      setLoading(false);
      return;
    }
    // First render: seed from whatever is in the in-memory ring so we have
    // *something* immediately while the tree fetch runs.
    const inRing = fallbackEvents.filter(
      (e) => e.trace_id === traceId || e.parent_trace_id === traceId,
    );
    setEvents(inRing);

    let cancelled = false;
    setLoading(true);
    (async () => {
      const tree = await api.runTree(traceId);
      if (cancelled) return;
      setLoading(false);
      if (tree !== null) setEvents(tree);
    })();
    return () => {
      cancelled = true;
    };
  // We only re-fetch when the selected trace changes; ring updates flow in
  // through `fallbackEvents` but shouldn't refire the API call.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [traceId]);

  const run = useMemo<Run | null>(() => {
    if (traceId === null) return null;
    const tree = eventsToRuns(events);
    function find(node: Run): Run | null {
      if (node.trace_id === traceId) return node;
      for (const c of node.children ?? []) {
        const r = find(c);
        if (r) return r;
      }
      return null;
    }
    for (const root of tree) {
      const r = find(root);
      if (r) return r;
    }
    return null;
  }, [events, traceId]);

  return { run, events, loading };
}


export function useRunsPage(initialPageSize = 50): RunsPageState {
  const [pageSize, setPageSize] = useState<number>(initialPageSize);
  const [pageIndex, setPageIndex] = useState<number>(0);
  const [status, setStatus] = useState<RunsStatusFilter>("all");
  const [page, setPage] = useState<RunsPage | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  const refresh = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    (async () => {
      const params: { limit: number; offset: number; status?: string } = {
        limit: pageSize,
        offset: pageIndex * pageSize,
      };
      if (status !== "all") params.status = status;
      const next = await api.runsPage(params);
      if (cancelled) return;
      if (next === null) {
        setError("server unreachable");
        setPage(null);
      } else {
        setError(null);
        setPage(next);
      }
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [pageSize, pageIndex, status, tick]);

  // Auto-refresh every 5s so the History tab stays current as new runs come in.
  useEffect(() => {
    const id = window.setInterval(refresh, 5000);
    return () => window.clearInterval(id);
  }, [refresh]);

  // Reset pageIndex when filter or page size changes — otherwise we may
  // land on an empty page.
  useEffect(() => {
    setPageIndex(0);
  }, [status, pageSize]);

  return {
    page,
    loading,
    error,
    pageSize,
    pageIndex,
    status,
    setPageSize,
    setPageIndex,
    setStatus,
    refresh,
  };
}
