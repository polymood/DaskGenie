"use client";

// Real-time run state. One WebSocket per open run streams the collector's
// ingest as it happens; we seed from REST once, then apply each frame as a
// delta into in-memory rings. Components read the live state through useLive()
// instead of polling — the memory chart, Workers table and task stream all
// update the instant a worker flushes.

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { DeathEvent, Sample, TaskSpan, WorkerStatus } from "./types";

const SAMPLE_CAP = 6000; // ~ tens of minutes at 0.2s across a few workers
const SPAN_CAP = 20000;

type Frame =
  | {
      type: "batch";
      worker: string;
      samples: Sample[];
      spans: TaskSpan[];
      statuses: WorkerStatus[];
      epochs: unknown[];
      task_memory: unknown[];
    }
  | { type: "death"; data: DeathEvent }
  | { type: "graph" };

export interface LiveState {
  connected: boolean;
  // Wall-clock (ms) of the last data frame received. A run is only "live" if
  // data is actually still arriving — a finished run has an open socket but no
  // recent frames, so the UI shows it idle rather than falsely "live".
  lastFrameAt: number;
  samples: Sample[];
  statuses: Record<string, WorkerStatus>;
  spans: TaskSpan[];
  deaths: DeathEvent[];
  // Bumps when the deep-memory stream or graph advances, so views backed by
  // aggregate REST endpoints (alloc-sites, task-memory, graph) can revalidate.
  deepNonce: number;
  graphNonce: number;
}

const empty: LiveState = {
  connected: false,
  lastFrameAt: 0,
  samples: [],
  statuses: {},
  spans: [],
  deaths: [],
  deepNonce: 0,
  graphNonce: 0,
};

const LiveContext = createContext<LiveState>(empty);

export function useLive(): LiveState {
  return useContext(LiveContext);
}

export function collectorWsBase(): string {
  const env = process.env.NEXT_PUBLIC_COLLECTOR_WS;
  if (env) return env.replace(/\/$/, "");
  // Default: the collector's published port on the same host the dashboard is
  // served from (docker-compose maps 8765:8765; dev runs it on localhost).
  if (typeof window !== "undefined") {
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${window.location.hostname}:8765`;
  }
  return "";
}

function collectorWsUrl(runId: string): string {
  const base = collectorWsBase();
  return base ? `${base}/ws/runs/${runId}` : "";
}

export function LiveProvider({
  runId,
  children,
}: {
  runId: string;
  children: React.ReactNode;
}) {
  const [state, setState] = useState<LiveState>(empty);
  const ref = useRef<LiveState>(empty);
  const set = (next: LiveState) => {
    ref.current = next;
    setState(next);
  };

  // Seed from REST once so the views aren't empty before the first frame.
  useEffect(() => {
    let cancelled = false;
    async function seed() {
      try {
        const [timeline, workers, spans, deaths] = await Promise.all([
          fetch(`/api/runs/${runId}/timeline`).then((r) => r.json()),
          fetch(`/api/runs/${runId}/workers`).then((r) => r.json()),
          fetch(`/api/runs/${runId}/spans`).then((r) => r.json()),
          fetch(`/api/runs/${runId}/deaths`).then((r) => r.json()),
        ]);
        if (cancelled) return;
        const statuses: Record<string, WorkerStatus> = {};
        for (const w of workers as WorkerStatus[]) statuses[w.worker] = w;
        set({
          ...ref.current,
          samples: (timeline as Sample[]).slice().sort((a, b) => a.timestamp - b.timestamp),
          statuses,
          spans: spans as TaskSpan[],
          deaths: deaths as DeathEvent[],
        });
      } catch {
        /* collector not up yet; the WS will fill in */
      }
    }
    seed();
    return () => {
      cancelled = true;
    };
  }, [runId]);

  // Live WebSocket with auto-reconnect.
  useEffect(() => {
    const url = collectorWsUrl(runId);
    if (!url) return;
    let ws: WebSocket | null = null;
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;

    const connect = () => {
      ws = new WebSocket(url);
      ws.onopen = () => set({ ...ref.current, connected: true });
      ws.onclose = () => {
        set({ ...ref.current, connected: false });
        if (!closed) retry = setTimeout(connect, 1500);
      };
      ws.onerror = () => ws?.close();
      ws.onmessage = (ev) => {
        let frame: Frame;
        try {
          frame = JSON.parse(ev.data);
        } catch {
          return;
        }
        apply(frame);
      };
    };

    const apply = (frame: Frame) => {
      const cur = { ...ref.current, lastFrameAt: Date.now() };
      if (frame.type === "batch") {
        const statuses = { ...cur.statuses };
        for (const s of frame.statuses) statuses[s.worker] = s;
        // A batch's MemorySamples don't carry the worker (it's on the envelope);
        // tag them so the memory chart can key lines by worker.
        const tagged = frame.samples.map((s) => ({ ...s, worker: frame.worker }));
        const samples = cur.samples.concat(tagged);
        const spans = cur.spans.concat(frame.spans);
        const deep = frame.epochs.length || frame.task_memory.length;
        set({
          ...cur,
          statuses,
          samples: samples.length > SAMPLE_CAP ? samples.slice(-SAMPLE_CAP) : samples,
          spans: spans.length > SPAN_CAP ? spans.slice(-SPAN_CAP) : spans,
          deepNonce: deep ? cur.deepNonce + 1 : cur.deepNonce,
        });
      } else if (frame.type === "death") {
        set({ ...cur, deaths: [frame.data, ...cur.deaths] });
      } else if (frame.type === "graph") {
        set({ ...cur, graphNonce: cur.graphNonce + 1 });
      }
    };

    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      ws?.close();
    };
  }, [runId]);

  const value = useMemo(() => state, [state]);
  return <LiveContext.Provider value={value}>{children}</LiveContext.Provider>;
}
