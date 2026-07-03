"use client";

import useSWR from "swr";
import type {
  AllocSiteRow,
  AllocTimelineRow,
  ChunkMeta,
  DeathEvent,
  FlameData,
  GraphData,
  LayerStat,
  RunInfo,
  Sample,
  TaskMemoryRow,
  TaskSpan,
  WorkerStatus,
} from "./types";

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
};

// Poll so runs and samples update live while a job is running.
const LIVE = { refreshInterval: 4000 };

export function useRuns() {
  return useSWR<RunInfo[]>("/api/runs", fetcher, LIVE);
}

export function useRun(id: string) {
  return useSWR<RunInfo>(`/api/runs/${id}`, fetcher, LIVE);
}

export function useDeaths(id: string) {
  return useSWR<DeathEvent[]>(`/api/runs/${id}/deaths`, fetcher, LIVE);
}

export function useTimeline(id: string) {
  return useSWR<Sample[]>(`/api/runs/${id}/timeline`, fetcher, LIVE);
}

export function useGraph(id: string) {
  return useSWR<GraphData>(`/api/runs/${id}/graph`, fetcher);
}

export function useSpans(id: string) {
  return useSWR<TaskSpan[]>(`/api/runs/${id}/spans`, fetcher, LIVE);
}

export function useLayerStats(id: string) {
  return useSWR<LayerStat[]>(`/api/runs/${id}/layer-stats`, fetcher, LIVE);
}

export function useWorkers(id: string) {
  return useSWR<WorkerStatus[]>(`/api/runs/${id}/workers`, fetcher, LIVE);
}

export function useAllocSites(id: string) {
  return useSWR<AllocSiteRow[]>(`/api/runs/${id}/alloc-sites`, fetcher, LIVE);
}

export function useTaskMemory(id: string) {
  return useSWR<TaskMemoryRow[]>(`/api/runs/${id}/task-memory`, fetcher, LIVE);
}

export function useAllocTimeline(id: string) {
  return useSWR<AllocTimelineRow[]>(`/api/runs/${id}/alloc-timeline`, fetcher, LIVE);
}

export function useFlamegraph(id: string, worker: string | null) {
  const q = worker ? `?worker=${encodeURIComponent(worker)}` : "";
  // keepPreviousData so switching worker doesn't blank the panel mid-fetch.
  return useSWR<FlameData>(`/api/runs/${id}/flamegraph${q}`, fetcher, {
    ...LIVE,
    keepPreviousData: true,
  });
}

export function useChunks(id: string, key: string | null) {
  return useSWR<ChunkMeta[]>(
    key ? `/api/runs/${id}/chunks/${encodeURIComponent(key)}` : null,
    fetcher,
  );
}

export async function deleteRun(id: string): Promise<void> {
  const res = await fetch(`/api/runs/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}
