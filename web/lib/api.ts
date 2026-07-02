"use client";

import useSWR from "swr";
import type { DeathEvent, GraphData, RunInfo, Sample } from "./types";

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

export async function deleteRun(id: string): Promise<void> {
  const res = await fetch(`/api/runs/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}
