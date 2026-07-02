// Typed API client + TanStack Query hooks. All data fetching lives here so
// components never call fetch directly (spec requirement).

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { DeathEvent, GraphData, RunInfo, Sample } from "./types";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

async function del(path: string): Promise<void> {
  const res = await fetch(path, { method: "DELETE" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

export function useRuns() {
  // Poll so runs appear live as jobs report in.
  return useQuery({
    queryKey: ["runs"],
    queryFn: () => get<RunInfo[]>("/api/runs"),
    refetchInterval: 5000,
  });
}

export function useDeaths(runId: string) {
  return useQuery({
    queryKey: ["deaths", runId],
    queryFn: () => get<DeathEvent[]>(`/api/runs/${runId}/deaths`),
    refetchInterval: 5000,
  });
}

export function useTimeline(runId: string) {
  return useQuery({
    queryKey: ["timeline", runId],
    queryFn: () => get<Sample[]>(`/api/runs/${runId}/timeline`),
    refetchInterval: 5000,
  });
}

export function useGraph(runId: string) {
  return useQuery({
    queryKey: ["graph", runId],
    queryFn: () => get<GraphData>(`/api/runs/${runId}/graph`),
  });
}

export function useDeleteRun() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (runId: string) => del(`/api/runs/${runId}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["runs"] }),
  });
}
