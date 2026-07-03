"use client";

import { useEffect } from "react";
import { useAllocSites, useTaskMemory } from "@/lib/api";
import { useLive } from "@/lib/live";
import { bytes, shortKey } from "@/lib/format";
import { baseName } from "@/lib/colors";
import { Flamegraph } from "./Flamegraph";

// The 10x view: memray-derived, per-source-line memory. Answers "which line
// allocated the array that's filling memory", not just "how much is resident".
export function MemoryDeep({ runId }: { runId: string }) {
  const { data: sites, mutate: mSites } = useAllocSites(runId);
  const { data: tasks, mutate: mTasks } = useTaskMemory(runId);
  const { deepNonce } = useLive();

  // Pull fresh aggregates the moment a new epoch's deep data streams in.
  useEffect(() => {
    if (deepNonce > 0) {
      mSites();
      mTasks();
    }
  }, [deepNonce, mSites, mTasks]);

  const rows = sites ?? [];
  const taskRows = tasks ?? [];

  if (rows.length === 0 && taskRows.length === 0)
    return (
      <div className="empty">
        No deep memory data. Enable the memray engine for this run:{" "}
        <code>register(client, url, deep=True)</code> or{" "}
        <code>LocalProfiler(..., deep=True)</code>.
      </div>
    );

  const topLine = rows[0]?.hwm_bytes ?? 1;
  const topTask = taskRows[0]?.peak_rss_delta ?? 1;

  return (
    <>
      <Flamegraph runId={runId} />

      <div className="grid-2" style={{ marginTop: 20 }}>
        <div>
          <div className="section-label">Peak memory by source line</div>
          <table className="data">
            <thead>
              <tr>
                <th>Source line</th>
                <th style={{ width: 150 }}>Peak</th>
                <th style={{ width: 70 }}>Allocs</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 40).map((s) => (
                <tr key={`${s.filename}:${s.lineno}:${s.function}`}>
                  <td>
                    <div className="mono srcline">
                      {s.filename.split("/").slice(-1)[0]}:{s.lineno}
                    </div>
                    <div className="faint small mono">{s.function}</div>
                  </td>
                  <td>
                    <div className="membar">
                      <span style={{ width: `${(s.hwm_bytes / topLine) * 100}%` }} />
                      <em>{bytes(s.hwm_bytes)}</em>
                    </div>
                  </td>
                  <td className="mono num">{s.n_allocations.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div>
          <div className="section-label">Peak memory by task</div>
          <table className="data">
            <thead>
              <tr>
                <th>Task</th>
                <th style={{ width: 150 }}>Peak Δ RSS</th>
                <th>Top line</th>
              </tr>
            </thead>
            <tbody>
              {taskRows.slice(0, 40).map((t) => {
                const top = t.top_sites[0];
                return (
                  <tr key={t.key}>
                    <td>
                      <div className="mono">{shortKey(t.key)}</div>
                      <div className="faint small">{baseName(t.layer)}</div>
                    </td>
                    <td>
                      <div className="membar">
                        <span style={{ width: `${(t.peak_rss_delta / topTask) * 100}%` }} />
                        <em>{bytes(t.peak_rss_delta)}</em>
                      </div>
                    </td>
                    <td className="mono small">
                      {top ? `${top.filename.split("/").slice(-1)[0]}:${top.lineno}` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
