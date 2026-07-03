"use client";

import { useMemo } from "react";
import { useLive } from "@/lib/live";
import { bytes } from "@/lib/format";
import type { WorkerStatus } from "@/lib/types";

// A memory-pressure colour: green under half, amber past 70%, red past 90% of
// the worker's limit — the same read the native Dask workers page gives you.
function pressure(frac: number): string {
  if (frac >= 0.9) return "var(--danger)";
  if (frac >= 0.7) return "var(--warn)";
  return "var(--ok)";
}

function Bar({ used, limit }: { used: number; limit: number }) {
  const frac = limit > 0 ? Math.min(1, used / limit) : 0;
  return (
    <div className="wbar" title={limit > 0 ? `${bytes(used)} / ${bytes(limit)}` : bytes(used)}>
      <span style={{ width: `${frac * 100}%`, background: pressure(frac) }} />
      <em>{limit > 0 ? `${Math.round(frac * 100)}%` : bytes(used)}</em>
    </div>
  );
}

export function WorkersView() {
  const { statuses } = useLive();
  const rows = useMemo<WorkerStatus[]>(
    () => Object.values(statuses).sort((a, b) => a.worker.localeCompare(b.worker)),
    [statuses],
  );

  if (rows.length === 0) return <div className="empty">No worker heartbeats yet.</div>;

  const totalRss = rows.reduce((a, w) => a + w.rss_bytes, 0);
  const totalExec = rows.reduce((a, w) => a + w.executing, 0);

  return (
    <>
      <div className="stats" style={{ gridTemplateColumns: "repeat(3, 1fr)" }}>
        <div className="stat">
          <div className="v">{rows.length}</div>
          <div className="k">Workers</div>
        </div>
        <div className="stat">
          <div className="v">{bytes(totalRss)}</div>
          <div className="k">Total RSS</div>
        </div>
        <div className="stat">
          <div className="v">{totalExec}</div>
          <div className="k">Tasks executing</div>
        </div>
      </div>

      <table className="data">
        <thead>
          <tr>
            <th>Worker</th>
            <th style={{ width: 220 }}>Memory</th>
            <th>Managed</th>
            <th>CPU</th>
            <th>Threads</th>
            <th>Executing</th>
            <th>Ready</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((w) => (
            <tr key={w.worker}>
              <td className="mono">
                <span className={`dot ${w.executing > 0 ? "busy" : "idle"}`} /> {w.worker}
              </td>
              <td>
                <Bar used={w.rss_bytes} limit={w.memory_limit} />
              </td>
              <td className="mono">{w.managed_bytes ? bytes(w.managed_bytes) : "—"}</td>
              <td className="mono">{w.cpu.toFixed(0)}%</td>
              <td className="mono">{w.nthreads || "—"}</td>
              <td className="mono">{w.executing}</td>
              <td className="mono">{w.ready}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
