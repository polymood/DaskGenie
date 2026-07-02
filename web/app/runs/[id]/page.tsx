"use client";

import Link from "next/link";
import { useDeaths, useGraph, useLayerStats, useRun, useTimeline } from "@/lib/api";
import { baseName, layerColorMap } from "@/lib/colors";
import { bytes } from "@/lib/format";
import { MemoryChart } from "@/components/MemoryChart";
import { useMemo } from "react";

export default function OverviewPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data: run } = useRun(id);
  const { data: samples } = useTimeline(id);
  const { data: deaths } = useDeaths(id);
  const { data: graph } = useGraph(id);
  const { data: layerStats } = useLayerStats(id);
  const colorOf = useMemo(() => layerColorMap(), []);

  const peak = Math.max(0, ...(samples ?? []).map((s) => s.rss_bytes));
  const oom = (deaths ?? []).filter((d) => d.suspected_oom && d.suspect_keys.length > 0);
  const times = (samples ?? []).map((s) => s.timestamp);
  const duration = times.length > 1 ? Math.max(...times) - Math.min(...times) : 0;
  const maxTotal = Math.max(1, ...(layerStats ?? []).map((l) => l.total_seconds));

  return (
    <>
      <div className="stats">
        <div className="stat">
          <div className="v">{run?.counts.workers ?? 0}</div>
          <div className="k">Workers</div>
        </div>
        <div className="stat">
          <div className="v">{graph?.task_count ?? 0}</div>
          <div className="k">Tasks</div>
        </div>
        <div className={`stat${oom.length ? " alert" : ""}`}>
          <div className="v">{run?.counts.deaths ?? 0}</div>
          <div className="k">Worker deaths</div>
        </div>
        <div className="stat">
          <div className="v">{peak ? bytes(peak) : "—"}</div>
          <div className="k">Peak worker RSS</div>
        </div>
      </div>

      {oom.length > 0 && (
        <div className="empty" style={{ marginBottom: 20 }}>
          <strong>{oom.length}</strong> suspected-OOM death{oom.length !== 1 ? "s" : ""} recorded.{" "}
          <Link href={`/runs/${id}/postmortem`} style={{ color: "var(--accent)" }}>
            Open the post-mortem →
          </Link>
        </div>
      )}

      <div className="grid-2">
        <div>
          <div className="section-label">Memory over time</div>
          {samples && samples.length > 0 ? (
            <MemoryChart samples={samples} />
          ) : (
            <div className="empty">No memory samples yet.</div>
          )}
        </div>
        <div>
          <div className="section-label">
            Tasks by layer{duration ? ` · ${duration.toFixed(1)}s total` : ""}
          </div>
          {layerStats && layerStats.length > 0 ? (
            <div className="panel pad">
              {layerStats.slice(0, 12).map((l) => (
                <div className="lstat" key={l.layer}>
                  <span className="lname">
                    <span className="sw" style={{ background: colorOf(l.layer) }} />
                    {baseName(l.layer)}
                  </span>
                  <div className="bar">
                    <span
                      style={{
                        width: `${(l.total_seconds / maxTotal) * 100}%`,
                        background: colorOf(l.layer),
                      }}
                    />
                  </div>
                  <span className="num">
                    {l.count} · {l.total_seconds < 1 ? `${(l.total_seconds * 1000).toFixed(0)}ms` : `${l.total_seconds.toFixed(1)}s`}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty">No task timing yet.</div>
          )}
        </div>
      </div>
    </>
  );
}
