"use client";

import Link from "next/link";
import { useDeaths, useRun, useTimeline } from "@/lib/api";
import { bytes } from "@/lib/format";
import { MemoryChart } from "@/components/MemoryChart";

export default function OverviewPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data: run } = useRun(id);
  const { data: samples } = useTimeline(id);
  const { data: deaths } = useDeaths(id);

  const peak = Math.max(0, ...(samples ?? []).map((s) => s.rss_bytes));
  const oom = (deaths ?? []).filter((d) => d.suspected_oom && d.suspect_keys.length > 0);

  return (
    <>
      <div className="stats">
        <div className="stat">
          <div className="v">{run?.counts.workers ?? 0}</div>
          <div className="k">Workers</div>
        </div>
        <div className="stat">
          <div className="v">{run?.counts.samples ?? 0}</div>
          <div className="k">Samples</div>
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

      <div className="section-label">Memory over time</div>
      {samples && samples.length > 0 ? (
        <MemoryChart samples={samples} />
      ) : (
        <div className="empty">No memory samples yet.</div>
      )}
    </>
  );
}
