"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useRuns } from "@/lib/api";
import { ago } from "@/lib/format";

export function Sidebar() {
  const { data: runs } = useRuns();
  const params = useParams();
  const activeId = params?.id as string | undefined;

  return (
    <>
      <div className="side-head">
        <span>Runs</span>
        <span className="count">{runs?.length ?? 0}</span>
      </div>
      {!runs || runs.length === 0 ? (
        <div className="side-empty">No runs reported yet.</div>
      ) : (
        runs.map((r) => (
          <Link
            key={r.id}
            href={`/runs/${r.id}`}
            className={`side-run${r.id === activeId ? " active" : ""}`}
          >
            <div className="r-top">
              <span className="r-name">{r.name}</span>
              {r.counts.deaths ? (
                <span className="badge danger">
                  <span className="dot" />
                  {r.counts.deaths}
                </span>
              ) : null}
            </div>
            <div className="r-meta">
              {ago(r.created_at)} · {r.counts.workers ?? 0}w · {r.counts.samples ?? 0}s
            </div>
          </Link>
        ))
      )}
    </>
  );
}
