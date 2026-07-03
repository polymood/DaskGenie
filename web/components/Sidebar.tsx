"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect } from "react";
import { useRuns } from "@/lib/api";
import { collectorWsBase } from "@/lib/live";
import { ago } from "@/lib/format";

export function Sidebar() {
  const { data: runs, mutate } = useRuns();
  const params = useParams();
  const activeId = params?.id as string | undefined;

  // Live run list: the collector pushes a nudge whenever a run is created,
  // deleted, or gains a death, so the sidebar updates without a page refresh.
  useEffect(() => {
    const base = collectorWsBase();
    if (!base) return;
    let ws: WebSocket | null = null;
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;
    const connect = () => {
      ws = new WebSocket(`${base}/ws/runs`);
      ws.onmessage = () => mutate();
      ws.onclose = () => {
        if (!closed) retry = setTimeout(connect, 1500);
      };
      ws.onerror = () => ws?.close();
    };
    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      ws?.close();
    };
  }, [mutate]);

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
            {r.origin || r.origin_ip ? (
              <div className="r-origin mono" title="Origin machine">
                {r.origin || r.origin_ip}
              </div>
            ) : null}
          </Link>
        ))
      )}
    </>
  );
}
