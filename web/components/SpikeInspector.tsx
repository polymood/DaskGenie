"use client";

import { useMemo } from "react";
import useSWR from "swr";
import { useGraph } from "@/lib/api";
import { baseName } from "@/lib/colors";
import { bytes, shortKey } from "@/lib/format";
import type { AllocSiteRow, GraphLayer, Sample, TaskSpan } from "@/lib/types";
import { CodeLine } from "./CodeLine";

const WINDOW = 3; // seconds around the click to attribute allocations to

const fetcher = (u: string) => fetch(u).then((r) => r.json());

function sourceForLayer(layer: string, layers: GraphLayer[]): GraphLayer | undefined {
  return layers.find((l) => layer === l.layer || layer.startsWith(l.layer) || l.layer.startsWith(layer));
}

// Given a pinned instant, answer "what was happening here": which tasks were
// running (where we were in the graph), what they cost in memory, and which
// source lines were allocating in that window — the "what caused this spike".
export function SpikeInspector({
  runId,
  time,
  samples,
  spans,
  onClose,
}: {
  runId: string;
  time: number | null;
  samples: Sample[];
  spans: TaskSpan[];
  onClose: () => void;
}) {
  const { data: graph } = useGraph(runId);
  const { data: sites } = useSWR<AllocSiteRow[]>(
    time != null
      ? `/api/runs/${runId}/alloc-sites?start=${time - WINDOW}&end=${time + WINDOW}`
      : null,
    fetcher,
  );

  const t0 = useMemo(
    () => (samples.length ? Math.min(...samples.map((s) => s.timestamp)) : 0),
    [samples],
  );

  const info = useMemo(() => {
    if (time == null) return null;
    // RSS per worker at the instant (nearest sample within 2s)
    const perWorker = new Map<string, number>();
    const nearest = new Map<string, { dt: number; rss: number }>();
    for (const s of samples) {
      if (!s.worker) continue;
      const dt = Math.abs(s.timestamp - time);
      const cur = nearest.get(s.worker);
      if (!cur || dt < cur.dt) nearest.set(s.worker, { dt, rss: s.rss_bytes });
    }
    for (const [w, v] of nearest) if (v.dt < 2) perWorker.set(w, v.rss);
    const totalRss = [...perWorker.values()].reduce((a, b) => a + b, 0);

    // tasks whose span covers this instant → "where we were in the graph"
    const running = spans
      .filter((s) => s.start <= time && s.end >= time)
      .sort((a, b) => b.end - b.start - (a.end - a.start));

    return { perWorker, totalRss, running };
  }, [time, samples, spans]);

  if (time == null)
    return (
      <div className="panel pad spike-empty faint">
        Click any point on the memory chart to see what was running and allocating at that instant.
      </div>
    );

  const layers = graph?.layers ?? [];
  const rel = (time - t0).toFixed(1);

  return (
    <div className="panel spike">
      <div className="spike-head">
        <span>
          Memory at <b>+{rel}s</b> · {info ? bytes(info.totalRss) : "—"} total RSS
        </span>
        <button className="btn" onClick={onClose}>
          ✕
        </button>
      </div>

      <div className="spike-body">
        <div className="spike-sec-label">Running here ({info?.running.length ?? 0})</div>
        {info && info.running.length > 0 ? (
          <div className="spike-tasks">
            {info.running.slice(0, 8).map((s, i) => {
              const src = sourceForLayer(s.layer, layers);
              return (
                <div className="spike-task" key={i}>
                  <div className="st-top">
                    <span className="mono st-key">{shortKey(s.key)}</span>
                    <span className="faint small mono">{s.worker.replace(/^tcp:\/\//, "")}</span>
                  </div>
                  <div className="faint small">{baseName(s.layer)}</div>
                  {src ? (
                    <>
                      <div className="srcpath">
                        {src.filename}:{src.lineno}
                      </div>
                      <CodeLine code={src.code_snippet || "(no snippet)"} />
                    </>
                  ) : null}
                </div>
              );
            })}
          </div>
        ) : (
          <div className="faint small">No task span covers this instant.</div>
        )}

        <div className="spike-sec-label" style={{ marginTop: 14 }}>
          Allocating in this window (±{WINDOW}s)
        </div>
        {sites && sites.length > 0 ? (
          <table className="data compact">
            <tbody>
              {sites.slice(0, 8).map((s, i) => (
                <tr key={i}>
                  <td className="mono small">
                    {s.filename.split("/").slice(-1)[0]}:{s.lineno}
                    <span className="faint"> {s.function}</span>
                  </td>
                  <td className="mono num" style={{ width: 90 }}>
                    <b style={{ color: "var(--warn)" }}>{bytes(s.hwm_bytes)}</b>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="faint small">
            No deep allocation data here — enable <code>deep=True</code> for line-level cause.
          </div>
        )}
      </div>
    </div>
  );
}
