"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useSpans, useTimeline } from "@/lib/api";
import { baseName, layerColorMap } from "@/lib/colors";
import { bytes } from "@/lib/format";
import type { Sample, TaskSpan } from "@/lib/types";

// Layout constants (px). Time runs top -> bottom; memory grows left -> right in
// its band; the task stream is worker lanes to the right — all on one time axis.
const PAD_T = 26;
const PAD_B = 18;
const TIME_LABEL_W = 46;
const MEM_X0 = TIME_LABEL_W + 6;
const MEM_W = 190;
const STREAM_X0 = MEM_X0 + MEM_W + 40;

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [w, setW] = useState(960);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setW(e.contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, w] as const;
}

function fmtTime(s: number): string {
  return s < 1 ? `${(s * 1000).toFixed(0)}ms` : `${s.toFixed(s < 10 ? 1 : 0)}s`;
}

export function AlignedTimeline({ runId }: { runId: string }) {
  const { data: samples } = useTimeline(runId);
  const { data: spans } = useSpans(runId);
  const [wrapRef, width] = useWidth<HTMLDivElement>();
  const colorOf = useMemo(() => layerColorMap(), []);

  const model = useMemo(() => {
    const S: Sample[] = samples ?? [];
    const T: TaskSpan[] = spans ?? [];
    const times = [
      ...S.map((s) => s.timestamp),
      ...T.map((t) => t.start),
      ...T.map((t) => t.end),
    ];
    if (times.length === 0) return null;
    const t0 = Math.min(...times);
    const t1 = Math.max(...times);
    const dur = Math.max(0.001, t1 - t0);
    const workers = [...new Set([...S.map((s) => s.worker), ...T.map((t) => t.worker)])].sort();
    const peak = Math.max(1, ...S.map((s) => s.rss_bytes));
    const layers = [...new Set(T.map((t) => baseName(t.layer)))];
    return { S, T, t0, dur, workers, peak, layers };
  }, [samples, spans]);

  if (!model)
    return <div className="empty">No timeline yet — samples and task spans will appear here.</div>;

  const { S, T, t0, dur, workers, peak, layers } = model;
  const plotH = Math.min(2400, Math.max(460, dur * 44));
  const H = plotH + PAD_T + PAD_B;
  const streamW = Math.max(120, width - STREAM_X0 - 8);
  const laneW = streamW / Math.max(1, workers.length);
  const y = (t: number) => PAD_T + ((t - t0) / dur) * plotH;
  const memX = (rss: number) => MEM_X0 + (rss / peak) * MEM_W;
  const laneIdx = new Map(workers.map((w, i) => [w, i]));

  // time gridlines
  const ticks = 6;
  const gridlines = Array.from({ length: ticks + 1 }, (_, i) => (i / ticks) * dur);

  return (
    <div ref={wrapRef}>
      <div className="legend">
        {layers.slice(0, 12).map((l) => (
          <span key={l}>
            <span className="sw" style={{ background: colorOf(l) }} />
            {l}
          </span>
        ))}
      </div>
      <div className="panel" style={{ overflow: "auto" }}>
        <svg width={width} height={H} style={{ display: "block" }}>
          {/* time gridlines + labels */}
          {gridlines.map((t, i) => (
            <g key={i}>
              <line x1={MEM_X0} y1={y(t0 + t)} x2={width - 8} y2={y(t0 + t)} stroke="#eef0f3" />
              <text x={TIME_LABEL_W} y={y(t0 + t) + 3} textAnchor="end" className="tl-axis">
                {fmtTime(t)}
              </text>
            </g>
          ))}

          {/* column headers */}
          <text x={MEM_X0} y={14} className="tl-head">
            memory (RSS →)
          </text>
          <text x={STREAM_X0} y={14} className="tl-head">
            tasks by worker
          </text>

          {/* memory: one polyline per worker, time down / rss right */}
          {workers.map((w, wi) => {
            const pts = S.filter((s) => s.worker === w)
              .sort((a, b) => a.timestamp - b.timestamp)
              .map((s) => `${memX(s.rss_bytes).toFixed(1)},${y(s.timestamp).toFixed(1)}`)
              .join(" ");
            if (!pts) return null;
            const c = ["#3b5bdb", "#0b7285", "#c2410c", "#5c940d"][wi % 4];
            return (
              <polyline key={w} points={pts} fill="none" stroke={c} strokeWidth={1.5} opacity={0.9} />
            );
          })}
          <text x={MEM_X0} y={H - 4} className="tl-axis">
            0
          </text>
          <text x={MEM_X0 + MEM_W} y={H - 4} textAnchor="end" className="tl-axis">
            {bytes(peak)}
          </text>

          {/* task stream lanes */}
          {workers.map((w) => {
            const i = laneIdx.get(w)!;
            const x = STREAM_X0 + i * laneW;
            return (
              <line key={`lane-${w}`} x1={x} y1={PAD_T} x2={x} y2={PAD_T + plotH} stroke="#f1f2f4" />
            );
          })}
          {T.map((s, i) => {
            const lane = laneIdx.get(s.worker) ?? 0;
            const x = STREAM_X0 + lane * laneW + 1;
            const yy = y(s.start);
            const h = Math.max(1.2, y(s.end) - y(s.start));
            return (
              <rect
                key={i}
                x={x}
                y={yy}
                width={Math.max(2, laneW - 2)}
                height={h}
                fill={colorOf(s.layer)}
                opacity={0.85}
              >
                <title>
                  {baseName(s.layer)} · {((s.end - s.start) * 1000).toFixed(0)}ms
                </title>
              </rect>
            );
          })}
        </svg>
      </div>
      <div className="graph-note" style={{ marginTop: 8 }}>
        {T.length} task spans across {workers.length} worker{workers.length !== 1 ? "s" : ""} · time
        runs top → bottom
      </div>
    </div>
  );
}
