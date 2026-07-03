"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useLive } from "@/lib/live";
import { baseName, layerColorMap } from "@/lib/colors";
import { shortKey } from "@/lib/format";
import type { TaskSpan } from "@/lib/types";

const LANE_H = 18;
const LANE_GAP = 3;
const SECTION_GAP = 16;
const GUTTER = 96; // left label column (fixed, not zoomed)
const PAD_R = 10;
const PAD_T = 8;
const AXIS_H = 18;

type Hover = { x: number; y: number; span: TaskSpan } | null;

// Greedy interval packing: assign each span to the first lane whose last task
// has finished — the compact "global" activity view across all workers.
function packLanes(spans: TaskSpan[]): { span: TaskSpan; lane: number }[] {
  const sorted = [...spans].sort((a, b) => a.start - b.start);
  const laneEnds: number[] = [];
  const out: { span: TaskSpan; lane: number }[] = [];
  for (const s of sorted) {
    let lane = laneEnds.findIndex((end) => end <= s.start);
    if (lane === -1) {
      lane = laneEnds.length;
      laneEnds.push(s.end);
    } else {
      laneEnds[lane] = s.end;
    }
    out.push({ span: s, lane });
  }
  return out;
}

export function TaskStream() {
  const { spans } = useLive();
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(900);
  const [hover, setHover] = useState<Hover>(null);
  const colorOf = useMemo(() => layerColorMap(), []);

  const domain = useMemo(() => {
    if (spans.length === 0) return { lo: 0, hi: 1 };
    let lo = Infinity;
    let hi = -Infinity;
    for (const s of spans) {
      if (s.start < lo) lo = s.start;
      if (s.end > hi) hi = s.end;
    }
    return { lo, hi: hi <= lo ? lo + 1 : hi };
  }, [spans]);

  // the visible time window (zoom/pan state)
  const view = useRef({ lo: domain.lo, hi: domain.hi });
  const [, force] = useState(0);
  const redraw = () => force((n) => n + 1);
  const drag = useRef<{ x: number; lo: number; hi: number } | null>(null);
  const marquee = useRef<{ x0: number } | null>(null);
  const [box, setBox] = useState<{ x: number; w: number } | null>(null);

  // reset the window when the run/domain changes
  useEffect(() => {
    view.current = { lo: domain.lo, hi: domain.hi };
    redraw();
  }, [domain.lo, domain.hi]);

  const workers = useMemo(() => Array.from(new Set(spans.map((s) => s.worker))).sort(), [spans]);
  const global = useMemo(() => packLanes(spans), [spans]);
  const globalLanes = useMemo(() => Math.max(1, ...global.map((g) => g.lane + 1)), [global]);
  const laneOf = useMemo(() => {
    const m = new Map<string, number>();
    workers.forEach((w, i) => m.set(w, i));
    return m;
  }, [workers]);

  const globalH = globalLanes * (LANE_H + LANE_GAP);
  const workerH = workers.length * (LANE_H + LANE_GAP);
  const globalTop = PAD_T + 14; // room for a section label
  const workerTop = globalTop + globalH + SECTION_GAP + 14;
  const height = workerTop + workerH + AXIS_H;

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setWidth(el.clientWidth));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  const plotL = GUTTER;
  const plotW = Math.max(1, width - GUTTER - PAD_R);
  const xOf = (t: number) => {
    const { lo, hi } = view.current;
    return plotL + ((t - lo) / (hi - lo)) * plotW;
  };
  const tOf = (x: number) => {
    const { lo, hi } = view.current;
    return lo + ((x - plotL) / plotW) * (hi - lo);
  };

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    const { lo, hi } = view.current;

    // section labels
    ctx.fillStyle = "#62666d";
    ctx.font = "600 11px ui-monospace, monospace";
    ctx.textBaseline = "alphabetic";
    ctx.fillText("ALL TASKS", 6, globalTop - 3);
    ctx.fillText("PER WORKER", 6, workerTop - 3);

    const drawRect = (s: TaskSpan, y: number) => {
      if (s.end < lo || s.start > hi) return;
      const x0 = Math.max(plotL, xOf(s.start));
      const x1 = Math.min(width - PAD_R, xOf(s.end));
      if (x1 <= x0) return;
      ctx.fillStyle = colorOf(s.layer);
      ctx.fillRect(x0, y, Math.max(1, x1 - x0), LANE_H);
    };

    // global lane backgrounds + rects
    ctx.fillStyle = "#f4f5f7";
    for (let i = 0; i < globalLanes; i++)
      ctx.fillRect(plotL, globalTop + i * (LANE_H + LANE_GAP), plotW, LANE_H);
    for (const g of global) drawRect(g.span, globalTop + g.lane * (LANE_H + LANE_GAP));

    // per-worker lanes
    ctx.fillStyle = "#f7f8fa";
    workers.forEach((_, i) =>
      ctx.fillRect(plotL, workerTop + i * (LANE_H + LANE_GAP), plotW, LANE_H),
    );
    ctx.fillStyle = "#62666d";
    ctx.font = "10px ui-monospace, monospace";
    workers.forEach((w, i) => {
      const y = workerTop + i * (LANE_H + LANE_GAP);
      ctx.fillStyle = "#8a8f98";
      ctx.fillText(w.replace(/^tcp:\/\//, "").slice(0, 15), 6, y + LANE_H - 5);
    });
    for (const s of spans) {
      const lane = laneOf.get(s.worker) ?? 0;
      drawRect(s, workerTop + lane * (LANE_H + LANE_GAP));
    }

    // axis
    ctx.fillStyle = "#8a8f98";
    ctx.font = "10px ui-monospace, monospace";
    ctx.textBaseline = "top";
    const ticks = 8;
    for (let i = 0; i <= ticks; i++) {
      const t = lo + ((hi - lo) * i) / ticks;
      const x = xOf(t);
      if (x < plotL - 1) continue;
      ctx.fillText(`${(t - domain.lo).toFixed(1)}s`, Math.min(x, width - 28), height - AXIS_H + 2);
    }
  });

  const canvasXY = (e: React.MouseEvent) => {
    const r = canvasRef.current!.getBoundingClientRect();
    return { cx: e.clientX - r.left, cy: e.clientY - r.top };
  };

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const { cx } = canvasXY(e);
    if (cx < plotL) return;
    const { lo, hi } = view.current;
    const tc = tOf(cx);
    const step = Math.min(0.25, Math.abs(e.deltaY) / 400 + 0.06);
    const factor = e.deltaY < 0 ? 1 - step : 1 + step; // in = shrink window
    let nlo = tc - (tc - lo) * factor;
    let nhi = tc + (hi - tc) * factor;
    // clamp to domain and a min window
    nlo = Math.max(domain.lo, nlo);
    nhi = Math.min(domain.hi, nhi);
    if (nhi - nlo > 0.02) {
      view.current = { lo: nlo, hi: nhi };
      redraw();
    }
  };

  const onDown = (e: React.MouseEvent) => {
    const { cx } = canvasXY(e);
    if (cx < plotL) return;
    if (e.shiftKey) {
      marquee.current = { x0: cx };
      setBox({ x: cx, w: 0 });
      return;
    }
    drag.current = { x: cx, lo: view.current.lo, hi: view.current.hi };
  };
  const onMove = (e: React.MouseEvent) => {
    const { cx, cy } = canvasXY(e);
    if (marquee.current) {
      setBox({ x: Math.min(marquee.current.x0, cx), w: Math.abs(cx - marquee.current.x0) });
      return;
    }
    if (drag.current) {
      const { lo, hi } = drag.current;
      const dt = ((cx - drag.current.x) / plotW) * (hi - lo);
      let nlo = lo - dt;
      let nhi = hi - dt;
      const span = hi - lo;
      if (nlo < domain.lo) {
        nlo = domain.lo;
        nhi = domain.lo + span;
      }
      if (nhi > domain.hi) {
        nhi = domain.hi;
        nlo = domain.hi - span;
      }
      view.current = { lo: nlo, hi: nhi };
      setHover(null);
      redraw();
      return;
    }
    // hover hit-test
    const t = tOf(cx);
    const yrow = (top: number, n: number) => {
      const i = Math.floor((cy - top) / (LANE_H + LANE_GAP));
      return i >= 0 && i < n ? i : -1;
    };
    let found: TaskSpan | null = null;
    const gi = yrow(globalTop, globalLanes);
    if (gi >= 0) {
      for (const g of global)
        if (g.lane === gi && g.span.start <= t && g.span.end >= t) found = g.span;
    } else {
      const wi = yrow(workerTop, workers.length);
      if (wi >= 0) {
        const w = workers[wi];
        for (const s of spans)
          if (s.worker === w && s.start <= t && s.end >= t) found = s;
      }
    }
    setHover(found ? { x: cx, y: cy, span: found } : null);
  };
  const onUp = () => {
    if (marquee.current) {
      const m = box;
      marquee.current = null;
      setBox(null);
      if (m && m.w > 6) {
        const nlo = tOf(m.x);
        const nhi = tOf(m.x + m.w);
        if (nhi - nlo > 0.02) {
          view.current = { lo: nlo, hi: nhi };
          redraw();
        }
      }
      return;
    }
    drag.current = null;
  };
  const reset = () => {
    view.current = { lo: domain.lo, hi: domain.hi };
    redraw();
  };

  if (spans.length === 0) return <div className="empty">No task spans yet.</div>;

  const zoomed = view.current.lo > domain.lo + 1e-6 || view.current.hi < domain.hi - 1e-6;

  return (
    <div className="tstream-wrap" ref={wrapRef} style={{ position: "relative" }}>
      <canvas
        ref={canvasRef}
        style={{
          width,
          height,
          cursor: box ? "crosshair" : drag.current ? "grabbing" : "default",
        }}
        onWheel={onWheel}
        onMouseDown={onDown}
        onMouseMove={onMove}
        onMouseUp={onUp}
        onMouseLeave={() => {
          drag.current = null;
          marquee.current = null;
          setBox(null);
          setHover(null);
        }}
      />
      {box && (
        <div
          className="gc-marquee"
          style={{ left: box.x, top: PAD_T, width: box.w, height: height - PAD_T - AXIS_H }}
        />
      )}
      <div className="tstream-tools">
        {zoomed && (
          <button className="btn" onClick={reset}>
            reset zoom
          </button>
        )}
      </div>
      {hover && (
        <div
          className="tstream-tip"
          style={{ left: Math.min(hover.x + 12, width - 220), top: hover.y + 12 }}
        >
          <div className="mono">{shortKey(hover.span.key)}</div>
          <div className="faint small">{baseName(hover.span.layer)}</div>
          <div className="small">
            {(hover.span.end - hover.span.start).toFixed(3)}s ·{" "}
            {hover.span.worker.replace(/^tcp:\/\//, "")}
          </div>
        </div>
      )}
      <div className="faint small" style={{ padding: "6px 8px" }}>
        scroll to zoom time · drag to pan · shift-drag to box-zoom · hover a task
      </div>
    </div>
  );
}
