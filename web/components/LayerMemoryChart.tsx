"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useAllocTimeline } from "@/lib/api";
import { useLive } from "@/lib/live";
import { baseName, layerColorMap } from "@/lib/colors";
import { bytes } from "@/lib/format";

const PAD_L = 66;
const PAD_R = 12;
const PAD_T = 10;
const AXIS_H = 20;
const HEIGHT = 360;
const MAX_LAYERS = 12;

interface Bucket {
  t: number;
  vals: Record<string, number>;
}

// Deep-memory allocations over time, stacked by task layer — canvas so it has
// the same scroll-to-zoom / drag-to-pan / shift-box-zoom as the other charts.
export function LayerMemoryChart({ runId }: { runId: string }) {
  const { data, mutate } = useAllocTimeline(runId);
  const { deepNonce } = useLive();
  const colorOf = useMemo(() => layerColorMap(), []);
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(900);
  const [hover, setHover] = useState<{ x: number; y: number; t: number } | null>(null);

  useEffect(() => {
    if (deepNonce > 0) mutate();
  }, [deepNonce, mutate]);

  const { buckets, layers, t0, domainHi, yMax } = useMemo(() => {
    const raw = (data ?? []).map((r) => ({ ...r, bytes: Number(r.bytes) }));
    if (raw.length === 0)
      return { buckets: [] as Bucket[], layers: [] as string[], t0: 0, domainHi: 1, yMax: 1 };
    const t0 = Math.min(...raw.map((r) => r.ts));
    const peak = new Map<string, number>();
    for (const r of raw) peak.set(r.layer, Math.max(peak.get(r.layer) ?? 0, r.bytes));
    const layers = [...peak.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([l]) => l)
      .slice(0, MAX_LAYERS);
    const keep = new Set(layers);
    const hasOther = raw.some((r) => !keep.has(r.layer));
    const byT = new Map<number, Record<string, number>>();
    for (const r of raw) {
      const t = Math.round((r.ts - t0) * 2) / 2;
      const row = byT.get(t) ?? {};
      const key = keep.has(r.layer) ? r.layer : "(other)";
      row[key] = (row[key] ?? 0) + r.bytes;
      byT.set(t, row);
    }
    if (hasOther) layers.push("(other)");
    const buckets: Bucket[] = [...byT.entries()]
      .map(([t, vals]) => ({ t, vals }))
      .sort((a, b) => a.t - b.t);
    let yMax = 1;
    let domainHi = 1;
    for (const b of buckets) {
      const tot = layers.reduce((a, l) => a + (b.vals[l] ?? 0), 0);
      if (tot > yMax) yMax = tot;
      if (b.t > domainHi) domainHi = b.t;
    }
    return { buckets, layers, t0, domainHi, yMax: yMax * 1.05 };
  }, [data]);

  const view = useRef({ lo: 0, hi: domainHi });
  const [, force] = useState(0);
  const redraw = () => force((n) => n + 1);
  const drag = useRef<{ x: number; lo: number; hi: number } | null>(null);
  const marquee = useRef<{ x0: number } | null>(null);
  const [box, setBox] = useState<{ x: number; w: number } | null>(null);

  useEffect(() => {
    view.current = { lo: 0, hi: domainHi };
    redraw();
  }, [domainHi]);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setWidth(el.clientWidth));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  const plotL = PAD_L;
  const plotW = Math.max(1, width - PAD_L - PAD_R);
  const plotT = PAD_T;
  const plotH = Math.max(1, HEIGHT - PAD_T - AXIS_H);
  const xOf = (t: number) =>
    plotL + ((t - view.current.lo) / (view.current.hi - view.current.lo)) * plotW;
  const tOf = (x: number) =>
    view.current.lo + ((x - plotL) / plotW) * (view.current.hi - view.current.lo);
  const yOf = (v: number) => plotT + (1 - v / yMax) * plotH;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = HEIGHT * dpr;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, HEIGHT);
    const { lo, hi } = view.current;

    // y grid
    ctx.strokeStyle = "#eef0f3";
    ctx.fillStyle = "#9298a2";
    ctx.font = "10px ui-monospace, monospace";
    ctx.textBaseline = "middle";
    for (let i = 0; i <= 4; i++) {
      const v = (yMax * i) / 4;
      const y = yOf(v);
      ctx.beginPath();
      ctx.moveTo(plotL, y);
      ctx.lineTo(width - PAD_R, y);
      ctx.stroke();
      ctx.fillText(bytes(v), 6, y);
    }

    const vis = buckets.filter((b) => b.t >= lo - 1 && b.t <= hi + 1);
    if (vis.length >= 2) {
      ctx.save();
      ctx.beginPath();
      ctx.rect(plotL, plotT, plotW, plotH);
      ctx.clip();
      // stack from bottom: keep a running cumulative per bucket
      const cum = new Array(vis.length).fill(0);
      for (let li = layers.length - 1; li >= 0; li--) {
        const layer = layers[li];
        ctx.beginPath();
        // top edge L->R
        for (let i = 0; i < vis.length; i++) {
          const top = cum[i] + (vis[i].vals[layer] ?? 0);
          const x = xOf(vis[i].t);
          const y = yOf(top);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        // bottom edge R->L
        for (let i = vis.length - 1; i >= 0; i--) {
          ctx.lineTo(xOf(vis[i].t), yOf(cum[i]));
        }
        ctx.closePath();
        ctx.fillStyle = colorOf(layer);
        ctx.globalAlpha = 0.78;
        ctx.fill();
        ctx.globalAlpha = 1;
        for (let i = 0; i < vis.length; i++) cum[i] += vis[i].vals[layer] ?? 0;
      }
      // hover crosshair
      if (hover) {
        ctx.strokeStyle = "#8a8f98";
        ctx.beginPath();
        ctx.moveTo(hover.x, plotT);
        ctx.lineTo(hover.x, plotT + plotH);
        ctx.stroke();
      }
      ctx.restore();
    }

    // x axis
    ctx.fillStyle = "#8a8f98";
    ctx.textBaseline = "top";
    for (let i = 0; i <= 8; i++) {
      const t = lo + ((hi - lo) * i) / 8;
      ctx.fillText(`${t.toFixed(1)}s`, Math.min(xOf(t), width - 28), plotT + plotH + 4);
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
    const factor = e.deltaY < 0 ? 1 - step : 1 + step;
    const nlo = Math.max(0, tc - (tc - lo) * factor);
    const nhi = Math.min(domainHi, tc + (hi - tc) * factor);
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
      if (nlo < 0) {
        nlo = 0;
        nhi = span;
      }
      if (nhi > domainHi) {
        nhi = domainHi;
        nlo = domainHi - span;
      }
      view.current = { lo: nlo, hi: nhi };
      setHover(null);
      redraw();
      return;
    }
    if (cx >= plotL && cx <= width - PAD_R) setHover({ x: cx, y: cy, t: tOf(cx) });
    else setHover(null);
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
    view.current = { lo: 0, hi: domainHi };
    redraw();
  };

  if (!data || buckets.length === 0)
    return (
      <div className="empty">
        No per-layer allocation data yet — needs the memray engine (<code>deep=True</code>).
      </div>
    );

  const zoomed = view.current.lo > 1e-6 || view.current.hi < domainHi - 1e-6;
  // nearest bucket for the hover tooltip
  let nb: Bucket | null = null;
  if (hover) for (const b of buckets) if (!nb || Math.abs(b.t - hover.t) < Math.abs(nb.t - hover.t)) nb = b;
  const hoverRows = nb
    ? layers
        .map((l) => ({ l, v: nb!.vals[l] ?? 0 }))
        .filter((r) => r.v > 0)
        .sort((a, b) => b.v - a.v)
    : [];

  return (
    <div className="panel pad">
      <div className="legend">
        {layers.map((l) => (
          <span key={l}>
            <span className="sw" style={{ background: colorOf(l) }} />
            {baseName(l)}
          </span>
        ))}
        <span className="faint">· scroll to zoom · drag to pan · shift-drag box-zoom</span>
      </div>
      <div className="mchart" ref={wrapRef} style={{ position: "relative" }}>
        <canvas
          ref={canvasRef}
          style={{
            width,
            height: HEIGHT,
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
            style={{ left: box.x, top: PAD_T, width: box.w, height: HEIGHT - PAD_T - AXIS_H }}
          />
        )}
        {zoomed && (
          <button className="btn mchart-reset" onClick={reset}>
            reset zoom
          </button>
        )}
        {hover && nb && hoverRows.length > 0 && (
          <div className="tstream-tip" style={{ left: Math.min(hover.x + 12, width - 220), top: 8 }}>
            <div className="small faint">t = {nb.t.toFixed(1)}s</div>
            {hoverRows.slice(0, 10).map((r) => (
              <div
                key={r.l}
                className="mono small"
                style={{ display: "flex", gap: 6, alignItems: "center" }}
              >
                <span className="sw" style={{ background: colorOf(r.l) }} />
                <span style={{ flex: 1 }}>{baseName(r.l)}</span>
                <b>{bytes(r.v)}</b>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
