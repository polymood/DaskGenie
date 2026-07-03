"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { bytes } from "@/lib/format";
import type { DeathEvent, Sample } from "@/lib/types";

const PALETTE = ["#4f46e5", "#0891b2", "#16a34a", "#d97706", "#db2777", "#7c3aed"];
const PAD_L = 66;
const PAD_R = 12;
const PAD_T = 22; // room for the "death" labels
const AXIS_H = 20;

function shortWorker(w: string) {
  return w.replace(/^tcp:\/\//, "");
}

interface Series {
  worker: string;
  color: string;
  pts: { t: number; rss: number }[];
}

// Memory over time on a canvas, with the same navigation as the graph / task
// stream: scroll to zoom the time axis, drag to pan, shift-drag to box-zoom.
// Click a point to pin an instant (the parent's spike inspector).
export function MemoryChart({
  samples,
  deaths = [],
  selectedTime = null,
  onSelect,
  height = 400,
}: {
  samples: Sample[];
  deaths?: DeathEvent[];
  selectedTime?: number | null;
  onSelect?: (absTime: number) => void;
  height?: number;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(900);
  const [hover, setHover] = useState<{ x: number; y: number; t: number } | null>(null);

  const { t0, series, peak, domainHi } = useMemo(() => {
    if (samples.length === 0) return { t0: 0, series: [] as Series[], peak: 1, domainHi: 1 };
    const t0 = Math.min(...samples.map((s) => s.timestamp));
    const byW = new Map<string, { t: number; rss: number }[]>();
    let peak = 1;
    let domainHi = 1;
    for (const s of samples) {
      if (!s.worker) continue;
      const t = s.timestamp - t0;
      (byW.get(s.worker) ?? byW.set(s.worker, []).get(s.worker)!).push({ t, rss: s.rss_bytes });
      if (s.rss_bytes > peak) peak = s.rss_bytes;
      if (t > domainHi) domainHi = t;
    }
    const series: Series[] = [...byW.entries()].map(([worker, pts], i) => ({
      worker,
      color: PALETTE[i % PALETTE.length],
      pts: pts.sort((a, b) => a.t - b.t),
    }));
    return { t0, series, peak, domainHi };
  }, [samples]);

  const view = useRef({ lo: 0, hi: domainHi });
  const [, force] = useState(0);
  const redraw = () => force((n) => n + 1);
  const drag = useRef<{ x: number; lo: number; hi: number; moved: boolean } | null>(null);
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
  const plotH = Math.max(1, height - PAD_T - AXIS_H);
  const yMax = peak * 1.1;
  const xOf = (t: number) => plotL + ((t - view.current.lo) / (view.current.hi - view.current.lo)) * plotW;
  const tOf = (x: number) => view.current.lo + ((x - plotL) / plotW) * (view.current.hi - view.current.lo);
  const yOf = (rss: number) => plotT + (1 - rss / yMax) * plotH;

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

    // y grid + labels
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

    // clip to plot for the lines
    ctx.save();
    ctx.beginPath();
    ctx.rect(plotL, plotT, plotW, plotH);
    ctx.clip();
    for (const s of series) {
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      let started = false;
      for (const p of s.pts) {
        if (p.t < lo - 1 || p.t > hi + 1) {
          started = false;
          continue;
        }
        const x = xOf(p.t);
        const y = yOf(p.rss);
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else ctx.lineTo(x, y);
      }
      ctx.stroke();
    }
    // death markers
    for (const d of deaths) {
      const t = d.timestamp - t0;
      if (t < lo || t > hi) continue;
      const x = xOf(t);
      ctx.strokeStyle = "#d43b3b";
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(x, plotT);
      ctx.lineTo(x, plotT + plotH);
      ctx.stroke();
      ctx.setLineDash([]);
    }
    // selected time
    if (selectedTime != null) {
      const t = selectedTime - t0;
      if (t >= lo && t <= hi) {
        ctx.strokeStyle = "#e8590c";
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(xOf(t), plotT);
        ctx.lineTo(xOf(t), plotT + plotH);
        ctx.stroke();
      }
    }
    // hover crosshair + a dot on each line at the cursor time
    if (hover) {
      ctx.strokeStyle = "#c9ccd2";
      ctx.beginPath();
      ctx.moveTo(hover.x, plotT);
      ctx.lineTo(hover.x, plotT + plotH);
      ctx.stroke();
      for (const s of series) {
        let best: { t: number; rss: number } | null = null;
        for (const p of s.pts)
          if (!best || Math.abs(p.t - hover.t) < Math.abs(best.t - hover.t)) best = p;
        if (best && best.t >= lo && best.t <= hi) {
          const x = xOf(best.t);
          const y = yOf(best.rss);
          ctx.fillStyle = s.color;
          ctx.beginPath();
          ctx.arc(x, y, 3.5, 0, Math.PI * 2);
          ctx.fill();
          ctx.strokeStyle = "#fff";
          ctx.lineWidth = 1.2;
          ctx.stroke();
        }
      }
    }
    ctx.restore();

    // death labels (outside clip, at top)
    ctx.fillStyle = "#d43b3b";
    ctx.textBaseline = "alphabetic";
    for (const d of deaths) {
      const t = d.timestamp - t0;
      if (t < lo || t > hi) continue;
      ctx.fillText("death", Math.min(xOf(t) + 3, width - 34), plotT - 8);
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
    let nlo = tc - (tc - lo) * factor;
    let nhi = tc + (hi - tc) * factor;
    nlo = Math.max(0, nlo);
    nhi = Math.min(domainHi, nhi);
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
    drag.current = { x: cx, lo: view.current.lo, hi: view.current.hi, moved: false };
  };
  const onMove = (e: React.MouseEvent) => {
    const { cx, cy } = canvasXY(e);
    if (marquee.current) {
      setBox({ x: Math.min(marquee.current.x0, cx), w: Math.abs(cx - marquee.current.x0) });
      return;
    }
    if (drag.current) {
      const dx = cx - drag.current.x;
      if (Math.abs(dx) > 2) drag.current.moved = true;
      const { lo, hi } = drag.current;
      const dt = (dx / plotW) * (hi - lo);
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
    const d = drag.current;
    drag.current = null;
    if (d && !d.moved && onSelect) onSelect(t0 + tOf(d.x));
  };
  const reset = () => {
    view.current = { lo: 0, hi: domainHi };
    redraw();
  };

  if (samples.length === 0) return <div className="empty">No memory samples yet.</div>;

  const zoomed = view.current.lo > 1e-6 || view.current.hi < domainHi - 1e-6;
  const hoverVals =
    hover &&
    series.map((s) => {
      // nearest point to hovered time
      let best: { t: number; rss: number } | null = null;
      for (const p of s.pts) if (!best || Math.abs(p.t - hover.t) < Math.abs(best.t - hover.t)) best = p;
      return { worker: s.worker, color: s.color, rss: best?.rss ?? 0 };
    });

  return (
    <div className="panel pad">
      <div className="legend">
        {series.map((s) => (
          <span key={s.worker}>
            <span className="sw" style={{ background: s.color }} />
            {shortWorker(s.worker)}
          </span>
        ))}
        {onSelect && (
          <span className="faint">· scroll to zoom · drag to pan · shift-drag box-zoom · click to inspect</span>
        )}
      </div>
      <div className="mchart" ref={wrapRef} style={{ position: "relative" }}>
        <canvas
          ref={canvasRef}
          style={{ width, height, cursor: box ? "crosshair" : drag.current ? "grabbing" : onSelect ? "pointer" : "default" }}
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
          <div className="gc-marquee" style={{ left: box.x, top: PAD_T, width: box.w, height: height - PAD_T - AXIS_H }} />
        )}
        {zoomed && (
          <button className="btn mchart-reset" onClick={reset}>
            reset zoom
          </button>
        )}
        {hover && hoverVals && (
          <div className="tstream-tip" style={{ left: Math.min(hover.x + 12, width - 190), top: 8 }}>
            <div className="small faint">t = {hover.t.toFixed(2)}s</div>
            {hoverVals.map((h) => (
              <div
                key={h.worker}
                className="mono small"
                style={{ display: "flex", gap: 6, alignItems: "center" }}
              >
                <span className="sw" style={{ background: h.color }} />
                <span style={{ flex: 1 }}>{shortWorker(h.worker)}</span>
                <b>{bytes(h.rss)}</b>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
