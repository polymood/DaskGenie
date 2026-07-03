"use client";

import dagre from "@dagrejs/dagre";
import { useEffect, useMemo, useRef, useState } from "react";

export interface CNode {
  id: string;
  label: string;
  layer: string;
  color: string;
  hot: boolean;
}

interface Placed {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  data: CNode;
}

const NW = 26;
const NH = 14;
const MAX_SCALE = 40; // deep zoom for reading individual tasks
const MIN_SCALE = 0.04;
const LABEL_SCALE = 1.4; // show node labels once zoomed in past this

// A canvas DAG for large task graphs — the real connected graph, not a
// layer-level summary. dagre gives a top-to-bottom layered layout; we draw it
// on a canvas with pan (drag) and zoom (wheel) so thousands of nodes stay
// responsive where a DOM/SVG renderer would choke. Click hit-tests to select.
export function GraphCanvas({
  nodes,
  edges,
  onSelect,
  selected,
}: {
  nodes: CNode[];
  edges: [string, string][];
  onSelect: (id: string) => void;
  selected: string | null;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [size, setSize] = useState({ w: 900, h: 620 });
  const view = useRef({ scale: 1, tx: 0, ty: 0 });
  const drag = useRef<{ x: number; y: number; moved: boolean } | null>(null);
  const marquee = useRef<{ x0: number; y0: number } | null>(null);
  const [box, setBox] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; node: CNode } | null>(null);
  const [, force] = useState(0);
  const redraw = () => force((n) => n + 1);

  const layout = useMemo(() => {
    const g = new dagre.graphlib.Graph();
    g.setGraph({ rankdir: "TB", nodesep: 8, ranksep: 24 });
    g.setDefaultEdgeLabel(() => ({}));
    const ids = new Set(nodes.map((n) => n.id));
    nodes.forEach((n) => g.setNode(n.id, { width: NW, height: NH }));
    const drawnEdges: [string, string][] = [];
    for (const [s, t] of edges) {
      if (ids.has(s) && ids.has(t)) {
        g.setEdge(s, t);
        drawnEdges.push([s, t]);
      }
    }
    dagre.layout(g);
    const placed: Placed[] = nodes.map((n) => {
      const p = g.node(n.id);
      return { id: n.id, x: p.x, y: p.y, w: NW, h: NH, data: n };
    });
    const byId = new Map(placed.map((p) => [p.id, p]));
    const gw = (g.graph().width ?? 1000) as number;
    const gh = (g.graph().height ?? 1000) as number;
    return { placed, byId, drawnEdges, gw, gh };
  }, [nodes, edges]);

  // Fit to view whenever the graph changes.
  useEffect(() => {
    const { gw, gh } = layout;
    const scale = Math.min(size.w / (gw + 40), size.h / (gh + 40), 1.5);
    view.current = {
      scale,
      tx: (size.w - gw * scale) / 2,
      ty: 20,
    };
    redraw();
  }, [layout, size]);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() =>
      setSize({ w: el.clientWidth, h: el.clientHeight }),
    );
    ro.observe(el);
    setSize({ w: el.clientWidth, h: el.clientHeight });
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = size.w * dpr;
    canvas.height = size.h * dpr;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, size.w, size.h);
    const { scale, tx, ty } = view.current;
    ctx.save();
    ctx.translate(tx, ty);
    ctx.scale(scale, scale);

    // edges
    ctx.strokeStyle = "#c9ccd2";
    ctx.lineWidth = 0.5 / scale;
    ctx.beginPath();
    for (const [s, t] of layout.drawnEdges) {
      const a = layout.byId.get(s);
      const b = layout.byId.get(t);
      if (!a || !b) continue;
      ctx.moveTo(a.x, a.y + a.h / 2);
      ctx.lineTo(b.x, b.y - b.h / 2);
    }
    ctx.stroke();

    // nodes — cull to the visible world rect so deep zoom stays fast
    const showLabels = scale > LABEL_SCALE;
    const vx0 = -tx / scale;
    const vy0 = -ty / scale;
    const vx1 = (size.w - tx) / scale;
    const vy1 = (size.h - ty) / scale;
    const fontPx = Math.min(9, Math.max(5, 8));
    for (const p of layout.placed) {
      if (p.x < vx0 - p.w || p.x > vx1 + p.w || p.y < vy0 - p.h || p.y > vy1 + p.h) continue;
      ctx.fillStyle = p.data.color;
      ctx.fillRect(p.x - p.w / 2, p.y - p.h / 2, p.w, p.h);
      const isSel = p.id === selected;
      const isHover = hover?.node.id === p.id;
      if (p.data.hot || isSel || isHover) {
        ctx.strokeStyle = isSel ? "#1b1d21" : isHover ? "#e8590c" : "#d43b3b";
        ctx.lineWidth = (isSel || isHover ? 2.5 : 2) / scale;
        ctx.strokeRect(p.x - p.w / 2, p.y - p.h / 2, p.w, p.h);
      }
      if (showLabels) {
        ctx.fillStyle = "#1b1d21";
        ctx.font = `${fontPx}px ui-monospace, monospace`;
        ctx.textBaseline = "middle";
        ctx.save();
        ctx.beginPath();
        ctx.rect(p.x - p.w / 2, p.y - p.h / 2, p.w, p.h);
        ctx.clip();
        ctx.fillText(p.data.label, p.x - p.w / 2 + 2, p.y);
        ctx.restore();
      }
    }

    // highlight the active node's edges so its connections are unmistakable
    const active = hover?.node.id ?? selected;
    if (active) {
      ctx.strokeStyle = "#e8590c";
      ctx.lineWidth = 1.6 / scale;
      ctx.beginPath();
      for (const [s, t] of layout.drawnEdges) {
        if (s !== active && t !== active) continue;
        const a = layout.byId.get(s);
        const b = layout.byId.get(t);
        if (!a || !b) continue;
        ctx.moveTo(a.x, a.y + a.h / 2);
        ctx.lineTo(b.x, b.y - b.h / 2);
      }
      ctx.stroke();
    }
    ctx.restore();
  });

  const toWorld = (cx: number, cy: number) => {
    const { scale, tx, ty } = view.current;
    return { x: (cx - tx) / scale, y: (cy - ty) / scale };
  };

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const rect = canvasRef.current!.getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const before = toWorld(cx, cy);
    // finer step for precise control; trackpads send small deltas → scale them
    const step = Math.min(0.25, Math.abs(e.deltaY) / 400 + 0.06);
    const factor = e.deltaY < 0 ? 1 + step : 1 / (1 + step);
    const v = view.current;
    v.scale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, v.scale * factor));
    v.tx = cx - before.x * v.scale;
    v.ty = cy - before.y * v.scale;
    redraw();
  };

  const nodeAt = (cx: number, cy: number): Placed | null => {
    const w = toWorld(cx, cy);
    for (const p of layout.placed) {
      if (Math.abs(w.x - p.x) <= p.w / 2 + 1 && Math.abs(w.y - p.y) <= p.h / 2 + 1) return p;
    }
    return null;
  };

  const canvasXY = (e: React.MouseEvent) => {
    const rect = canvasRef.current!.getBoundingClientRect();
    return { cx: e.clientX - rect.left, cy: e.clientY - rect.top };
  };

  const onDown = (e: React.MouseEvent) => {
    // Shift-drag draws a box to zoom into that region; plain drag pans.
    if (e.shiftKey) {
      const { cx, cy } = canvasXY(e);
      marquee.current = { x0: cx, y0: cy };
      setBox({ x: cx, y: cy, w: 0, h: 0 });
      return;
    }
    drag.current = { x: e.clientX, y: e.clientY, moved: false };
  };
  const onMoveDrag = (e: React.MouseEvent) => {
    if (marquee.current) {
      const { cx, cy } = canvasXY(e);
      const { x0, y0 } = marquee.current;
      setBox({ x: Math.min(x0, cx), y: Math.min(y0, cy), w: Math.abs(cx - x0), h: Math.abs(cy - y0) });
      return;
    }
    if (drag.current) {
      const dx = e.clientX - drag.current.x;
      const dy = e.clientY - drag.current.y;
      if (Math.abs(dx) + Math.abs(dy) > 2) drag.current.moved = true;
      view.current.tx += dx;
      view.current.ty += dy;
      drag.current.x = e.clientX;
      drag.current.y = e.clientY;
      if (hover) setHover(null);
      redraw();
      return;
    }
    // hover hit-test for the tooltip + highlight
    const rect = canvasRef.current!.getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const p = nodeAt(cx, cy);
    if (p) setHover({ x: cx, y: cy, node: p.data });
    else if (hover) setHover(null);
  };
  const onUp = (e: React.MouseEvent) => {
    if (marquee.current) {
      marquee.current = null;
      const m = box;
      setBox(null);
      if (m && m.w > 8 && m.h > 8) {
        const w0 = toWorld(m.x, m.y);
        const w1 = toWorld(m.x + m.w, m.y + m.h);
        const worldW = Math.max(1, w1.x - w0.x);
        const worldH = Math.max(1, w1.y - w0.y);
        const scale = Math.max(
          MIN_SCALE,
          Math.min(MAX_SCALE, Math.min(size.w / worldW, size.h / worldH)),
        );
        const cxw = (w0.x + w1.x) / 2;
        const cyw = (w0.y + w1.y) / 2;
        view.current = { scale, tx: size.w / 2 - cxw * scale, ty: size.h / 2 - cyw * scale };
        redraw();
      }
      return;
    }
    const d = drag.current;
    drag.current = null;
    if (d && !d.moved) {
      const { cx, cy } = canvasXY(e);
      const p = nodeAt(cx, cy);
      if (p) onSelect(p.id);
    }
  };

  const zoomBy = (factor: number) => {
    const v = view.current;
    const cx = size.w / 2;
    const cy = size.h / 2;
    const before = toWorld(cx, cy);
    v.scale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, v.scale * factor));
    v.tx = cx - before.x * v.scale;
    v.ty = cy - before.y * v.scale;
    redraw();
  };
  const fit = () => {
    const { gw, gh } = layout;
    const scale = Math.min(size.w / (gw + 40), size.h / (gh + 40), 1.5);
    view.current = { scale, tx: (size.w - gw * scale) / 2, ty: 20 };
    redraw();
  };

  return (
    <div className="rf-wrap" ref={wrapRef} style={{ flex: 1, position: "relative" }}>
      <canvas
        ref={canvasRef}
        style={{
          width: size.w,
          height: size.h,
          cursor: box ? "crosshair" : drag.current ? "grabbing" : "grab",
        }}
        onWheel={onWheel}
        onMouseDown={onDown}
        onMouseMove={onMoveDrag}
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
          style={{ left: box.x, top: box.y, width: box.w, height: box.h }}
        />
      )}
      <div className="gc-zoom">
        <button className="btn" onClick={() => zoomBy(1.4)} title="Zoom in">
          +
        </button>
        <button className="btn" onClick={() => zoomBy(1 / 1.4)} title="Zoom out">
          −
        </button>
        <button className="btn" onClick={fit} title="Fit">
          ⤢
        </button>
        <span className="gc-scale mono">{Math.round(view.current.scale * 100)}%</span>
      </div>
      {hover && (
        <div
          className="tstream-tip"
          style={{ left: Math.min(hover.x + 12, size.w - 240), top: hover.y + 12 }}
        >
          <div className="mono">{hover.node.label}</div>
          <div className="faint small mono">{hover.node.layer}</div>
        </div>
      )}
      <div className="graph-hint faint small">
        scroll / +− to zoom · drag to pan · shift-drag to box-zoom · click a node
      </div>
    </div>
  );
}
