"use client";

import { useMemo, useState } from "react";
import { useFlamegraph } from "@/lib/api";
import { useLive } from "@/lib/live";
import { useEffect } from "react";
import { bytes } from "@/lib/format";
import { layerColorMap } from "@/lib/colors";
import type { FlameFrame, FlameStack } from "@/lib/types";

const ROW = 20; // px per stack depth

interface FNode {
  key: string;
  frame: FlameFrame | null;
  bytes: number;
  children: Map<string, FNode>;
}

interface Cell {
  node: FNode;
  x: number; // 0..1 within the rendered root
  w: number;
  depth: number;
}

function shortFile(path: string): string {
  return path.split("/").slice(-1)[0] ?? path;
}
function isLib(path: string): boolean {
  return /site-packages|dist-packages|\/(numpy|dask|distributed|pandas|xarray|zarr)\//.test(path);
}

function buildTree(stacks: FlameStack[]): FNode {
  const root: FNode = { key: "root", frame: null, bytes: 0, children: new Map() };
  for (const s of stacks) {
    root.bytes += s.hwm_bytes;
    let node = root;
    for (const f of s.frames) {
      const k = `${f.function}|${f.filename}|${f.lineno}`;
      let c = node.children.get(k);
      if (!c) {
        c = { key: `${node.key}/${k}`, frame: f, bytes: 0, children: new Map() };
        node.children.set(k, c);
      }
      c.bytes += s.hwm_bytes;
      node = c;
    }
  }
  return root;
}

function layout(rootNode: FNode): { cells: Cell[]; maxDepth: number } {
  const cells: Cell[] = [];
  let maxDepth = 0;
  const place = (node: FNode, x: number, w: number, depth: number) => {
    if (w < 0.0015) return; // too thin to see or interact with
    cells.push({ node, x, w, depth });
    maxDepth = Math.max(maxDepth, depth);
    const kids = [...node.children.values()].sort((a, b) => b.bytes - a.bytes);
    let cx = x;
    for (const c of kids) {
      const cw = w * (c.bytes / node.bytes);
      place(c, cx, cw, depth + 1);
      cx += cw;
    }
  };
  place(rootNode, 0, 1, 0);
  return { cells, maxDepth };
}

function findByKey(root: FNode, key: string): FNode | null {
  if (root.key === key) return root;
  for (const c of root.children.values()) {
    const hit = findByKey(c, key);
    if (hit) return hit;
  }
  return null;
}

export function Flamegraph({ runId }: { runId: string }) {
  const [worker, setWorker] = useState<string | null>(null);
  const [focusKey, setFocusKey] = useState("root");
  const [selected, setSelected] = useState<FNode | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; node: FNode } | null>(null);
  const { data, mutate } = useFlamegraph(runId, worker);
  const { deepNonce } = useLive();
  const colorOf = useMemo(() => layerColorMap(), []);

  useEffect(() => {
    if (deepNonce > 0) mutate();
  }, [deepNonce, mutate]);

  const tree = useMemo(() => buildTree(data?.stacks ?? []), [data]);
  const focus = useMemo(() => findByKey(tree, focusKey) ?? tree, [tree, focusKey]);
  const { cells, maxDepth } = useMemo(() => layout(focus), [focus]);

  const total = tree.bytes || 1;
  const workers = data?.workers ?? [];
  const hasStacks = (data?.stacks?.length ?? 0) > 0;

  return (
    <div className="panel pad">
      <div className="flame-head">
        <span className="section-label" style={{ margin: 0 }}>
          Allocation flamegraph {worker ? `· ${shortWorker(worker)}` : "· all workers"} · peak{" "}
          {bytes(focus.bytes)}
        </span>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {focusKey !== "root" && (
            <button className="btn" onClick={() => setFocusKey("root")}>
              ⤢ reset zoom
            </button>
          )}
          <select
            className="fg-select mono"
            value={worker ?? ""}
            onChange={(e) => {
              setWorker(e.target.value || null);
              setFocusKey("root");
              setSelected(null);
            }}
          >
            <option value="">all workers</option>
            {workers.map((w) => (
              <option key={w} value={w}>
                {shortWorker(w)}
              </option>
            ))}
          </select>
        </div>
      </div>

      {!hasStacks && (
        <div className="empty">
          {data
            ? worker
              ? "No call-stack data for this worker."
              : "No call-stack data yet — needs the memray engine (deep=True) and one epoch."
            : "Loading…"}
        </div>
      )}

      <div className="flame-split" style={{ display: hasStacks ? "flex" : "none" }}>
        <div
          className="flame-canvas"
          style={{ height: (maxDepth + 1) * ROW }}
          onMouseLeave={() => setHover(null)}
        >
          {cells.map((c) => {
            const f = c.node.frame;
            const label = f ? `${f.function}` : "all";
            const lib = f ? isLib(f.filename) : false;
            const widthPct = c.w * 100;
            return (
              <div
                key={c.node.key}
                className={`flame-node${lib ? " lib" : ""}${
                  selected?.key === c.node.key ? " sel" : ""
                }`}
                style={{
                  left: `${c.x * 100}%`,
                  width: `${widthPct}%`,
                  top: c.depth * ROW,
                  height: ROW - 1,
                  background: f ? colorOf(shortFile(f.filename)) : "#9aa1ab",
                }}
                title={f ? `${f.function}  ${shortFile(f.filename)}:${f.lineno}` : "all"}
                onMouseMove={(e) => {
                  const r = e.currentTarget.parentElement!.getBoundingClientRect();
                  setHover({ x: e.clientX - r.left, y: e.clientY - r.top, node: c.node });
                }}
                onClick={() => {
                  setSelected(c.node);
                  if (c.node.children.size > 0) setFocusKey(c.node.key);
                }}
              >
                <span>{widthPct > 4 ? label : ""}</span>
              </div>
            );
          })}
          {hover && (
            <div
              className="tstream-tip"
              style={{
                left: Math.min(hover.x + 12, 600),
                top: hover.y + 14,
                pointerEvents: "none",
              }}
            >
              {hover.node.frame ? (
                <>
                  <div className="mono">{hover.node.frame.function}</div>
                  <div className="faint small mono">
                    {shortFile(hover.node.frame.filename)}:{hover.node.frame.lineno}
                  </div>
                </>
              ) : (
                <div className="mono">all allocations</div>
              )}
              <div className="small">
                {bytes(hover.node.bytes)} · {((hover.node.bytes / total) * 100).toFixed(1)}%
              </div>
            </div>
          )}
        </div>

        {selected?.frame && (
          <div className="flame-detail">
            <div className="fd-fn mono">{selected.frame.function}</div>
            <div className="srcpath">
              {selected.frame.filename}:{selected.frame.lineno}
            </div>
            <div className="kv">
              <span className="k">peak held</span>
              <b>{bytes(selected.bytes)}</b>
            </div>
            <div className="kv">
              <span className="k">share of total</span>
              <span>{((selected.bytes / total) * 100).toFixed(1)}%</span>
            </div>
            <div className="kv">
              <span className="k">in this library?</span>
              <span>{isLib(selected.frame.filename) ? "yes (framework)" : "your code"}</span>
            </div>
            <button className="btn" style={{ marginTop: 10 }} onClick={() => setSelected(null)}>
              close
            </button>
          </div>
        )}
      </div>
      <div className="faint small" style={{ marginTop: 8 }}>
        Each bar is a call frame; width is peak bytes held below it. Click to zoom, again for
        detail — the memray tree read, on your own code.
      </div>
    </div>
  );
}

function shortWorker(w: string): string {
  return w.replace(/^tcp:\/\//, "");
}
