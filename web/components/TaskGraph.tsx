"use client";

import dagre from "@dagrejs/dagre";
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  type Edge,
  type Node,
  type NodeProps,
  Position,
  ReactFlow,
} from "@xyflow/react";
import { useMemo, useState } from "react";
import { useChunks, useDeaths, useGraph } from "@/lib/api";
import { bytes, layerToken, shortKey } from "@/lib/format";
import type { GraphData, GraphLayer } from "@/lib/types";
import { CodeLine } from "./CodeLine";

const PALETTE = [
  "#3b5bdb", "#2b8a3e", "#c2410c", "#7048e8", "#0b7285", "#a61e4d", "#5c940d", "#862e9c",
];
const NODE_W = 168;
const NODE_H = 38;
// Above this many task nodes we render the layer-level graph instead — a few
// hundred nodes is the sweet spot for a readable, responsive DAG.
const TASK_LIMIT = 400;

type NodeData = { label: string; layer: string; color: string; hot: boolean };

function GraphNode({ data }: NodeProps) {
  const d = data as NodeData;
  return (
    <div className={`gnode${d.hot ? " hot" : ""}`} style={{ borderLeft: `3px solid ${d.color}` }}>
      <Handle type="target" position={Position.Top} style={{ opacity: 0 }} />
      <div className="gname">{d.label}</div>
      <Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
    </div>
  );
}
const nodeTypes = { g: GraphNode };

function baseName(layer: string): string {
  return layer.replace(/-[0-9a-f]{6,}$/i, "").split("-").slice(0, 2).join("-") || layer;
}

// Top-to-bottom layered layout via dagre.
function laidOut(
  raw: { id: string; data: NodeData }[],
  rawEdges: [string, string][],
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: "TB", nodesep: 22, ranksep: 55 });
  g.setDefaultEdgeLabel(() => ({}));
  raw.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }));
  const ids = new Set(raw.map((n) => n.id));
  const edges: Edge[] = [];
  for (const [s, t] of rawEdges) {
    if (ids.has(s) && ids.has(t)) {
      g.setEdge(s, t);
      edges.push({ id: `${s}->${t}`, source: s, target: t });
    }
  }
  dagre.layout(g);
  const nodes: Node[] = raw.map((n) => {
    const p = g.node(n.id);
    return {
      id: n.id,
      type: "g",
      position: { x: p.x - NODE_W / 2, y: p.y - NODE_H / 2 },
      data: n.data,
    };
  });
  return { nodes, edges };
}

function build(graph: GraphData, hot: Set<string>, colorOf: (l: string) => string) {
  const taskLevel = graph.nodes.length > 0 && graph.nodes.length <= TASK_LIMIT && !graph.truncated;
  if (taskLevel) {
    const raw = graph.nodes.map((n) => ({
      id: n.key,
      data: {
        label: shortKey(n.key),
        layer: n.layer,
        color: colorOf(n.layer),
        hot: [...hot].some((t) => n.key.includes(t) || layerToken(n.key).startsWith(t)),
      } as NodeData,
    }));
    return { ...laidOut(raw, graph.edges), taskLevel };
  }
  // layer-level aggregation
  const counts = new Map<string, number>();
  for (const n of graph.nodes) counts.set(n.layer, (counts.get(n.layer) ?? 0) + 1);
  const layerNames = new Set<string>([
    ...graph.layers.map((l) => l.layer),
    ...Object.keys(graph.layer_dependencies),
    ...Object.values(graph.layer_dependencies).flat(),
  ]);
  const raw = [...layerNames].map((layer) => ({
    id: layer,
    data: {
      label: `${baseName(layer)}${counts.get(layer) ? ` ×${counts.get(layer)}` : ""}`,
      layer,
      color: colorOf(layer),
      hot: [...hot].some((t) => t.startsWith(layer) || layer.startsWith(t)),
    } as NodeData,
  }));
  const edges: [string, string][] = [];
  for (const [layer, deps] of Object.entries(graph.layer_dependencies))
    for (const dep of deps) edges.push([dep, layer]);
  return { ...laidOut(raw, edges), taskLevel };
}

function SourcePanel({
  runId,
  selected,
  layer,
  source,
  taskLevel,
  onClose,
}: {
  runId: string;
  selected: string;
  layer: string;
  source?: GraphLayer;
  taskLevel: boolean;
  onClose: () => void;
}) {
  const { data: chunks } = useChunks(runId, taskLevel ? selected : null);
  return (
    <div className="gpanel">
      <div className="gpanel-head">
        <span className="mono">{taskLevel ? shortKey(selected) : baseName(layer)}</span>
        <button className="btn" onClick={onClose}>
          ✕
        </button>
      </div>
      <div className="gpanel-body">
        <div className="kv">
          <span className="k">layer</span>
          <span className="mono small">{layer}</span>
        </div>
        {source ? (
          <>
            <div className="srcpath">
              {source.filename}:{source.lineno}
            </div>
            <CodeLine code={source.code_snippet || "(no snippet)"} />
          </>
        ) : (
          <div className="faint small">No source mapping for this layer.</div>
        )}
        {taskLevel && chunks && chunks.length > 0 && (
          <div className="chunks" style={{ marginTop: 12 }}>
            {chunks.map((c, i) => (
              <span className="chunk" key={i}>
                ({c.shape.join(", ")}) {c.dtype} = <b>{bytes(c.nbytes)}</b>
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export function TaskGraph({ runId }: { runId: string }) {
  const { data: graph, isLoading } = useGraph(runId);
  const { data: deaths } = useDeaths(runId);
  const [selected, setSelected] = useState<string | null>(null);

  const hot = useMemo(
    () => new Set((deaths ?? []).flatMap((d) => d.suspect_keys.map(layerToken))),
    [deaths],
  );

  const colorOf = useMemo(() => {
    const map = new Map<string, string>();
    return (layer: string) => {
      const key = baseName(layer);
      if (!map.has(key)) map.set(key, PALETTE[map.size % PALETTE.length]);
      return map.get(key)!;
    };
  }, []);

  const { nodes, edges, taskLevel } = useMemo(
    () =>
      graph
        ? build(graph, hot, colorOf)
        : { nodes: [] as Node[], edges: [] as Edge[], taskLevel: false },
    [graph, hot, colorOf],
  );

  if (isLoading) return <div className="spinner">Loading…</div>;
  if (!graph || (graph.nodes.length === 0 && graph.layers.length === 0))
    return (
      <div className="empty">
        No task graph for this run. Pass the collection to{" "}
        <code>upload_graph(url, run_id, source_map, collection=result)</code> (or{" "}
        <code>LocalProfiler(..., collection=result)</code>).
      </div>
    );

  const layerOf = new Map(graph.nodes.map((n) => [n.key, n.layer]));
  const sourceOf = new Map(graph.layers.map((l) => [l.layer, l]));
  const selLayer = selected ? (taskLevel ? layerOf.get(selected) ?? "" : selected) : "";

  return (
    <>
      <div className="graph-note">
        {graph.task_count} tasks ·{" "}
        {taskLevel ? "task-level graph" : "layer-level graph (too large for task view)"} · click a
        node for its source
      </div>
      <div className="graph-split">
        <div className="rf-wrap" style={{ flex: 1 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            minZoom={0.1}
            proOptions={{ hideAttribution: true }}
            nodesDraggable={false}
            nodesConnectable={false}
            onNodeClick={(_e, node) => setSelected(node.id)}
          >
            <Background color="#e6e8ec" gap={22} />
            <Controls showInteractive={false} />
            <MiniMap pannable zoomable nodeColor={(n) => (n.data as NodeData).color} />
          </ReactFlow>
        </div>
        {selected && (
          <SourcePanel
            runId={runId}
            selected={selected}
            layer={selLayer}
            source={sourceOf.get(selLayer)}
            taskLevel={taskLevel}
            onClose={() => setSelected(null)}
          />
        )}
      </div>
    </>
  );
}
