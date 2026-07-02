"use client";

import {
  Background,
  Controls,
  Handle,
  type Node,
  type Edge,
  type NodeProps,
  Position,
  ReactFlow,
} from "@xyflow/react";
import { useMemo } from "react";
import { useDeaths, useGraph } from "@/lib/api";
import { layerToken } from "@/lib/format";

type NodeData = { name: string; src?: string; hot: boolean };

function LayerNode({ data }: NodeProps) {
  const d = data as NodeData;
  return (
    <div className={`gnode${d.hot ? " hot" : ""}`}>
      <Handle type="target" position={Position.Top} style={{ opacity: 0 }} />
      <div className="gname">{d.name}</div>
      {d.src && <div className="gsrc">{d.src}</div>}
      <Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
    </div>
  );
}

const nodeTypes = { layer: LayerNode };

// Longest-path depth from roots — a compact layered DAG layout without pulling
// in a heavyweight graph-layout dependency.
function depths(layers: string[], deps: Record<string, string[]>): Record<string, number> {
  const memo: Record<string, number> = {};
  const visiting = new Set<string>();
  const depth = (l: string): number => {
    if (l in memo) return memo[l];
    if (visiting.has(l)) return 0;
    visiting.add(l);
    const ds = deps[l] ?? [];
    const d = ds.length === 0 ? 0 : 1 + Math.max(...ds.map(depth));
    visiting.delete(l);
    memo[l] = d;
    return d;
  };
  layers.forEach(depth);
  return memo;
}

export function TaskGraph({ runId }: { runId: string }) {
  const { data: graph, isLoading } = useGraph(runId);
  const { data: deaths } = useDeaths(runId);

  const { nodes, edges } = useMemo(() => {
    if (!graph) return { nodes: [] as Node[], edges: [] as Edge[] };
    const layerNames = graph.layers.map((l) => l.layer);
    const srcByLayer = new Map(graph.layers.map((l) => [l.layer, `${l.filename.split("/").pop()}:${l.lineno}`]));
    const hotTokens = new Set(
      (deaths ?? []).flatMap((d) => d.suspect_keys.map(layerToken)),
    );
    const isHot = (layer: string) =>
      [...hotTokens].some((t) => t.startsWith(layer) || layer.startsWith(t));

    const d = depths(layerNames, graph.layer_dependencies);
    const perDepth: Record<number, number> = {};
    const nodes: Node[] = layerNames.map((layer) => {
      const depth = d[layer] ?? 0;
      const col = perDepth[depth] ?? 0;
      perDepth[depth] = col + 1;
      return {
        id: layer,
        type: "layer",
        position: { x: col * 240, y: depth * 130 },
        data: { name: layer.split("-").slice(0, -1).join("-") || layer, src: srcByLayer.get(layer), hot: isHot(layer) },
      };
    });
    const edges: Edge[] = [];
    for (const [layer, ds] of Object.entries(graph.layer_dependencies)) {
      for (const dep of ds) {
        edges.push({ id: `${dep}->${layer}`, source: dep, target: layer, animated: false });
      }
    }
    return { nodes, edges };
  }, [graph, deaths]);

  if (isLoading) return <div className="spinner">Loading…</div>;
  if (!graph || graph.layers.length === 0)
    return (
      <div className="empty">
        No task graph uploaded for this run. Capture it with <code>daskgenie.track()</code> and push
        it with <code>upload_graph()</code>.
      </div>
    );

  return (
    <div className="rf-wrap">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
      >
        <Background color="#e6e8ec" gap={22} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
