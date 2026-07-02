// Mirror of daskgenie.common.schemas — keep in sync with the pydantic models.

export interface RunInfo {
  id: string;
  name: string;
  created_at: number;
  counts: { samples?: number; deaths?: number; workers?: number };
}

export interface ChunkMeta {
  task_key: string;
  shape: number[];
  dtype: string;
  nbytes: number;
}

export interface DeathEvent {
  timestamp: number;
  worker: string;
  suspect_keys: string[];
  suspect_chunks: ChunkMeta[];
  suspected_oom: boolean;
  reason: string;
}

export interface Sample {
  worker: string;
  timestamp: number;
  rss_bytes: number;
  managed_bytes: number;
  executing_keys: string[];
}

export interface GraphLayer {
  layer: string;
  filename: string;
  lineno: number;
  code_snippet: string;
}

export interface GraphNode {
  key: string;
  layer: string;
}

export interface TaskSpan {
  key: string;
  layer: string;
  start: number;
  end: number;
  worker: string;
}

export interface LayerStat {
  layer: string;
  count: number;
  total_seconds: number;
  longest_seconds: number;
}

export interface GraphData {
  run_id: string;
  layers: GraphLayer[];
  layer_dependencies: Record<string, string[]>;
  nodes: GraphNode[];
  edges: [string, string][];
  task_count: number;
  truncated: boolean;
}
