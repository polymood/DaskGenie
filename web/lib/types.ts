// Mirror of daskgenie.common.schemas — keep in sync with the pydantic models.

export interface RunInfo {
  id: string;
  name: string;
  created_at: number;
  origin?: string;
  origin_ip?: string;
  counts: { samples?: number; deaths?: number; workers?: number };
}

export interface ChunkMeta {
  task_key: string;
  shape: number[];
  dtype: string;
  nbytes: number;
}

export interface AllocationSite {
  filename: string;
  lineno: number;
  function: string;
  hwm_bytes: number;
  n_allocations: number;
  task_key: string;
  layer: string;
}

export interface DeathEvent {
  timestamp: number;
  worker: string;
  suspect_keys: string[];
  suspect_chunks: ChunkMeta[];
  suspect_sites: AllocationSite[];
  suspected_oom: boolean;
  reason: string;
}

export interface WorkerStatus {
  worker: string;
  timestamp: number;
  rss_bytes: number;
  managed_bytes: number;
  memory_limit: number;
  cpu: number;
  nthreads: number;
  executing: number;
  ready: number;
}

// Aggregated per-source-line deep memory (peak bytes across epochs).
export interface AllocSiteRow {
  filename: string;
  lineno: number;
  function: string;
  hwm_bytes: number;
  n_allocations: number;
  layers: string[];
}

export interface TaskMemoryRow {
  key: string;
  layer: string;
  worker: string;
  peak_rss_delta: number;
  top_sites: AllocationSite[];
}

export interface FlameFrame {
  function: string;
  filename: string;
  lineno: number;
}

export interface FlameStack {
  frames: FlameFrame[];
  hwm_bytes: number;
  n_allocations: number;
}

export interface FlameData {
  workers: string[];
  stacks: FlameStack[];
}

export interface AllocTimelineRow {
  ts: number;
  layer: string;
  bytes: number;
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
