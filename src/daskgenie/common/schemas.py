"""Versioned payload schemas exchanged between the plugins and the collector.

Every model that crosses a process boundary lives here. ``SCHEMA_VERSION`` is
stamped onto each batch/upload so the collector can reject or migrate payloads
from a mismatched plugin build instead of silently mis-parsing them.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# Bump on any breaking change to the models below. The collector compares this
# against the value carried on incoming payloads.
# v2: introduced the first-class "run" — every payload now carries a run_id.
# v3: GraphUpload carries the full task graph (nodes/edges/task_count).
# v4: SampleBatch carries task spans (per-task start/end) for the timeline.
# v5: deep memory (memray) — AllocationSite/MemoryEpoch/TaskMemory + live
#     WorkerStatus heartbeats; DeathEvent gains suspect_sites.
# v6: MemoryEpoch carries full folded call stacks (AllocStack) for a real
#     per-worker flamegraph / memray-style tree.
SCHEMA_VERSION = 6


class ChunkMeta(BaseModel):
    """Shape/dtype/size of one concrete array a task operated on.

    Captured on the worker when a task starts, by inspecting the materialized
    inputs already resident in ``worker.data``. ``nbytes`` is what actually
    sits in memory — the number that adds up to an OOM.
    """

    task_key: str
    shape: tuple[int, ...]
    dtype: str
    nbytes: int


class MemorySample(BaseModel):
    """One point-in-time memory reading, tagged with the running task keys."""

    timestamp: float  # unix epoch seconds
    rss_bytes: int  # process resident set size
    managed_bytes: int  # Dask-tracked (managed) memory
    executing_keys: list[str]  # task keys running when this sample was taken


class TaskSpan(BaseModel):
    """One task's execution interval, for the task-stream / timeline view."""

    key: str
    layer: str
    start: float  # unix epoch seconds
    end: float
    worker: str


class AllocationSite(BaseModel):
    """One hot allocation source line, folded from a memray high-water-mark
    stack to the first *user* frame (the same non-library frame the source map
    keys on). ``hwm_bytes`` is the bytes live at the process high-water mark
    attributed to this line — the number that adds up to an OOM.

    ``task_key``/``layer`` are filled when the owning epoch overlaps a single
    task's span; otherwise blank (the line ran outside any tracked task).
    """

    filename: str
    lineno: int
    function: str
    hwm_bytes: int
    n_allocations: int = 0
    task_key: str = ""
    layer: str = ""


class StackFrame(BaseModel):
    """One frame of a call stack: the function and the source line it lives on."""

    function: str
    filename: str
    lineno: int


class AllocStack(BaseModel):
    """One full call path (root -> allocation site) and the high-water-mark bytes
    attributed to it — the unit a flamegraph / memray-style tree is built from.
    """

    frames: list[StackFrame] = Field(default_factory=list)  # root -> leaf
    hwm_bytes: int
    n_allocations: int = 0


class MemoryEpoch(BaseModel):
    """One memray rotation window on one worker: the high-water-mark allocation
    sites (folded to the user line) and full call stacks live during
    ``[start, end]``. Epochs give time-resolved attribution without an unbounded
    capture file.
    """

    worker: str
    start: float  # unix epoch seconds
    end: float
    peak_rss: int
    sites: list[AllocationSite] = Field(default_factory=list)
    stacks: list[AllocStack] = Field(default_factory=list)


class TaskMemory(BaseModel):
    """Per-task deep memory attribution: how much RSS the task added and the
    allocation lines that dominated while it ran.
    """

    key: str
    layer: str
    worker: str
    peak_rss_delta: int
    top_sites: list[AllocationSite] = Field(default_factory=list)


class WorkerStatus(BaseModel):
    """A live heartbeat for the Workers view — the native-dask-style snapshot of
    one worker at one instant. ``memory_limit`` is 0 when unknown (local
    schedulers have no per-worker limit).
    """

    worker: str
    timestamp: float
    rss_bytes: int
    managed_bytes: int
    memory_limit: int = 0
    cpu: float = 0.0
    nthreads: int = 0
    executing: int = 0
    ready: int = 0


class SampleBatch(BaseModel):
    """A batch of samples + freshly-seen chunk metadata + task spans, plus the
    optional deep-memory (memray) epochs / per-task memory and live statuses.
    """

    schema_version: int = Field(default=SCHEMA_VERSION)
    run_id: str
    worker: str  # worker address, e.g. "tcp://127.0.0.1:39001"
    samples: list[MemorySample] = Field(default_factory=list)
    chunks: list[ChunkMeta] = Field(default_factory=list)
    spans: list[TaskSpan] = Field(default_factory=list)
    epochs: list[MemoryEpoch] = Field(default_factory=list)
    task_memory: list[TaskMemory] = Field(default_factory=list)
    statuses: list[WorkerStatus] = Field(default_factory=list)


class GraphLayer(BaseModel):
    """One task-graph layer mapped to the user source line that built it."""

    layer: str
    filename: str
    lineno: int
    code_snippet: str


class GraphNode(BaseModel):
    """One task node in the concrete task graph. ``layer`` joins to the source
    map; ``key`` is the exact Dask task key.
    """

    key: str
    layer: str


class GraphUpload(BaseModel):
    """The layer -> source map, layer dependency edges, and (optionally) the
    full task-level graph for one run.
    """

    schema_version: int = Field(default=SCHEMA_VERSION)
    run_id: str
    layers: list[GraphLayer] = Field(default_factory=list)
    layer_dependencies: dict[str, list[str]] = Field(default_factory=dict)
    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[tuple[str, str]] = Field(default_factory=list)
    task_count: int = 0
    truncated: bool = False


class DeathEvent(BaseModel):
    """A worker-death post-mortem seed produced by the scheduler plugin.

    ``suspect_keys`` are the tasks that were in-flight on the worker when it
    vanished; ``suspected_oom`` is the scheduler's best guess (abrupt mid-task
    disappearance) vs. a clean scale-down, which it must not over-claim.
    """

    schema_version: int = Field(default=SCHEMA_VERSION)
    run_id: str
    timestamp: float
    worker: str
    suspect_keys: list[str] = Field(default_factory=list)
    suspect_chunks: list[ChunkMeta] = Field(default_factory=list)
    # Deep-memory attribution joined in by the collector: the allocation lines
    # that were at the high-water mark when the worker died.
    suspect_sites: list[AllocationSite] = Field(default_factory=list)
    suspected_oom: bool = False
    reason: str = ""


class RunCreate(BaseModel):
    """Client request to open a new run. ``origin`` is the caller's hostname (so
    a team dashboard shows which machine each run came from); the collector also
    records the request IP.
    """

    name: str = ""
    origin: str = ""


class RunInfo(BaseModel):
    """A profiling run: one cluster session's worth of data.

    ``counts`` is a small summary (samples/deaths/workers) the dashboard shows
    in the run list without pulling the full timeline.
    """

    id: str
    name: str
    created_at: float
    origin: str = ""  # caller hostname
    origin_ip: str = ""  # caller IP as seen by the collector
    counts: dict[str, int] = Field(default_factory=dict)
