"""Versioned payload schemas exchanged between the plugins and the collector.

Every model that crosses a process boundary lives here. ``SCHEMA_VERSION`` is
stamped onto each batch/upload so the collector can reject or migrate payloads
from a mismatched plugin build instead of silently mis-parsing them.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# Bump on any breaking change to the models below. The collector compares this
# against the value carried on incoming payloads.
SCHEMA_VERSION = 1


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


class SampleBatch(BaseModel):
    """A batch of samples + freshly-seen chunk metadata from one worker."""

    schema_version: int = Field(default=SCHEMA_VERSION)
    worker: str  # worker address, e.g. "tcp://127.0.0.1:39001"
    samples: list[MemorySample] = Field(default_factory=list)
    chunks: list[ChunkMeta] = Field(default_factory=list)


class GraphLayer(BaseModel):
    """One task-graph layer mapped to the user source line that built it."""

    layer: str
    filename: str
    lineno: int
    code_snippet: str


class GraphUpload(BaseModel):
    """The layer -> source map plus layer dependency edges for one run."""

    schema_version: int = Field(default=SCHEMA_VERSION)
    run_id: str
    layers: list[GraphLayer] = Field(default_factory=list)
    layer_dependencies: dict[str, list[str]] = Field(default_factory=dict)


class DeathEvent(BaseModel):
    """A worker-death post-mortem seed produced by the scheduler plugin.

    ``suspect_keys`` are the tasks that were in-flight on the worker when it
    vanished; ``suspected_oom`` is the scheduler's best guess (abrupt mid-task
    disappearance) vs. a clean scale-down, which it must not over-claim.
    """

    schema_version: int = Field(default=SCHEMA_VERSION)
    timestamp: float
    worker: str
    suspect_keys: list[str] = Field(default_factory=list)
    suspect_chunks: list[ChunkMeta] = Field(default_factory=list)
    suspected_oom: bool = False
    reason: str = ""
