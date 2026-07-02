"""A ``WorkerPlugin`` that samples per-task memory and captures chunk metadata,
then batches it to the collector over HTTP.

Design constraints from the spec, and how they shape this code:

- **Tier 1, nearly free.** We only read numbers Dask/psutil already compute
  (RSS, managed nbytes, the executing set). No allocation tracing here.
- **Never cause memory pressure.** Buffers are bounded ``deque``s with a
  ``maxlen``; when full, the oldest samples are dropped rather than growing
  without limit.
- **Never crash the user's job.** Every sampling/flush operation is wrapped so
  a failure degrades to "no data", never an exception that could take down a
  worker. That is also why pushes are best-effort: on worker death the
  unsent tail is expected to be lost — the scheduler plugin is the backstop.
"""

from __future__ import annotations

import logging
import threading
import time
import urllib.request
from collections import deque
from typing import TYPE_CHECKING, Any

import psutil
from distributed.diagnostics.plugin import WorkerPlugin

from daskgenie.common.schemas import ChunkMeta, MemorySample, SampleBatch

if TYPE_CHECKING:
    from dask.typing import Key
    from distributed import Worker
    from distributed.worker_state_machine import TaskState, TaskStateState

logger = logging.getLogger("daskgenie.worker_plugin")

# Bound the buffers so a collector outage can never turn the profiler into the
# thing that OOMs the worker. At 200 ms sampling, 5000 samples is ~16 minutes
# of backlog before the oldest are dropped.
_MAX_SAMPLES = 5000
_MAX_CHUNKS = 5000


class MemoryProfilerPlugin(WorkerPlugin):
    """Sample RSS + managed memory per task and record input chunk metadata."""

    name = "daskgenie-memory-profiler"

    # Runtime state, created in setup() on the worker (see __init__ note).
    _worker: Worker | None = None
    _proc: psutil.Process
    _samples: deque[MemorySample]
    _chunks: deque[ChunkMeta]
    _seen_chunk_keys: set[str]
    _lock: threading.Lock
    _stop: threading.Event
    _thread: threading.Thread | None = None

    def __init__(
        self,
        collector_url: str,
        *,
        sample_interval: float = 0.2,
        flush_interval: float = 2.0,
        http_timeout: float = 5.0,
    ) -> None:
        # Only config lives here: the plugin is pickled and shipped to every
        # worker, so it must not hold locks/threads/deques (they aren't
        # picklable). All runtime state is created in setup(), on the worker.
        self.collector_url = collector_url.rstrip("/")
        self.sample_interval = sample_interval
        self.flush_interval = flush_interval
        self.http_timeout = http_timeout

    # -- WorkerPlugin hooks -------------------------------------------------

    def setup(self, worker: Worker) -> None:
        self._worker = worker
        self._proc = psutil.Process()
        self._samples: deque[MemorySample] = deque(maxlen=_MAX_SAMPLES)
        self._chunks: deque[ChunkMeta] = deque(maxlen=_MAX_CHUNKS)
        self._seen_chunk_keys: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="daskgenie-sampler", daemon=True)
        self._thread.start()

    def teardown(self, worker: Worker) -> None:
        if self._thread is None:  # setup never ran
            return
        self._stop.set()
        self._thread.join(timeout=self.flush_interval + self.http_timeout)
        self._flush()  # best-effort final drain

    def transition(
        self,
        key: Key,
        start: TaskStateState,
        finish: TaskStateState,
        **kwargs: Any,
    ) -> None:
        # Record input chunk metadata exactly once, when a task begins running.
        if finish != "executing":
            return
        try:
            self._capture_chunks(key)
        except Exception:  # noqa: BLE001 - profiling must never crash the worker
            logger.debug("chunk capture failed for %s", key, exc_info=True)

    # -- internals ----------------------------------------------------------

    def _capture_chunks(self, key: Key) -> None:
        worker = self._worker
        if worker is None:
            return
        ts: TaskState | None = worker.state.tasks.get(key)
        if ts is None:
            return
        # A task's inputs are its dependencies' outputs, already materialized in
        # worker.data by the time it starts executing. Those concrete arrays are
        # what actually occupy memory — inspect them, not the lazy dask objects.
        for dep in ts.dependencies:
            data = worker.data.get(dep.key)
            meta = _describe_array(dep.key, data)
            if meta is not None and meta.task_key not in self._seen_chunk_keys:
                self._seen_chunk_keys.add(meta.task_key)
                with self._lock:
                    self._chunks.append(meta)

    def _sample(self) -> None:
        worker = self._worker
        if worker is None:
            return
        try:
            rss = self._proc.memory_info().rss
            managed = int(getattr(worker.state, "nbytes", 0))
            executing = [_key_str(ts.key) for ts in worker.state.executing]
        except Exception:  # noqa: BLE001 - degrade to no data, never crash
            logger.debug("sample failed", exc_info=True)
            return
        sample = MemorySample(
            timestamp=time.time(),
            rss_bytes=rss,
            managed_bytes=managed,
            executing_keys=executing,
        )
        with self._lock:
            self._samples.append(sample)

    def _drain(self) -> SampleBatch | None:
        worker = self._worker
        if worker is None:
            return None
        with self._lock:
            if not self._samples and not self._chunks:
                return None
            batch = SampleBatch(
                worker=worker.address,
                samples=list(self._samples),
                chunks=list(self._chunks),
            )
            self._samples.clear()
            self._chunks.clear()
        return batch

    def _flush(self) -> None:
        batch = self._drain()
        if batch is None:
            return
        try:
            self._post(batch)
        except Exception:  # noqa: BLE001 - collector down must not affect the job
            logger.debug("flush to collector failed", exc_info=True)

    def _post(self, batch: SampleBatch) -> None:
        payload = batch.model_dump_json().encode("utf-8")
        req = urllib.request.Request(
            f"{self.collector_url}/ingest/samples",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.http_timeout):  # noqa: S310
            pass

    def _run(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.is_set():
            self._sample()
            now = time.monotonic()
            if now - last_flush >= self.flush_interval:
                self._flush()
                last_flush = now
            self._stop.wait(self.sample_interval)


def _key_str(key: object) -> str:
    # Dask keys are often tuples like ("rechunk-merge-abc", 0, 1); str() gives a
    # stable, join-friendly form that matches what the graph/source map uses.
    return key if isinstance(key, str) else str(key)


def _describe_array(key: object, data: object) -> ChunkMeta | None:
    shape = getattr(data, "shape", None)
    dtype = getattr(data, "dtype", None)
    nbytes = getattr(data, "nbytes", None)
    if shape is None or nbytes is None:
        return None
    try:
        return ChunkMeta(
            task_key=_key_str(key),
            shape=tuple(int(d) for d in shape),
            dtype=str(dtype),
            nbytes=int(nbytes),
        )
    except (TypeError, ValueError):
        return None
