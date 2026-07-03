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

from daskgenie.common.arrays import describe_array, key_str, layer_of
from daskgenie.common.schemas import (
    ChunkMeta,
    MemoryEpoch,
    MemorySample,
    SampleBatch,
    TaskMemory,
    TaskSpan,
    WorkerStatus,
)

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
    _spans: deque[TaskSpan]
    _recent_spans: deque[TaskSpan]
    _statuses: deque[WorkerStatus]
    _epochs: deque[MemoryEpoch]
    _task_memory: deque[TaskMemory]
    _starts: dict[str, float]
    _seen_chunk_keys: set[tuple[str, str]]
    _lock: threading.Lock
    _stop: threading.Event
    _thread: threading.Thread | None = None
    _deep: Any = None  # DeepTracker | None, created in setup() when deep=True

    def __init__(
        self,
        collector_url: str,
        run_id: str,
        *,
        sample_interval: float = 0.2,
        flush_interval: float = 0.5,
        http_timeout: float = 5.0,
        deep: bool = False,
        deep_epoch_seconds: float = 5.0,
    ) -> None:
        # Only config lives here: the plugin is pickled and shipped to every
        # worker, so it must not hold locks/threads/deques (they aren't
        # picklable). All runtime state is created in setup(), on the worker.
        self.collector_url = collector_url.rstrip("/")
        self.run_id = run_id
        self.sample_interval = sample_interval
        self.flush_interval = flush_interval
        self.http_timeout = http_timeout
        self.deep = deep
        self.deep_epoch_seconds = deep_epoch_seconds

    # -- WorkerPlugin hooks -------------------------------------------------

    def setup(self, worker: Worker) -> None:
        self._worker = worker
        self._proc = psutil.Process()
        self._samples: deque[MemorySample] = deque(maxlen=_MAX_SAMPLES)
        self._chunks: deque[ChunkMeta] = deque(maxlen=_MAX_CHUNKS)
        self._spans: deque[TaskSpan] = deque(maxlen=_MAX_CHUNKS)
        # Retained across flushes so the deep tracker can correlate an epoch to
        # the tasks that ran in its window (the outbound _spans is cleared each
        # flush, ~10x more often than an epoch closes).
        self._recent_spans: deque[TaskSpan] = deque(maxlen=_MAX_CHUNKS)
        self._statuses: deque[WorkerStatus] = deque(maxlen=_MAX_SAMPLES)
        self._epochs: deque[MemoryEpoch] = deque(maxlen=_MAX_CHUNKS)
        self._task_memory: deque[TaskMemory] = deque(maxlen=_MAX_CHUNKS)
        self._starts: dict[str, float] = {}
        # dedup on (consumer_key, input_key) so re-entry doesn't re-record
        self._seen_chunk_keys: set[tuple[str, str]] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # prime cpu_percent so the first real reading isn't a meaningless 0.0
        try:
            self._proc.cpu_percent(None)
        except Exception:  # noqa: BLE001
            pass
        self._start_deep()
        self._thread = threading.Thread(target=self._run, name="daskgenie-sampler", daemon=True)
        self._thread.start()

    def _start_deep(self) -> None:
        self._deep = None
        if not self.deep:
            return
        try:
            from daskgenie.deepmem import DeepTracker

            self._deep = DeepTracker(
                worker_label=self._worker.address if self._worker else "",
                epoch_seconds=self.deep_epoch_seconds,
                spans_source=lambda: list(self._recent_spans),
                executing_source=self._executing_now,
            )
            self._deep.start()
        except Exception:  # noqa: BLE001 - deep is opt-in; degrade to Tier-1
            logger.debug("deep tracker unavailable, continuing Tier-1 only", exc_info=True)
            self._deep = None

    def teardown(self, worker: Worker) -> None:
        if self._thread is None:  # setup never ran
            return
        if self._deep is not None:
            try:
                self._deep.stop()
                self._collect_deep()
            except Exception:  # noqa: BLE001
                logger.debug("deep teardown failed", exc_info=True)
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
        try:
            if finish == "executing":
                # Task begins running: record its input chunks + start time.
                self._starts[key_str(key)] = time.time()
                self._capture_chunks(key)
            elif start == "executing":
                # Task left the executing state: close out its span.
                self._close_span(key)
        except Exception:  # noqa: BLE001 - profiling must never crash the worker
            logger.debug("transition handling failed for %s", key, exc_info=True)

    def _close_span(self, key: Key) -> None:
        sk = key_str(key)
        started = self._starts.pop(sk, None)
        if started is None:
            return
        worker = self._worker
        span = TaskSpan(
            key=sk,
            layer=layer_of(key),
            start=started,
            end=time.time(),
            worker=worker.address if worker is not None else "",
        )
        with self._lock:
            self._spans.append(span)
            self._recent_spans.append(span)

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
        # We key the metadata by the *consuming* task (this `key`), because that
        # is the key the scheduler reports as a suspect on worker death, so this
        # is what makes the death -> chunk join work downstream.
        consumer = key_str(key)
        for dep in ts.dependencies:
            dep_key = key_str(dep.key)
            dedup = (consumer, dep_key)
            if dedup in self._seen_chunk_keys:
                continue
            data = worker.data.get(dep.key)
            meta = describe_array(consumer, data)
            if meta is not None:
                self._seen_chunk_keys.add(dedup)
                with self._lock:
                    self._chunks.append(meta)

    def _sample(self) -> None:
        worker = self._worker
        if worker is None:
            return
        now = time.time()
        try:
            rss = self._proc.memory_info().rss
            managed = int(getattr(worker.state, "nbytes", 0))
            executing = [key_str(ts.key) for ts in worker.state.executing]
        except Exception:  # noqa: BLE001 - degrade to no data, never crash
            logger.debug("sample failed", exc_info=True)
            return
        sample = MemorySample(
            timestamp=now,
            rss_bytes=rss,
            managed_bytes=managed,
            executing_keys=executing,
        )
        status = self._status(worker, now, rss, managed, len(executing))
        with self._lock:
            self._samples.append(sample)
            if status is not None:
                self._statuses.append(status)

    def _status(
        self, worker: Worker, now: float, rss: int, managed: int, executing: int
    ) -> WorkerStatus | None:
        """A live heartbeat for the Workers view — best-effort, never raises."""
        try:
            cpu = self._proc.cpu_percent(None)
            nthreads = int(getattr(worker, "nthreads", 0) or 0)
            ready = len(getattr(worker.state, "ready", ()))
            limit = int(getattr(getattr(worker, "memory_manager", None), "memory_limit", 0) or 0)
        except Exception:  # noqa: BLE001
            return None
        return WorkerStatus(
            worker=worker.address,
            timestamp=now,
            rss_bytes=rss,
            managed_bytes=managed,
            memory_limit=limit,
            cpu=cpu,
            nthreads=nthreads,
            executing=executing,
            ready=ready,
        )

    def _executing_now(self) -> list[tuple[str, str]]:
        """(key, layer) for tasks executing on this worker right now — lets the
        deep tracker attribute an epoch to a task that is still running (and may
        be about to OOM), not only to tasks whose spans have already closed."""
        worker = self._worker
        if worker is None:
            return []
        try:
            return [(key_str(ts.key), layer_of(ts.key)) for ts in worker.state.executing]
        except Exception:  # noqa: BLE001
            return []

    def _collect_deep(self) -> None:
        """Drain finished memray epochs + per-task memory from the deep tracker."""
        if self._deep is None:
            return
        try:
            epochs, task_mem = self._deep.drain()
        except Exception:  # noqa: BLE001
            logger.debug("deep drain failed", exc_info=True)
            return
        if not epochs and not task_mem:
            return
        with self._lock:
            self._epochs.extend(epochs)
            self._task_memory.extend(task_mem)

    def _drain(self) -> SampleBatch | None:
        worker = self._worker
        if worker is None:
            return None
        with self._lock:
            if not any(
                (
                    self._samples,
                    self._chunks,
                    self._spans,
                    self._statuses,
                    self._epochs,
                    self._task_memory,
                )
            ):
                return None
            batch = SampleBatch(
                run_id=self.run_id,
                worker=worker.address,
                samples=list(self._samples),
                chunks=list(self._chunks),
                spans=list(self._spans),
                statuses=list(self._statuses),
                epochs=list(self._epochs),
                task_memory=list(self._task_memory),
            )
            self._samples.clear()
            self._chunks.clear()
            self._spans.clear()
            self._statuses.clear()
            self._epochs.clear()
            self._task_memory.clear()
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
            self._collect_deep()
            now = time.monotonic()
            if now - last_flush >= self.flush_interval:
                self._flush()
                last_flush = now
            self._stop.wait(self.sample_interval)
