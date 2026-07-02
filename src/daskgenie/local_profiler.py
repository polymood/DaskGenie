"""Profile Dask's *local* schedulers — synchronous, threaded, and processes —
via the ``dask.callbacks.Callback`` API.

The distributed worker/scheduler plugins only exist on ``dask.distributed``.
Everything else (``.compute()`` with ``scheduler="threads"|"synchronous"|
"processes"``, the default for bare dask arrays/dataframes) runs through the
callback hooks instead. This class is the local-scheduler analogue of the
worker plugin: it samples process RSS over time, tags each sample with the
tasks running at that instant, and records the shape/dtype/nbytes of each
task's output chunk — so the same dashboard works for any scheduler.

There is no death attribution here: a local scheduler has no workers to lose;
an OOM simply kills the process. The value is the memory-over-time and
per-chunk view.

Usage::

    import daskgenie as dg

    with dg.track() as source_map:
        result = build_pipeline()                      # your dask graph

    with dg.LocalProfiler("http://localhost:8765", run_name="threaded job",
                          source_map=source_map, collection=result) as prof:
        result.compute(scheduler="threads")
    # prof.run_id identifies the run in the dashboard
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Mapping
from typing import Any

import psutil
from dask.callbacks import Callback

from daskgenie import report
from daskgenie.common.arrays import describe_array, key_str, layer_of
from daskgenie.common.schemas import ChunkMeta, MemorySample, SampleBatch, TaskSpan
from daskgenie.graphcapture import SourceLocation

logger = logging.getLogger("daskgenie.local_profiler")

_MAX_BUFFER = 20000


class LocalProfiler(Callback):
    def __init__(
        self,
        collector_url: str,
        *,
        run_name: str = "",
        run_id: str | None = None,
        source_map: Mapping[str, SourceLocation] | None = None,
        collection: object | None = None,
        sample_interval: float = 0.1,
        worker_label: str | None = None,
    ) -> None:
        super().__init__()  # type: ignore[no-untyped-call]
        self.collector_url = collector_url.rstrip("/")
        self.sample_interval = sample_interval
        self.source_map = source_map
        self.collection = collection
        # One process, so one "worker" line on the memory chart. Default label
        # names the process so multiple hosts stay distinguishable.
        self.worker_label = worker_label or f"local-pid-{os.getpid()}"

        self.run_id = run_id or report.create_run(collector_url, run_name)
        self._proc = psutil.Process()
        self._running: set[str] = set()
        self._starts: dict[str, float] = {}
        self._samples: list[MemorySample] = []
        self._chunks: list[ChunkMeta] = []
        self._spans: list[TaskSpan] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- context manager: run the sampler for the whole `with` block ---------

    def __enter__(self) -> LocalProfiler:
        super().__enter__()  # type: ignore[no-untyped-call]  # register the callback hooks
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="daskgenie-local", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            if self._thread is not None:
                self._stop.set()
                self._thread.join(timeout=self.sample_interval + 5.0)
            self._flush()
            if self.source_map is not None:
                report.upload_graph(
                    self.collector_url, self.run_id, self.source_map, self.collection
                )
        finally:
            super().__exit__(*exc)  # type: ignore[no-untyped-call]  # unregister hooks

    # -- callback hooks (run inside the scheduler) ---------------------------

    def _pretask(self, key: Any, dsk: Any, state: Any) -> None:
        sk = key_str(key)
        with self._lock:
            self._running.add(sk)
            self._starts[sk] = time.time()

    def _posttask(self, key: Any, result: Any, dsk: Any, state: Any, worker_id: Any) -> None:
        sk = key_str(key)
        meta = describe_array(key, result)
        now = time.time()
        with self._lock:
            self._running.discard(sk)
            start = self._starts.pop(sk, None)
            if meta is not None and len(self._chunks) < _MAX_BUFFER:
                self._chunks.append(meta)
            if start is not None and len(self._spans) < _MAX_BUFFER:
                self._spans.append(
                    TaskSpan(
                        key=sk,
                        layer=layer_of(key),
                        start=start,
                        end=now,
                        worker=self.worker_label,
                    )
                )

    # -- sampler -------------------------------------------------------------

    def _sample(self) -> None:
        try:
            rss = self._proc.memory_info().rss
        except Exception:  # noqa: BLE001 - degrade to no data, never crash the job
            logger.debug("sample failed", exc_info=True)
            return
        with self._lock:
            executing = sorted(self._running)
            if len(self._samples) < _MAX_BUFFER:
                self._samples.append(
                    MemorySample(
                        timestamp=time.time(),
                        rss_bytes=rss,
                        managed_bytes=0,
                        executing_keys=executing,
                    )
                )

    def _drain(self) -> SampleBatch | None:
        with self._lock:
            if not self._samples and not self._chunks and not self._spans:
                return None
            batch = SampleBatch(
                run_id=self.run_id,
                worker=self.worker_label,
                samples=list(self._samples),
                chunks=list(self._chunks),
                spans=list(self._spans),
            )
            self._samples.clear()
            self._chunks.clear()
            self._spans.clear()
        return batch

    def _flush(self) -> None:
        batch = self._drain()
        if batch is None:
            return
        try:
            report.post_sample_batch(self.collector_url, batch)
        except Exception:  # noqa: BLE001 - collector down must not affect the job
            logger.debug("flush failed", exc_info=True)

    def _run(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.is_set():
            self._sample()
            if time.monotonic() - last_flush >= 1.0:
                self._flush()
                last_flush = time.monotonic()
            self._stop.wait(self.sample_interval)
