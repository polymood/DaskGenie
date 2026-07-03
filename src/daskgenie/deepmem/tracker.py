"""The deep memory engine: memray driven *as a library*.

memray is normally a CLI that leaves a ``.bin`` capture you post-process. Here
we drive it in-process so the user never sees a file: a background thread runs
memray in **epochs** — a short ``Tracker`` capture to a throwaway temp file,
stopped every ``epoch_seconds``, read back with ``FileReader``, folded to the
high-water-mark bytes per *user* source line, then the temp file is deleted and
the next epoch starts. Rotation keeps each capture tiny and, as a bonus, makes
the attribution time-resolved: each epoch's hot lines are correlated to the
tasks whose spans overlapped that window.

Everything is guarded — memray import failure, tracker errors, unreadable
captures all degrade to "no deep data", never an exception into the worker. Only
one memray ``Tracker`` may be live per process, so a module singleton refuses a
second concurrent tracker.
"""

from __future__ import annotations

import logging
import os
import sysconfig
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

import psutil

from daskgenie.common.schemas import (
    AllocationSite,
    AllocStack,
    MemoryEpoch,
    StackFrame,
    TaskMemory,
    TaskSpan,
)
from daskgenie.graphcapture import is_library_frame

logger = logging.getLogger("daskgenie.deepmem")

# The CPython standard-library directory (e.g. .../python3.12). memray stacks
# routinely pass through threading/queue/asyncio internals; those are no more
# "the user's code" than numpy is, so fold past them too. is_library_frame only
# knows the third-party packages, so we add the stdlib check here.
_STDLIB_DIRS = tuple(
    d for d in {sysconfig.get_paths().get("stdlib"), sysconfig.get_paths().get("platstdlib")} if d
)


def _is_stdlib(filename: str) -> bool:
    return any(filename.startswith(d) for d in _STDLIB_DIRS) and "site-packages" not in filename


# Directory of the daskgenie package, so the deep tracker never blames itself.
_DASKGENIE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Cap sites emitted per epoch so a pathological allocation pattern can't bloat a
# payload; the top lines by bytes are the only ones that matter for an OOM.
_MAX_SITES_PER_EPOCH = 40
# Full call stacks are richer (a whole flamegraph), so cap count + depth harder.
_MAX_STACKS_PER_EPOCH = 60
_MAX_STACK_DEPTH = 48

# One live memray Tracker per process, enforced across all DeepTracker uses.
_process_tracker_lock = threading.Lock()


def _memray_available() -> bool:
    try:
        import memray  # noqa: F401
    except Exception:  # noqa: BLE001 - unsupported platform / not installed
        return False
    return True


def _fold_to_user_line(
    stack: Sequence[tuple[str, str, int]], extra_paths: Sequence[str]
) -> tuple[str, int, str] | None:
    """Walk a memray stack (innermost first) to the first user frame → the line
    that *caused* the allocation, skipping dask/numpy/site-packages internals.
    """
    fallback: tuple[str, int, str] | None = None
    for function, filename, lineno in stack:
        if not filename or filename.startswith("<"):
            continue
        if _is_stdlib(filename) or _DASKGENIE_DIR in filename:
            # stdlib internals and the profiler's own frames (the epoch thread's
            # sleep, etc.) are never the user's allocation site.
            continue
        if fallback is None:
            fallback = (filename, lineno, function)
        if not is_library_frame(filename, extra_paths):
            return (filename, lineno, function)
    return fallback


def _clean_stack(stack: Sequence[tuple[str, str, int]]) -> list[StackFrame]:
    """Turn a memray stack (innermost first) into root->leaf StackFrames for a
    flamegraph. Drops the profiler's own frames and synthetic ``<...>`` frames
    but keeps library frames (numpy/dask) so the tree reads like memray's."""
    frames: list[StackFrame] = []
    for function, filename, lineno in stack:
        if not filename or filename.startswith("<"):
            continue
        if _DASKGENIE_DIR in filename:
            continue
        frames.append(StackFrame(function=function, filename=filename, lineno=lineno))
    frames.reverse()  # memray gives leaf->root; a flamegraph reads root->leaf
    if len(frames) > _MAX_STACK_DEPTH:
        # keep the leaf end (where the allocation is) plus the outermost frame
        frames = [frames[0], *frames[-(_MAX_STACK_DEPTH - 1) :]]
    return frames


class DeepTracker:
    """Epoch-rotating memray tracker. Owned by a worker/local profiler, drained
    into its outbound ``SampleBatch``.
    """

    def __init__(
        self,
        *,
        worker_label: str,
        epoch_seconds: float = 5.0,
        spans_source: Callable[[], list[TaskSpan]] | None = None,
        executing_source: Callable[[], list[tuple[str, str]]] | None = None,
        extra_library_paths: Sequence[str] = (),
    ) -> None:
        self.worker_label = worker_label
        self.epoch_seconds = max(0.5, epoch_seconds)
        self._spans_source = spans_source
        # Returns (key, layer) for tasks executing *right now*. A task that OOMs
        # never closes its span, so closed-span correlation alone would miss the
        # very task that killed the worker — this catches it.
        self._executing_source = executing_source
        self._extra_paths = tuple(extra_library_paths)
        self._proc = psutil.Process()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._epochs: list[MemoryEpoch] = []
        self._task_memory: list[TaskMemory] = []
        self._owns_tracker = False

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        if not _memray_available():
            raise RuntimeError("memray not available")
        if not _process_tracker_lock.acquire(blocking=False):
            raise RuntimeError("another memray Tracker is already active in this process")
        self._owns_tracker = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="daskgenie-deepmem", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.epoch_seconds + 5.0)
        if self._owns_tracker:
            self._owns_tracker = False
            try:
                _process_tracker_lock.release()
            except RuntimeError:
                pass

    def drain(self) -> tuple[list[MemoryEpoch], list[TaskMemory]]:
        with self._lock:
            epochs = self._epochs
            task_mem = self._task_memory
            self._epochs = []
            self._task_memory = []
        return epochs, task_mem

    # -- epoch loop ---------------------------------------------------------

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._one_epoch()
            except Exception:  # noqa: BLE001 - deep profiling must never crash the job
                logger.debug("deep epoch failed", exc_info=True)
                # Back off a beat so a persistent failure doesn't spin hot.
                self._stop.wait(self.epoch_seconds)

    def _one_epoch(self) -> None:
        import memray

        tmp = os.path.join(tempfile.gettempdir(), f"daskgenie-{os.getpid()}-{time.time_ns()}.bin")
        start = time.time()
        try:
            rss_start = self._proc.memory_info().rss
        except Exception:  # noqa: BLE001
            rss_start = 0
        tracker = memray.Tracker(destination=memray.FileDestination(tmp, overwrite=True))
        tracker.__enter__()
        try:
            self._stop.wait(self.epoch_seconds)
        finally:
            tracker.__exit__(None, None, None)
        end = time.time()
        try:
            self._process_capture(tmp, start, end, rss_start)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass

    def _process_capture(self, path: str, start: float, end: float, rss_start: int) -> None:
        import memray

        reader = memray.FileReader(path)
        peak_rss = int(getattr(reader.metadata, "peak_memory", 0) or 0)
        # Aggregate high-water-mark bytes per user source line, and separately
        # per full call stack (for the flamegraph / tree view).
        agg: dict[tuple[str, int, str], list[int]] = {}
        stack_agg: dict[tuple[tuple[str, str, int], ...], list[Any]] = {}
        for rec in reader.get_high_watermark_allocation_records():
            try:
                raw = rec.stack_trace()
                folded = _fold_to_user_line(raw, self._extra_paths)
            except Exception:  # noqa: BLE001 - a bad record shouldn't sink the epoch
                continue
            size = int(rec.size)
            nalloc = int(rec.n_allocations)
            if folded is not None:
                slot = agg.setdefault(folded, [0, 0])
                slot[0] += size
                slot[1] += nalloc
            cleaned = _clean_stack(raw)
            if cleaned:
                skey = tuple((f.function, f.filename, f.lineno) for f in cleaned)
                sslot = stack_agg.setdefault(skey, [0, 0, cleaned])
                sslot[0] += size
                sslot[1] += nalloc

        if not agg and not stack_agg:
            return

        stacks = [
            AllocStack(frames=frames, hwm_bytes=hwm, n_allocations=nalloc)
            for (hwm, nalloc, frames) in sorted(
                stack_agg.values(), key=lambda v: v[0], reverse=True
            )[:_MAX_STACKS_PER_EPOCH]
        ]

        keys, _ = self._tasks_in_window(start, end)
        # Attribute this epoch's allocations to the task that dominated the
        # window (most overlap), not only when a single task ran — otherwise
        # short concurrent tasks leave every epoch "unattributed".
        dom_key, dom_layer = self._dominant_in_window(start, end)

        sites = [
            AllocationSite(
                filename=fn,
                lineno=ln,
                function=fun,
                hwm_bytes=hwm,
                n_allocations=nalloc,
                task_key=dom_key,
                layer=dom_layer,
            )
            for (fn, ln, fun), (hwm, nalloc) in sorted(
                agg.items(), key=lambda kv: kv[1][0], reverse=True
            )[:_MAX_SITES_PER_EPOCH]
        ]
        epoch = MemoryEpoch(
            worker=self.worker_label,
            start=start,
            end=end,
            peak_rss=peak_rss,
            sites=sites,
            stacks=stacks,
        )
        # Attribute this epoch's dominant lines to each task that ran in it. The
        # RSS delta is the epoch's growth — shared across concurrent tasks, so
        # it's an upper bound per task, not an exact split (documented).
        delta = max(0, peak_rss - rss_start)
        top = sites[:5]
        task_mem = [
            TaskMemory(
                key=key,
                layer=self._layer_of_key(key, start, end),
                worker=self.worker_label,
                peak_rss_delta=delta,
                top_sites=top,
            )
            for key in keys
        ]
        with self._lock:
            self._epochs.append(epoch)
            self._task_memory.extend(task_mem)

    # -- span correlation ---------------------------------------------------

    def _spans_in_window(self, start: float, end: float) -> list[TaskSpan]:
        if self._spans_source is None:
            return []
        try:
            spans = self._spans_source()
        except Exception:  # noqa: BLE001
            return []
        return [s for s in spans if s.start <= end and s.end >= start]

    def _executing_now(self) -> list[tuple[str, str]]:
        if self._executing_source is None:
            return []
        try:
            return list(self._executing_source())
        except Exception:  # noqa: BLE001
            return []

    def _tasks_in_window(self, start: float, end: float) -> tuple[list[str], list[str]]:
        pairs = {(s.key, s.layer) for s in self._spans_in_window(start, end)}
        pairs |= set(self._executing_now())  # the still-running (maybe-dying) task
        keys = sorted({k for k, _ in pairs})
        layers = sorted({ly for _, ly in pairs if ly})
        return keys, layers

    def _dominant_in_window(self, start: float, end: float) -> tuple[str, str]:
        """The (key, layer) with the most overlap in [start, end]. A task still
        executing at epoch end is weighted by the full window — it's the live
        allocator, the one most likely responsible for the high-water mark.
        """
        overlap: dict[tuple[str, str], float] = {}
        for s in self._spans_in_window(start, end):
            ov = min(s.end, end) - max(s.start, start)
            if ov > 0:
                overlap[(s.key, s.layer)] = overlap.get((s.key, s.layer), 0.0) + ov
        span = max(end - start, 1e-6)
        for k, ly in self._executing_now():
            overlap[(k, ly)] = overlap.get((k, ly), 0.0) + span
        if not overlap:
            return ("", "")
        (key, layer), _ = max(overlap.items(), key=lambda kv: kv[1])
        return (key, layer)

    def _layer_of_key(self, key: str, start: float, end: float) -> str:
        for s in self._spans_in_window(start, end):
            if s.key == key:
                return s.layer
        for k, ly in self._executing_now():
            if k == key:
                return ly
        return ""
