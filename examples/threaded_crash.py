"""A threaded-scheduler pipeline that ACTUALLY CRASHES from memory exhaustion.

Unlike ``threaded_big.py`` (which hoards a bounded amount to show the staircase),
this one grows resident memory **without bound** until the process dies — a real
``MemoryError`` (or an OS OOM-kill). It's the "my script just died and I don't
know why" scenario: DaskGenie's memory-over-time chart shows the climb right up
to the ceiling, and the deep Memory tab names the line doing the allocating.

This is *self-limiting*: it grows resident memory to a ceiling (default 55% of
this machine's RAM), then attempts one allocation larger than the memory that's
left — which raises a real ``MemoryError`` and crashes the Python process,
WITHOUT dragging the whole machine (or WSL) down with it. That's the honest "my
job died of memory" event, safely.

Run (start the stack first with ``docker compose up -d``):

    uv run --extra demo --extra deep python examples/threaded_crash.py

Because the sampler flushes every second, the memory curve and the deep
allocation line are already in the collector by the time the process dies — open
the run and you'll see exactly how big it got and which line got it there.

Tunables (env): DG_STEP_MB (per-step MiB, default 1024), DG_N/DG_CHUNK (tile),
DG_MAX_GB (hoard ceiling; default ~55% of RAM), DG_THREADS.
"""

from __future__ import annotations

import os

import numpy as np
import psutil

import daskgenie as dg

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
STEP_MB = int(os.environ.get("DG_STEP_MB", "1024"))  # ~1 GiB added per step
N = int(os.environ.get("DG_N", "6000"))
CHUNK = int(os.environ.get("DG_CHUNK", "1000"))
THREADS = int(os.environ.get("DG_THREADS", "4"))
# Ceiling for the hoard, defaulting to a safe fraction of physical RAM so we
# crash *this process*, not the machine. Override with DG_MAX_GB.
_DEFAULT_MAX_GB = round(psutil.virtual_memory().total * 0.55 / 1e9, 1)
MAX_GB = float(os.environ.get("DG_MAX_GB", str(_DEFAULT_MAX_GB)))


def heavy_block(block: np.ndarray) -> np.ndarray:
    # real per-tile compute so this isn't just np.ones — normalise + smooth
    b = (block - block.mean()) / (block.std() + 1e-6)
    return np.tanh(b).astype(np.float32)


def main() -> None:
    import dask.array as da

    # one float64 array of this many elements ~ STEP_MB per step
    side = int((STEP_MB * 1024 * 1024 / 8) ** 0.5)
    leak: list[np.ndarray] = []  # never freed → the runaway growth

    print(f"threaded scheduler, {THREADS} threads — growing ~{STEP_MB} MiB/step")
    print(f"will crash (MemoryError) once it holds ~{MAX_GB} GB (DG_MAX_GB to change)\n")

    with dg.LocalProfiler(
        COLLECTOR, run_name="threaded CRASH (OOM)", deep=True, deep_epoch_seconds=1.0
    ) as prof:
        print(f"run_id = {prof.run_id}  — open the dashboard and watch it climb to the wall\n")
        step = 0
        while True:
            step += 1
            with dg.track():
                scene = da.random.random((N, N), chunks=(CHUNK, CHUNK)).astype(np.float32)
                _ = (
                    scene.map_blocks(heavy_block, dtype=np.float32)
                    .sum()
                    .compute(scheduler="threads", num_workers=THREADS)
                )
            # THE LEAK: a big block that never leaves memory. This is the line the
            # deep Memory tab will blame for the runaway growth.
            leak.append(np.ones((side, side), dtype=np.float64))
            held_gb = sum(a.nbytes for a in leak) / 1e9
            print(f"  step {step:3d}  holding {held_gb:6.2f} GB resident", flush=True)

            if held_gb >= MAX_GB:
                # Tip over the edge: ask for more than the RAM that's left. numpy
                # raises MemoryError immediately — a real crash, machine intact.
                avail = psutil.virtual_memory().available
                boom = int((avail * 1.5) ** 0.5) + 1
                print(f"  ceiling reached — allocating past the wall ({avail / 1e9:.1f} GB free)")
                _ = np.ones((boom, boom), dtype=np.float64)  # -> MemoryError, crashes here


if __name__ == "__main__":
    main()
