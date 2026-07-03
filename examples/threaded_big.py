"""A big **threaded-scheduler** pipeline that shows the other failure mode: not a
single monster allocation, but *holding too much in memory that should have been
spilled to disk (Zarr)*.

There are no distributed workers here — everything runs in one process on
``scheduler="threads"``, profiled by ``LocalProfiler`` (the Dask callback API).
The pipeline processes a stack of large scenes and — the bug — keeps **every**
processed scene resident in a Python list instead of streaming each to a Zarr
store. Memory climbs step by step; DaskGenie's deep Memory tab attributes the
growth to the exact accumulation line, and the memory-over-time chart shows the
staircase that a to-zarr write would have kept flat.

Run (start the stack first with ``docker compose up -d``):

    uv run --extra demo --extra deep python examples/threaded_big.py

Tunables (env): DG_N (scene size), DG_CHUNK, DG_STEPS (how many scenes to hoard),
DG_THREADS. Defaults hold ~4-5 GB and run ~2 min. Lower DG_STEPS if your machine
has less RAM — the point is the *shape* of the memory curve, not a hard crash.
"""

from __future__ import annotations

import os
import time

import numpy as np

import daskgenie as dg

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
N = int(os.environ.get("DG_N", "8000"))
CHUNK = int(os.environ.get("DG_CHUNK", "1000"))
STEPS = int(os.environ.get("DG_STEPS", "18"))
THREADS = int(os.environ.get("DG_THREADS", "4"))


def process_scene(block: np.ndarray) -> np.ndarray:
    # genuine per-tile work: normalise, FFT magnitude, a matmul smooth
    n = block.shape[0]
    b = (block - block.mean()) / (block.std() + 1e-6)
    spec = np.abs(np.fft.rfft2(b)).astype(np.float32)
    out = np.zeros_like(b, dtype=np.float32)
    out[:, : spec.shape[1]] = spec[:, : out.shape[1]]
    kernel = np.exp(-((np.arange(n)[:, None] - np.arange(n)[None, :]) ** 2) / (2 * 64.0))
    kernel = (kernel / kernel.sum(axis=1, keepdims=True)).astype(np.float32)
    return (kernel @ out).astype(np.float32)


def main() -> None:
    import dask.array as da

    print(f"threaded scheduler: {THREADS} threads; {STEPS} scenes of {N}x{N} float32")
    print("BUG on purpose: every processed scene is kept resident instead of spilled to Zarr\n")

    hoarded: list[np.ndarray] = []  # <-- the leak: everything stays in memory

    with dg.LocalProfiler(
        COLLECTOR,
        run_name=f"threaded hoard {STEPS}x{N}",
        deep=True,
        deep_epoch_seconds=1.5,
    ) as prof:
        print(f"run_id = {prof.run_id}  — open the dashboard and watch memory climb\n")
        for step in range(STEPS):
            with dg.track():
                scene = da.random.random((N, N), chunks=(CHUNK, CHUNK)).astype(np.float32)
                processed = scene.map_blocks(process_scene, dtype=np.float32)
                # RIGHT way (kept for contrast): stream straight to disk, stay flat
                #   processed.to_zarr(f"/tmp/scenes/{step}.zarr", overwrite=True)
                # WRONG way (what this script does): materialise and hoard it
                result = processed.compute(scheduler="threads", num_workers=THREADS)
            hoarded.append(result)  # the line that holds ~256 MB per step forever
            held_gb = sum(a.nbytes for a in hoarded) / 1e9
            print(f"  step {step + 1:2d}/{STEPS}  holding {held_gb:5.2f} GB resident")
            time.sleep(0.2)

    print(
        f"\ndone — run {prof.run_id!r}. Memory tab: the hoard line at the top of the\n"
        "per-source-line table; Overview: a rising staircase instead of a flat line —\n"
        "the signature of data that should have gone to a Zarr store, not a Python list."
    )


if __name__ == "__main__":
    main()
