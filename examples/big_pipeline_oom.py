"""A realistic, minutes-long Dask pipeline that builds a large task graph, does
genuine heavy per-block compute, and then dies from a real out-of-memory on a
worker — the full DaskGenie story end to end.

It mimics an image/geospatial processing pipeline over a big float32 field:

  1. ingest        random scene, chunked                (big base graph)
  2. normalise     elementwise (x - mean) / std          (per-block)
  3. smooth        overlapping stencil (map_overlap)      (halo exchange → edges)
  4. spectral      per-block FFT magnitude                (CPU heavy)
  5. refine ×N     matmul-based diffusion + rechunk       (burns minutes, huge graph)
  6. upsample      naive super-resolution (np.kron)       (THE OOM — one block
                   explodes to a multi-GB float64 array, past the worker limit)

Stages 1-5 run for a couple of minutes and stream live to the dashboard
(Workers / Task stream / Memory update in real time). Stage 6 then OOM-kills a
worker on a single, nameable line, so the Post-mortem and the deep Memory tab
point straight at ``big_pipeline_oom.py:<line>  upsample_block``.

Start the stack first (``docker compose up -d``), then:

    uv run --extra demo --extra deep python examples/big_pipeline_oom.py

Tunables (env): DG_N (field size), DG_CHUNK, DG_ITERS, DG_WORKERS, DG_MEMLIMIT.
Defaults run ~2-4 min and reliably OOM with a 1900MB worker limit.
"""

from __future__ import annotations

import os
import time

import numpy as np
from distributed import Client, LocalCluster, wait

import daskgenie as dg
import daskgenie.client as genie

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
N = int(os.environ.get("DG_N", "14000"))
CHUNK = int(os.environ.get("DG_CHUNK", "1400"))
ITERS = int(os.environ.get("DG_ITERS", "3"))
REPEAT = int(os.environ.get("DG_REPEAT", "8"))  # inner matmuls per diffuse — runtime knob
WORKERS = int(os.environ.get("DG_WORKERS", "4"))
# Headroom so the refine loop runs clean; the upsample is the intended OOM.
MEMLIMIT = os.environ.get("DG_MEMLIMIT", "2600MB")
UPSAMPLE = int(os.environ.get("DG_UPSAMPLE", "11"))  # Kron factor → ~1.9 GB per tile


# -- per-block compute kernels (real work, each a nameable source line) --------


def normalise_block(block: np.ndarray) -> np.ndarray:
    # standardise the tile; cheap, elementwise
    mu = block.mean()
    sd = block.std() + 1e-6
    return ((block - mu) / sd).astype(np.float32)


def spectral_block(block: np.ndarray) -> np.ndarray:
    # 2-D FFT magnitude — genuinely CPU-heavy, keeps workers busy
    spec = np.fft.rfft2(block)
    mag = np.abs(spec).astype(np.float32)
    # pad back to the block's shape so the graph stays rectangular
    out = np.zeros_like(block)
    out[:, : mag.shape[1]] = mag[:, : out.shape[1]]
    return out


def diffuse_block(block: np.ndarray) -> np.ndarray:
    # matmul-based smoothing: an (n x n) gaussian kernel applied REPEAT times.
    # O(n^3) per pass — the main time sink of the refine loop. Single-sided so it
    # stays valid for non-square edge tiles after a rechunk; REPEAT (not more
    # tasks) is the runtime knob, keeping the graph a readable size.
    n = block.shape[0]
    kernel = np.exp(-((np.arange(n)[:, None] - np.arange(n)[None, :]) ** 2) / (2 * 64.0))
    kernel = (kernel / kernel.sum(axis=1, keepdims=True)).astype(np.float32)
    out = block
    for _ in range(REPEAT):
        out = (kernel @ out).astype(np.float32)
    return out


def upsample_block(block: np.ndarray) -> np.ndarray:
    # Naive super-resolution: Kronecker-upsample each tile. For a 1400x1400 tile
    # at factor 11 this materialises a 15400x15400 float64 array (~1.9 GB) on ONE
    # task — the careless allocation the deep Memory tab and post-mortem name.
    hires = np.kron(block.astype(np.float64), np.ones((UPSAMPLE, UPSAMPLE)))  # ~1.9 GB
    # Hold + touch the array for ~1.2s of real work so a deep-memory epoch closes
    # and flushes this multi-GB high-water mark *before* the fatal second buffer
    # below tips the worker over its limit (a hard OOM kill is faster than an
    # epoch, so without this the killing line dies unrecorded).
    t0 = time.time()
    acc = 0.0
    while time.time() - t0 < 1.2:
        acc += float(hires.sum())
    # now blow past the limit: a second equally-huge buffer forces the OOM
    killer = np.kron(block.astype(np.float64), np.ones((UPSAMPLE, UPSAMPLE)))  # +1.9 GB → OOM
    return ((hires + killer)[::UPSAMPLE, ::UPSAMPLE] + acc * 0.0).astype(np.float32)


def main() -> None:
    import dask.array as da

    cluster = LocalCluster(
        n_workers=WORKERS,
        threads_per_worker=1,
        processes=True,
        memory_limit=MEMLIMIT,
        dashboard_address=":0",
    )
    client = Client(cluster)
    print(f"cluster: {WORKERS} workers x {MEMLIMIT}; field {N}x{N} float32, chunk {CHUNK}")

    try:
        run_id = genie.register(
            client,
            COLLECTOR,
            run_name=f"big pipeline {N}x{N} (OOM)",
            flush_interval=0.25,
            deep=True,
            deep_epoch_seconds=1.0,
        )
        print(f"run_id = {run_id}  — open the dashboard now and watch it live")

        with dg.track() as source_map:
            # 1. ingest — a big chunked float32 field
            x = da.random.random((N, N), chunks=(CHUNK, CHUNK)).astype(np.float32)

            # 2. normalise (elementwise, per-block)
            x = x.map_blocks(normalise_block, dtype=np.float32)

            # 3. smooth — overlapping stencil, adds halo-exchange edges to the graph
            x = x.map_overlap(
                lambda b: (b + np.roll(b, 1, 0) + np.roll(b, -1, 0)) / 3.0,
                depth=1,
                boundary="reflect",
                dtype=np.float32,
            )

            # 4. spectral features (CPU heavy)
            x = x.map_blocks(spectral_block, dtype=np.float32)

            # 5. refine loop — matmul diffusion + a rechunk each pass. This is
            #    where the minutes go and the graph balloons.
            for i in range(ITERS):
                x = x.map_blocks(diffuse_block, dtype=np.float32)
                # alternate the chunking so successive passes must reshuffle
                new_chunk = CHUNK if i % 2 == 0 else CHUNK + 500
                x = x.rechunk((new_chunk, new_chunk))
                x = (x - x.mean()) / (x.std() + 1e-6)

        # Upload the FULL refined-field graph (stages 1-5, thousands of tasks)
        # so the Graph tab shows the real multi-stage DAG — not just the small
        # post-persist tail. The upsample line is added to the source map below
        # so the Memory/Post-mortem tabs can still name it.
        with dg.track() as source_map2:
            # 6. the OOM: naive super-resolution upsample
            result = x.map_blocks(upsample_block, dtype=np.float32).sum()
        source_map.update(source_map2)
        dg.upload_graph(COLLECTOR, run_id, source_map, collection=x)

        # Persist the refined field so stages 1-5 actually run (and stream) before
        # the OOM — this is the "couple of minutes of real work" part.
        print("running stages 1-5 (this takes a few minutes)...")
        t0 = time.time()
        x = client.persist(x)
        wait(x)
        print(f"refined field ready in {time.time() - t0:.0f}s; now the upsample OOM...")

        # rebuild the OOM step on the now-persisted field
        result = x.map_blocks(upsample_block, dtype=np.float32).sum()

        try:
            client.compute(result).result(timeout=180)
            print("no OOM — try a smaller DG_MEMLIMIT or larger DG_CHUNK")
        except Exception as exc:  # noqa: BLE001 - expected KilledWorker
            print(f"worker OOM-killed as expected: {type(exc).__name__}")

        print(
            f"\ndone — run {run_id!r}:\n"
            "  • Memory tab   → upsample_block line at multi-GB peak\n"
            "  • Post-mortem  → the suspect task, chunk, and (timing permitting) the line\n"
            "  • Graph tab    → the full multi-thousand-node task DAG"
        )
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
