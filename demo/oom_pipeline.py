"""Step 1 deliverable from the spec: prove GraphCapture's source attribution
works end to end on a real pipeline, with no UI.

The pipeline mirrors a typical ESA-style xarray-on-Zarr workload: open a
chunked dataset, rechunk it into a few large chunks (the classic memory-
pressure trigger), then map a per-block function over it. It is shaped to
run out of memory at production scale — building and capturing the graph
does not require actually running it. Executing it against a real cluster
and watching a worker die is Step 2/3's job (WorkerPlugin + SchedulerPlugin).

Run: uv run python demo/oom_pipeline.py
"""

from __future__ import annotations

import dask.array as da

import daskgenie as dg


def open_dataset() -> da.Array:
    # Stand-in for xr.open_dataset(path, chunks=...).data: a modest,
    # chunked array as it would appear right after reading from Zarr.
    return da.random.random((8000, 8000), chunks=(500, 500))


def rechunk_merge(arr: da.Array) -> da.Array:
    # Merges hundreds of small chunks into a handful of huge ones. This is
    # the #1 real-world OOM trigger in ESA pipelines: rechunking across the
    # grain the data was written in forces the scheduler to hold many input
    # chunks in memory at once to build each output chunk.
    return arr.rechunk((8000, 8000))


def double_precision_blowup(block: object) -> object:
    # Per-block step that materializes an outer product of each block with
    # itself: an O(n^2) memory blowup per chunk, same shape of bug as a
    # careless broadcast in a real pipeline.
    import numpy as np

    b = np.asarray(block)
    return (b[:, :, None] * b[:, None, :]).sum(axis=-1)


def build_pipeline() -> da.Array:
    x = open_dataset()
    y = rechunk_merge(x)
    z = y.map_blocks(double_precision_blowup, dtype="float64")
    return z


def print_source_map(layer_map: dict[str, dg.SourceLocation]) -> None:
    print(f"{'layer':<45} source")
    print("-" * 100)
    for name, loc in sorted(layer_map.items(), key=lambda kv: kv[1].lineno):
        print(f"{name:<45} {loc.filename}:{loc.lineno}  {loc.code_snippet}")


def main() -> None:
    with dg.track() as layer_map:
        build_pipeline()

    print_source_map(layer_map)


if __name__ == "__main__":
    main()
