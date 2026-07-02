"""Profile a computation on a *local* Dask scheduler — no distributed cluster.

DaskGenie is not distributed-only: `LocalProfiler` hooks Dask's callback API,
so it works with ``scheduler="threads"``, ``"synchronous"``, or ``"processes"``
— the default path for bare dask arrays/dataframes. You get the memory-over-time
chart, per-task output chunk sizes, and the source map, all in the dashboard.

Start the stack first (``docker compose up -d`` — collector on :8765, dashboard
on :3000), then:

    uv run --extra demo python examples/local_scheduler.py

Open http://localhost:3000 and pick the run it prints.
"""

from __future__ import annotations

import dask.array as da

import daskgenie as dg

COLLECTOR = "http://localhost:8765"
SCHEDULER = "threads"  # try "synchronous" or "processes" too


def main() -> None:
    with dg.track() as source_map:
        x = da.random.random((10000, 10000), chunks=(1000, 1000))
        result = (x @ x.T).mean()

    with dg.LocalProfiler(
        COLLECTOR,
        run_name=f"{SCHEDULER} matmul",
        source_map=source_map,
        collection=result,
        sample_interval=0.05,
    ) as prof:
        value = result.compute(scheduler=SCHEDULER)

    print(f"result = {value:.4f}")
    print(f"done — open the dashboard and find run {prof.run_id!r}")


if __name__ == "__main__":
    main()
