"""Kill a worker with an OOM on a real distributed cluster, then read the
post-mortem in the dashboard: which task was in flight, the chunk it was
holding, and the source line that produced it.

Start the stack first (``docker compose up -d``), then:

    uv run --extra demo python examples/distributed_oom.py

Open http://localhost:3000, pick the run, and go to the Post-mortem tab.
"""

from __future__ import annotations

import numpy as np
from distributed import Client, LocalCluster, wait

import daskgenie as dg
import daskgenie.client as genie

COLLECTOR = "http://localhost:8765"


def blowup(block: np.ndarray) -> float:
    # Hold the input chunk, then allocate far past the worker's memory limit so
    # the nanny's memory monitor kills the process — a stand-in for the careless
    # broadcast / rechunk that blows up a real pipeline.
    junk = np.ones((16000, 16000), dtype="float64")  # ~2 GB
    return float(np.asarray(block).sum() + junk.sum())


def main() -> None:
    import dask.array as da

    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1, processes=True, memory_limit="1500MB"
    )
    client = Client(cluster)
    try:
        run_id = genie.register(client, COLLECTOR, run_name="rechunk OOM", flush_interval=0.1)

        # Persist the input so the OOMing task has a materialized chunk to measure.
        with dg.track() as source_map:
            x = client.persist(da.ones((8000, 8000), chunks=(4000, 4000)))
            wait(x)
            result = x.map_blocks(blowup, dtype="float64").sum()
        dg.upload_graph(COLLECTOR, run_id, source_map, collection=result)

        print("running a pipeline that will OOM a worker...")
        try:
            client.compute(result).result(timeout=60)
        except Exception as exc:  # noqa: BLE001 - expected KilledWorker
            print(f"worker died as expected: {type(exc).__name__}")

        print(f"done — open the dashboard, run {run_id!r}, Post-mortem tab")
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
