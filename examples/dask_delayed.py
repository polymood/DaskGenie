"""Profile a **dask.delayed** pipeline on a distributed cluster.

``@delayed`` builds a task graph out of ordinary Python functions — the classic
"custom ETL DAG" use of Dask. DaskGenie captures the graph, per-task memory, and
(with ``deep=True``) the source line behind each allocation, exactly as it does
for arrays.

Start the stack (``docker compose up -d``), then:

    uv run --extra examples --extra deep python examples/dask_delayed.py

Open the run: the **Graph** tab shows the delayed DAG (load → transform →
combine), **Memory** attributes bytes to ``transform_shard``.
"""

from __future__ import annotations

import os

import numpy as np
from dask import delayed
from distributed import Client, LocalCluster

import daskgenie as dg
import daskgenie.client as genie

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
SHARDS = int(os.environ.get("DG_SHARDS", "24"))
SIZE = int(os.environ.get("DG_SIZE", "2000"))


@delayed
def load_shard(i: int) -> np.ndarray:
    # stand-in for reading shard i off disk / object store
    rng = np.random.default_rng(i)
    return rng.random((SIZE, SIZE), dtype=np.float64)


@delayed
def transform_shard(a: np.ndarray) -> np.ndarray:
    # per-shard feature transform — where the memory goes
    return np.tanh(a @ a.T) / (a.std() + 1e-9)


@delayed
def reduce_shard(a: np.ndarray) -> float:
    return float(a.sum())


@delayed
def combine(parts: list[float]) -> float:
    return float(np.sum(parts))


def main() -> None:
    cluster = LocalCluster(
        n_workers=4, threads_per_worker=1, processes=True, dashboard_address=":0"
    )
    client = Client(cluster)
    try:
        run_id = genie.register(client, COLLECTOR, run_name="dask.delayed ETL", deep=True)

        with dg.track() as source_map:
            parts = [reduce_shard(transform_shard(load_shard(i))) for i in range(SHARDS)]
            total = combine(parts)
        dg.upload_graph(COLLECTOR, run_id, source_map, collection=total)

        print(f"run {run_id}: computing {SHARDS} delayed shards...")
        result = total.compute()
        print(f"done — result={result:.3e}; open the dashboard run {run_id!r}")
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
