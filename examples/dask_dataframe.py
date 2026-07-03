"""Profile a **dask.dataframe** pipeline on a distributed cluster.

Uses Dask's built-in synthetic timeseries (no external data), then does the
memory-heavy things dataframes are known for: a groupby-aggregation and a
shuffle-y ``set_index``. DaskGenie shows the partition memory over time and the
per-layer allocation timeline (groupby vs shuffle).

Start the stack (``docker compose up -d``), then:

    uv run --extra examples --extra deep python examples/dask_dataframe.py
"""

from __future__ import annotations

import os

from distributed import Client, LocalCluster

import daskgenie as dg
import daskgenie.client as genie

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
DAYS = int(os.environ.get("DG_DAYS", "120"))  # ~ size of the synthetic frame


def main() -> None:
    import dask.dataframe as dd
    import dask.datasets

    cluster = LocalCluster(
        n_workers=4, threads_per_worker=2, processes=True, dashboard_address=":0"
    )
    client = Client(cluster)
    try:
        run_id = genie.register(client, COLLECTOR, run_name="dask.dataframe groupby", deep=True)

        with dg.track() as source_map:
            df = dask.datasets.timeseries(
                start="2020-01-01",
                end=f"2020-{1 + DAYS // 30:02d}-01",
                freq="100ms",
                partition_freq="1d",
                dtypes={"x": float, "y": float, "id": int},
            )
            df = df.assign(z=(df.x * df.y).abs())
            # groupby-aggregation (hash aggregation memory)
            agg = df.groupby("id").agg({"x": "mean", "y": "std", "z": "sum"})
            # a rolling window over a set_index (shuffle) branch
            rolled = df.set_index("id").z.rolling(50).mean()
            result = agg.z.sum() + dd.to_numeric(rolled.fillna(0)).sum()
        dg.upload_graph(COLLECTOR, run_id, source_map, collection=result)

        print(f"run {run_id}: computing groupby + rolling over the frame...")
        val = result.compute()
        print(f"done — result={float(val):.3e}; open the dashboard run {run_id!r}")
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
