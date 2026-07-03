"""Deep memory profiling end to end: run a pipeline that allocates a huge array
on a line we can name, with the memray engine on, then read the *source line*
that dominated memory — not just "a chunk was resident".

Start the stack first (``docker compose up -d``), then:

    uv run --extra demo --extra deep python examples/deep_oom.py

Open http://localhost:3000, pick the run, and watch it live:

- **Workers** / **Task stream** update in real time as the job runs.
- **Memory** shows the allocation flamegraph and the per-source-line table —
  ``deep_oom.py:<line>  build_monster  ~2 GB`` at the top.
- **Post-mortem** (if the worker is OOM-killed) names that allocation line as
  the cause, above the input-chunk view.
"""

from __future__ import annotations

import numpy as np
from distributed import Client, LocalCluster, wait

import daskgenie as dg
import daskgenie.client as genie

COLLECTOR = "http://localhost:8765"


def build_monster(block: np.ndarray) -> float:
    # This is the line that eats the memory — the deep engine attributes the
    # high-water mark straight back here (file:line), then to this task/layer.
    monster = np.ones((16000, 16000), dtype="float64")  # ~2 GB, on THIS line
    return float(np.asarray(block).sum() + monster.sum())


def main() -> None:
    import dask.array as da

    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True, memory_limit="1500MB")
    client = Client(cluster)
    try:
        # deep=True installs the memray engine on each worker (needs the `deep`
        # extra). deep_epoch_seconds bounds how quickly per-line data streams in.
        run_id = genie.register(
            client,
            COLLECTOR,
            run_name="deep OOM (memray)",
            flush_interval=0.25,
            deep=True,
            deep_epoch_seconds=2.0,
        )

        with dg.track() as source_map:
            x = client.persist(da.ones((8000, 8000), chunks=(4000, 4000)))
            wait(x)
            result = x.map_blocks(build_monster, dtype="float64").sum()
        dg.upload_graph(COLLECTOR, run_id, source_map, collection=result)

        print(f"running with deep memory profiling — run {run_id!r}")
        try:
            client.compute(result).result(timeout=90)
        except Exception as exc:  # noqa: BLE001 - expected KilledWorker on OOM
            print(f"worker died as expected: {type(exc).__name__}")

        print(f"done — open the dashboard, run {run_id!r}, Memory + Post-mortem tabs")
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
