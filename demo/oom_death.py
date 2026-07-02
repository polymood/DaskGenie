"""The v1 headline: run a pipeline that OOMs a worker, then print the
post-mortem — which worker died, which task was in flight, the chunk it was
holding, and the source line that produced it.

This wires all four pieces together end to end on a LocalCluster:
GraphCapture (source map) + WorkerPlugin (chunk capture) + SchedulerPlugin
(death attribution) + Collector (the join), with no UI. It is the runnable
form of the spec's step-3 success criterion.

Run: uv run --extra collector --extra demo python demo/oom_death.py
"""

from __future__ import annotations

import socket
import threading
import time

import dask.array as da
import numpy as np
import uvicorn
from distributed import Client, LocalCluster, wait

import daskgenie as dg
import daskgenie.client as genie
from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def blowup(block: np.ndarray) -> float:
    # Hold the input chunk, then allocate far past the worker's memory limit so
    # the nanny's memory monitor terminates the process — a stand-in for the
    # careless broadcast / rechunk that blows up a real pipeline.
    junk = np.ones((16000, 16000), dtype="float64")  # ~2 GB
    return float(np.asarray(block).sum() + junk.sum())


def build_pipeline(client: Client) -> object:
    ones = da.ones((8000, 8000), chunks=(4000, 4000))  # 4 x ~128 MB input chunks
    persisted = client.persist(ones)  # materialize inputs so they can be measured
    wait(persisted)
    return persisted.map_blocks(blowup, dtype="float64").sum()


def print_post_mortem(store: Store, source_map: dict[str, dg.SourceLocation]) -> None:
    deaths = [d for d in store.deaths() if d["suspected_oom"] and d["suspect_keys"]]
    if not deaths:
        print("No suspected-OOM death recorded (worker may have survived).")
        return

    print("\n" + "=" * 78)
    print("POST-MORTEM: which chunk killed which worker, and what code produced it")
    print("=" * 78)
    for d in deaths[:1]:  # the first real OOM
        print(f"\nworker died: {d['worker']}")
        print(f"reason:      {d['reason']}")
        for key in d["suspect_keys"]:
            layer = str(key).strip("()'").split("'")[0].split(",")[0]
            loc = _match_source(layer, source_map)
            src = f"{loc.filename}:{loc.lineno}  {loc.code_snippet}" if loc else "(no source map)"
            print(f"\n  in-flight task: {key}")
            print(f"  source line:    {src}")
            for c in d["suspect_chunks"]:
                if c["task_key"] == key:
                    mb = c["nbytes"] / 1e6
                    print(f"  chunk held:     {tuple(c['shape'])} {c['dtype']} = {mb:.0f} MB")
    print()


def _match_source(layer: str, source_map: dict[str, dg.SourceLocation]) -> dg.SourceLocation | None:
    # suspect keys carry a token like "sum-<hash>"; match it to a captured layer
    for name, loc in source_map.items():
        if name.split("-")[0] in layer or layer in name:
            return loc
    return None


def main() -> None:
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    store = Store(":memory:")
    server = uvicorn.Server(
        uvicorn.Config(create_app(store), host="127.0.0.1", port=port, log_level="error")
    )
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)

    cluster = LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        memory_limit="1500MB",
        dashboard_address=":0",
    )
    client = Client(cluster)
    try:
        genie.register(client, url, sample_interval=0.05, flush_interval=0.1)
        time.sleep(0.5)

        with dg.track() as source_map:
            result = build_pipeline(client)
        genie.upload_graph(url, "demo-run", source_map)

        print("Running a pipeline that will OOM a worker...")
        future = client.compute(result)
        try:
            future.result(timeout=60)
        except Exception as exc:  # noqa: BLE001 - expected KilledWorker
            print(f"job failed as expected: {type(exc).__name__}")

        time.sleep(2)  # let the death event flush to the collector
        print_post_mortem(store, source_map)
    finally:
        client.close()
        cluster.close()
        server.should_exit = True


if __name__ == "__main__":
    main()
