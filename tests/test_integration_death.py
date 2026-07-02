"""The v1 success criterion: run a job that genuinely OOMs a worker, then read
back from the collector which task was in-flight and what chunk it was holding.

This exercises the whole loop end to end — worker plugin (chunk capture) +
scheduler plugin (death attribution) + collector (the join) — against a real
multi-process cluster whose worker is actually killed by the nanny's memory
monitor. It is slow and memory-hungry, hence the ``integration`` marker.
"""

from __future__ import annotations

# Guarded imports below need importorskip first, so E402 is expected here.
# ruff: noqa: E402
import socket
import threading
import time

import pytest

uvicorn = pytest.importorskip("uvicorn")
np = pytest.importorskip("numpy")
distributed = pytest.importorskip("distributed")

from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _ServerThread:
    def __init__(self, app: object, port: int) -> None:
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> _ServerThread:
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                return self
            time.sleep(0.05)
        raise RuntimeError("collector did not start")

    def __exit__(self, *exc: object) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def _blowup(block: object) -> float:
    # Hold the (already sizeable) input, then allocate far past the worker's
    # memory limit so the nanny's memory monitor terminates the process.
    import numpy as np

    junk = np.ones((16000, 16000), dtype="float64")  # ~2 GB
    return float(np.asarray(block).sum() + junk.sum())


@pytest.mark.integration
def test_worker_oom_names_suspect_task_and_chunk() -> None:
    import dask.array as da
    from distributed import Client, LocalCluster, wait

    store = Store(":memory:")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    with _ServerThread(create_app(store), port):
        with (
            LocalCluster(
                n_workers=1,
                threads_per_worker=1,
                processes=True,
                memory_limit="1500MB",
                dashboard_address=":0",
            ) as cluster,
            Client(cluster) as client,
        ):
            import daskgenie.client as genie

            # fast flush so chunk metadata escapes before the worker dies
            run_id = genie.register(client, url, sample_interval=0.05, flush_interval=0.1)
            time.sleep(0.5)

            # Persist the input first so the OOMing task has a *materialized*
            # input dependency in worker.data for the plugin to describe. Without
            # this, ones->map_blocks->sum fully fuses into one dependency-free
            # task and the killer allocation is internal (nothing to record).
            x = client.persist(da.ones((8000, 8000), chunks=(4000, 4000)))  # 4 x ~128 MB
            wait(x)
            future = client.compute(x.map_blocks(_blowup, dtype="float64").sum())
            with pytest.raises(Exception):  # noqa: B017,PT011 - KilledWorker or cancellation
                future.result(timeout=60)

        # a suspected-OOM death must have been recorded naming an in-flight task
        deaths = _poll(store, run_id, timeout=15)
        assert deaths, "no death event reached the collector"
        oom = [d for d in deaths if d["suspected_oom"] and d["suspect_keys"]]
        assert oom, f"no suspected-OOM death named a suspect task: {deaths}"

        # and the "which chunk killed it" join: at least one such death should
        # carry the chunk metadata the worker captured for its suspect task
        assert any(d["suspect_chunks"] for d in oom), (
            f"no death carried chunk metadata: {[d['suspect_keys'] for d in oom]}"
        )


def _poll(store: Store, run_id: str, timeout: float) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        deaths = store.deaths(run_id)
        if deaths:
            return deaths
        time.sleep(0.5)
    return store.deaths(run_id)
