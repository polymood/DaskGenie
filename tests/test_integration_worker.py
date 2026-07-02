"""End-to-end: a real LocalCluster runs a memory-heavy job with the profiler
plugin installed, and we assert samples + chunk metadata reach a live collector.

This is the spec's "verify against a deliberately OOM-ing job" for step 2. We
run a heavy (not fatally OOM) job so the test runner survives; the true
worker-death path is exercised in the scheduler-plugin tests (step 3).
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


@pytest.mark.integration
def test_worker_plugin_reports_samples_and_chunks() -> None:
    import dask.array as da
    from distributed import Client, LocalCluster

    import daskgenie.client as dg_client

    store = Store(":memory:")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    with _ServerThread(create_app(store), port):
        with (
            LocalCluster(
                n_workers=1, threads_per_worker=2, processes=True, dashboard_address=":0"
            ) as cluster,
            Client(cluster) as client,
        ):
            dg_client.register(client, url, sample_interval=0.05)
            time.sleep(0.5)  # let the plugin install on the worker

            x = da.random.random((4000, 4000), chunks=(500, 500))
            y = (x @ x.T).sum()
            y.compute()

            # give the plugin a flush cycle to push the tail
            time.sleep(2.5)

        # samples must have landed, tagged to the worker, with non-zero RSS
        timeline = store.timeline()
        assert timeline, "no samples reached the collector"
        assert any(row["rss_bytes"] > 0 for row in timeline)

        # at least one materialized input chunk must have been described
        with store._lock:  # noqa: SLF001 - test inspects the store directly
            chunk_count = store._conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        assert chunk_count > 0, "no chunk metadata captured"
