"""End-to-end: a real (processes=True) LocalCluster runs a job with the memray
deep engine on, and we assert the per-source-line allocation attribution reaches
the collector — the "which line allocated the big array" headline, on the real
distributed worker path.
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
pytest.importorskip("memray")

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


def _big_alloc(block):  # type: ignore[no-untyped-def]
    junk = np.ones((3000, 3000), dtype="float64")  # ~72 MB on THIS line
    time.sleep(0.15)
    return block + junk.sum() * 0.0


@pytest.mark.integration
def test_deep_engine_attributes_allocation_to_source_line() -> None:
    import dask.array as da
    from distributed import Client, LocalCluster

    import daskgenie as dg
    import daskgenie.client as dg_client

    store = Store(":memory:")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    with _ServerThread(create_app(store), port):
        with (
            LocalCluster(
                n_workers=1, threads_per_worker=1, processes=True, dashboard_address=":0"
            ) as cluster,
            Client(cluster) as client,
        ):
            run_id = dg_client.register(
                client, url, sample_interval=0.05, deep=True, deep_epoch_seconds=1.0
            )
            time.sleep(0.6)  # let the plugin + memray tracker install

            with dg.track():
                x = da.ones((6000, 6000), chunks=(3000, 3000))
                x.map_blocks(_big_alloc, dtype="float64").sum().compute()

            time.sleep(3.0)  # let an epoch close and flush

        sites = store.alloc_sites(run_id)
        assert sites, "no deep allocation sites reached the collector"
        # the biggest allocation must be attributed to our _big_alloc line
        top = sites[0]
        assert top["function"] == "_big_alloc"
        assert top["filename"].endswith("test_integration_deep.py")
        assert top["hwm_bytes"] > 50_000_000
