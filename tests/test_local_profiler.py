"""The local-scheduler profiler works with any non-distributed scheduler.
Driven with the collector HTTP calls monkeypatched out, so it runs fast and
offline while still exercising the real dask.callbacks hooks.
"""

from __future__ import annotations

from typing import Any

import dask.array as da
import pytest

import daskgenie.report as report
from daskgenie import LocalProfiler
from daskgenie.common.schemas import SampleBatch


@pytest.fixture
def captured(monkeypatch: Any) -> list[SampleBatch]:
    batches: list[SampleBatch] = []
    monkeypatch.setattr(report, "create_run", lambda url, name="": "run-test")
    monkeypatch.setattr(report, "post_sample_batch", lambda url, batch, **kw: batches.append(batch))
    monkeypatch.setattr(report, "upload_graph", lambda *a, **k: None)
    return batches


def _all(batches: list[SampleBatch]):
    samples = [s for b in batches for s in b.samples]
    chunks = [c for b in batches for c in b.chunks]
    return samples, chunks


@pytest.mark.parametrize("scheduler", ["synchronous", "threads"])
def test_local_profiler_records_samples_and_chunks(captured, scheduler: str) -> None:
    prof = LocalProfiler("http://x", run_name=f"{scheduler} job", sample_interval=0.02)
    assert prof.run_id == "run-test"

    with prof:
        result = (da.ones((400, 400), chunks=(100, 100)) + 1).sum()
        value = result.compute(scheduler=scheduler)
    assert float(value) == 400 * 400 * 2

    samples, chunks = _all(captured)
    # every batch is tagged with the run and the local worker label
    assert all(b.run_id == "run-test" for b in captured)
    assert all(b.worker == prof.worker_label for b in captured)
    # chunk metadata was captured from task outputs (numpy arrays)
    assert chunks, "no chunk metadata recorded"
    assert any(c.nbytes > 0 and c.dtype for c in chunks)


def test_local_profiler_uploads_graph_on_exit(monkeypatch: Any) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(report, "create_run", lambda url, name="": "run-g")
    monkeypatch.setattr(report, "post_sample_batch", lambda *a, **k: None)
    monkeypatch.setattr(report, "upload_graph", lambda *a, **k: calls.append(a))

    source_map = {"ones": object()}
    with LocalProfiler("http://x", source_map=source_map):  # type: ignore[arg-type]
        (da.ones((10, 10), chunks=(5, 5)) + 1).sum().compute(scheduler="synchronous")

    assert calls, "graph was not uploaded on exit"
    assert calls[0][1] == "run-g"  # (url, run_id, source_map, ...)
