from __future__ import annotations

from fastapi.testclient import TestClient

from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store
from daskgenie.common.schemas import (
    SCHEMA_VERSION,
    AllocationSite,
    ChunkMeta,
    DeathEvent,
    GraphLayer,
    GraphUpload,
    MemoryEpoch,
    MemorySample,
    SampleBatch,
    TaskMemory,
    WorkerStatus,
)

RUN = "run1"


def _client() -> TestClient:
    return TestClient(create_app(Store(":memory:")))


def test_ingest_samples_roundtrips_to_timeline() -> None:
    client = _client()
    batch = SampleBatch(
        run_id=RUN,
        worker="tcp://w1",
        samples=[
            MemorySample(timestamp=1.0, rss_bytes=100, managed_bytes=40, executing_keys=["a"]),
            MemorySample(timestamp=2.0, rss_bytes=200, managed_bytes=80, executing_keys=["b"]),
        ],
        chunks=[ChunkMeta(task_key="a", shape=(10, 10), dtype="float64", nbytes=800)],
    )
    resp = client.post("/ingest/samples", json=batch.model_dump())
    assert resp.status_code == 200
    assert resp.json() == {"samples": 2, "chunks": 1}

    timeline = client.get(f"/api/runs/{RUN}/timeline", params={"worker": "tcp://w1"}).json()
    assert len(timeline) == 2
    assert {row["rss_bytes"] for row in timeline} == {100, 200}

    chunks = client.get(f"/api/runs/{RUN}/chunks/a").json()
    assert len(chunks) == 1
    assert chunks[0]["nbytes"] == 800
    assert tuple(chunks[0]["shape"]) == (10, 10)


def test_ingest_auto_registers_run_and_lists_it() -> None:
    client = _client()
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="tcp://w1",
            samples=[MemorySample(timestamp=1.0, rss_bytes=1, managed_bytes=1, executing_keys=[])],
        ).model_dump(),
    )
    runs = client.get("/api/runs").json()
    assert [r["id"] for r in runs] == [RUN]
    assert runs[0]["counts"]["samples"] == 1
    assert runs[0]["counts"]["workers"] == 1


def test_create_list_and_delete_run() -> None:
    client = _client()
    run = client.post("/api/runs", json={"name": "nightly"}).json()
    rid = run["id"]
    assert run["name"] == "nightly"

    assert client.get(f"/api/runs/{rid}").json()["name"] == "nightly"
    assert rid in [r["id"] for r in client.get("/api/runs").json()]

    assert client.delete(f"/api/runs/{rid}").json() == {"deleted": True}
    assert client.get(f"/api/runs/{rid}").status_code == 404
    assert rid not in [r["id"] for r in client.get("/api/runs").json()]


def test_delete_run_cascades_data() -> None:
    client = _client()
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="tcp://w1",
            samples=[MemorySample(timestamp=1.0, rss_bytes=1, managed_bytes=1, executing_keys=[])],
        ).model_dump(),
    )
    client.delete(f"/api/runs/{RUN}")
    assert client.get(f"/api/runs/{RUN}/timeline").json() == []


def test_runs_are_isolated_from_each_other() -> None:
    client = _client()
    for rid, rss in [("A", 111), ("B", 222)]:
        client.post(
            "/ingest/samples",
            json=SampleBatch(
                run_id=rid,
                worker="tcp://w1",
                samples=[
                    MemorySample(timestamp=1.0, rss_bytes=rss, managed_bytes=0, executing_keys=[])
                ],
            ).model_dump(),
        )
    a = client.get("/api/runs/A/timeline").json()
    assert [row["rss_bytes"] for row in a] == [111]


def test_metrics_exposes_worker_memory() -> None:
    client = _client()
    batch = SampleBatch(
        run_id=RUN,
        worker="tcp://w1",
        samples=[MemorySample(timestamp=1.0, rss_bytes=555, managed_bytes=222, executing_keys=[])],
    )
    client.post("/ingest/samples", json=batch.model_dump())
    body = client.get("/metrics").text
    assert 'daskgenie_worker_rss_bytes{worker="tcp://w1"} 555.0' in body
    assert "daskgenie_samples_total 1.0" in body


def test_schema_version_mismatch_rejected() -> None:
    client = _client()
    payload = SampleBatch(run_id=RUN, worker="w").model_dump()
    payload["schema_version"] = SCHEMA_VERSION + 1
    resp = client.post("/ingest/samples", json=payload)
    assert resp.status_code == 409


def test_graph_upload_and_query() -> None:
    client = _client()
    upload = GraphUpload(
        run_id=RUN,
        layers=[GraphLayer(layer="rechunk", filename="p.py", lineno=5, code_snippet="x.rechunk()")],
        layer_dependencies={"rechunk": ["open"]},
    )
    client.post("/ingest/graph", json=upload.model_dump())
    graph = client.get(f"/api/runs/{RUN}/graph").json()
    assert graph["layers"][0]["layer"] == "rechunk"
    assert graph["layer_dependencies"] == {"rechunk": ["open"]}


def test_death_event_roundtrip_and_counter() -> None:
    client = _client()
    event = DeathEvent(
        run_id=RUN,
        timestamp=9.0,
        worker="tcp://w1",
        suspect_keys=["rechunk-merge-1"],
        suspect_chunks=[
            ChunkMeta(
                task_key="rechunk-merge-1", shape=(8000, 8000), dtype="float64", nbytes=512_000_000
            )
        ],
        suspected_oom=True,
        reason="abrupt disappearance mid-task",
    )
    client.post("/ingest/death", json=event.model_dump())
    deaths = client.get(f"/api/runs/{RUN}/deaths").json()
    assert deaths[0]["suspected_oom"] is True
    assert deaths[0]["suspect_keys"] == ["rechunk-merge-1"]
    assert "daskgenie_worker_deaths_total 1.0" in client.get("/metrics").text


def test_death_event_is_enriched_with_stored_chunk_metadata() -> None:
    """The scheduler posts suspect keys only; the collector joins in the chunk
    metadata it already holds — this is the "which chunk killed the worker" join.
    """
    client = _client()
    # worker plugin recorded the input chunk this task was chewing on
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="tcp://w1",
            chunks=[
                ChunkMeta(
                    task_key="rechunk-merge-1",
                    shape=(8000, 8000),
                    dtype="float64",
                    nbytes=512_000_000,
                )
            ],
        ).model_dump(),
    )
    # scheduler posts the death with the suspect key but NO chunk metadata
    client.post(
        "/ingest/death",
        json=DeathEvent(
            run_id=RUN,
            timestamp=1.0,
            worker="tcp://w1",
            suspect_keys=["rechunk-merge-1"],
            suspected_oom=True,
        ).model_dump(),
    )
    death = client.get(f"/api/runs/{RUN}/deaths").json()[0]
    assert len(death["suspect_chunks"]) == 1
    assert death["suspect_chunks"][0]["nbytes"] == 512_000_000
    assert death["suspect_chunks"][0]["task_key"] == "rechunk-merge-1"


def test_worker_status_returns_latest_per_worker() -> None:
    client = _client()
    for ts, cpu in [(1.0, 10.0), (2.0, 55.0)]:
        client.post(
            "/ingest/samples",
            json=SampleBatch(
                run_id=RUN,
                worker="tcp://w1",
                statuses=[
                    WorkerStatus(
                        worker="tcp://w1",
                        timestamp=ts,
                        rss_bytes=int(ts),
                        managed_bytes=0,
                        memory_limit=1000,
                        cpu=cpu,
                        nthreads=4,
                        executing=2,
                        ready=3,
                    )
                ],
            ).model_dump(),
        )
    workers = client.get(f"/api/runs/{RUN}/workers").json()
    assert len(workers) == 1
    assert workers[0]["cpu"] == 55.0  # most recent heartbeat wins
    assert workers[0]["executing"] == 2


def test_alloc_sites_peak_per_line_across_epochs() -> None:
    client = _client()
    site = lambda hwm: AllocationSite(  # noqa: E731
        filename="job.py", lineno=42, function="build", hwm_bytes=hwm, n_allocations=1
    )
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="w1",
            epochs=[
                MemoryEpoch(worker="w1", start=0.0, end=1.0, peak_rss=100, sites=[site(100)]),
                MemoryEpoch(worker="w1", start=1.0, end=2.0, peak_rss=300, sites=[site(300)]),
            ],
        ).model_dump(),
    )
    sites = client.get(f"/api/runs/{RUN}/alloc-sites").json()
    assert len(sites) == 1
    # peak across disjoint epochs is the MAX, not the sum
    assert sites[0]["hwm_bytes"] == 300
    assert sites[0]["lineno"] == 42


def test_task_memory_roundtrip() -> None:
    client = _client()
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="w1",
            task_memory=[
                TaskMemory(
                    key="build-0",
                    layer="build",
                    worker="w1",
                    peak_rss_delta=128_000_000,
                    top_sites=[
                        AllocationSite(
                            filename="job.py", lineno=7, function="build", hwm_bytes=128_000_000
                        )
                    ],
                )
            ],
        ).model_dump(),
    )
    tm = client.get(f"/api/runs/{RUN}/task-memory").json()
    assert tm[0]["key"] == "build-0"
    assert tm[0]["peak_rss_delta"] == 128_000_000
    assert tm[0]["top_sites"][0]["lineno"] == 7


def test_death_enriched_with_alloc_sites() -> None:
    """The deep engine records which line was at the high-water mark per task;
    a death join surfaces it as the cause, alongside the chunk view.
    """
    client = _client()
    client.post(
        "/ingest/samples",
        json=SampleBatch(
            run_id=RUN,
            worker="tcp://w1",
            epochs=[
                MemoryEpoch(
                    worker="tcp://w1",
                    start=0.0,
                    end=1.0,
                    peak_rss=8_000_000_000,
                    sites=[
                        AllocationSite(
                            filename="job.py",
                            lineno=42,
                            function="build",
                            hwm_bytes=8_000_000_000,
                            task_key="build-0",
                            layer="build",
                        )
                    ],
                )
            ],
        ).model_dump(),
    )
    client.post(
        "/ingest/death",
        json=DeathEvent(
            run_id=RUN,
            timestamp=1.0,
            worker="tcp://w1",
            suspect_keys=["build-0"],
            suspected_oom=True,
        ).model_dump(),
    )
    death = client.get(f"/api/runs/{RUN}/deaths").json()[0]
    assert len(death["suspect_sites"]) == 1
    assert death["suspect_sites"][0]["lineno"] == 42
    assert death["suspect_sites"][0]["hwm_bytes"] == 8_000_000_000
