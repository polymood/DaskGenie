from __future__ import annotations

from fastapi.testclient import TestClient

from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store
from daskgenie.common.schemas import (
    SCHEMA_VERSION,
    ChunkMeta,
    DeathEvent,
    GraphLayer,
    GraphUpload,
    MemorySample,
    SampleBatch,
)


def _client() -> TestClient:
    return TestClient(create_app(Store(":memory:")))


def test_ingest_samples_roundtrips_to_timeline() -> None:
    client = _client()
    batch = SampleBatch(
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

    timeline = client.get("/api/timeline", params={"worker": "tcp://w1"}).json()
    assert len(timeline) == 2
    assert {row["rss_bytes"] for row in timeline} == {100, 200}

    chunks = client.get("/api/chunks/a").json()
    assert len(chunks) == 1
    assert chunks[0]["nbytes"] == 800
    assert tuple(chunks[0]["shape"]) == (10, 10)


def test_metrics_exposes_worker_memory() -> None:
    client = _client()
    batch = SampleBatch(
        worker="tcp://w1",
        samples=[MemorySample(timestamp=1.0, rss_bytes=555, managed_bytes=222, executing_keys=[])],
    )
    client.post("/ingest/samples", json=batch.model_dump())
    body = client.get("/metrics").text
    assert 'daskgenie_worker_rss_bytes{worker="tcp://w1"} 555.0' in body
    assert "daskgenie_samples_total 1.0" in body


def test_schema_version_mismatch_rejected() -> None:
    client = _client()
    payload = SampleBatch(worker="w").model_dump()
    payload["schema_version"] = SCHEMA_VERSION + 1
    resp = client.post("/ingest/samples", json=payload)
    assert resp.status_code == 409


def test_graph_upload_and_query() -> None:
    client = _client()
    upload = GraphUpload(
        run_id="run1",
        layers=[GraphLayer(layer="rechunk", filename="p.py", lineno=5, code_snippet="x.rechunk()")],
        layer_dependencies={"rechunk": ["open"]},
    )
    client.post("/ingest/graph", json=upload.model_dump())
    graph = client.get("/api/graph/run1").json()
    assert graph["layers"][0]["layer"] == "rechunk"
    assert graph["layer_dependencies"] == {"rechunk": ["open"]}


def test_death_event_roundtrip_and_counter() -> None:
    client = _client()
    event = DeathEvent(
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
    deaths = client.get("/api/deaths").json()
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
            timestamp=1.0, worker="tcp://w1", suspect_keys=["rechunk-merge-1"], suspected_oom=True
        ).model_dump(),
    )
    death = client.get("/api/deaths").json()[0]
    assert len(death["suspect_chunks"]) == 1
    assert death["suspect_chunks"][0]["nbytes"] == 512_000_000
    assert death["suspect_chunks"][0]["task_key"] == "rechunk-merge-1"
