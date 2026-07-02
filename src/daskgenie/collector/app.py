"""FastAPI collector: ingest endpoints, Prometheus ``/metrics``, and the query
API the UI reads from.

Metrics are deliberately useful on their own — point Grafana at ``/metrics``
and you get per-worker memory and a death counter without ever opening the
custom UI. Schema-version mismatches are rejected at the door with 409 so a
stale plugin can't quietly corrupt the store.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from daskgenie.collector.store import Store
from daskgenie.common.schemas import SCHEMA_VERSION, DeathEvent, GraphUpload, SampleBatch


def create_app(store: Store | None = None) -> FastAPI:
    app = FastAPI(title="DaskGenie Collector", version="0.1.0")
    store = store or Store()

    # A private registry (not the global default) so repeated create_app calls
    # in tests don't raise "Duplicated timeseries" on re-registration.
    registry = CollectorRegistry()
    rss_gauge = Gauge(
        "daskgenie_worker_rss_bytes", "Worker resident set size", ["worker"], registry=registry
    )
    managed_gauge = Gauge(
        "daskgenie_worker_managed_bytes",
        "Worker Dask-managed memory",
        ["worker"],
        registry=registry,
    )
    samples_counter = Counter(
        "daskgenie_samples_total", "Memory samples ingested", registry=registry
    )
    deaths_counter = Counter(
        "daskgenie_worker_deaths_total", "Worker death events recorded", registry=registry
    )

    def _reject_stale(version: int) -> None:
        if version != SCHEMA_VERSION:
            raise HTTPException(
                status_code=409,
                detail=f"schema_version {version} != collector {SCHEMA_VERSION}",
            )

    @app.post("/ingest/samples")
    def ingest_samples(batch: SampleBatch) -> dict[str, int]:
        _reject_stale(batch.schema_version)
        store.add_samples(batch)
        samples_counter.inc(len(batch.samples))
        for s in batch.samples:
            rss_gauge.labels(batch.worker).set(s.rss_bytes)
            managed_gauge.labels(batch.worker).set(s.managed_bytes)
        return {"samples": len(batch.samples), "chunks": len(batch.chunks)}

    @app.post("/ingest/graph")
    def ingest_graph(upload: GraphUpload) -> dict[str, int]:
        _reject_stale(upload.schema_version)
        store.add_graph(upload)
        return {"layers": len(upload.layers)}

    @app.post("/ingest/death")
    def ingest_death(event: DeathEvent) -> dict[str, str]:
        _reject_stale(event.schema_version)
        store.add_death(event)
        deaths_counter.inc()
        return {"status": "recorded"}

    @app.get("/metrics")
    def metrics() -> Response:
        # Refresh gauges from the latest stored sample per worker so a scrape
        # reflects current state even if no push landed since the last one.
        for worker, sample in store.latest_memory_by_worker().items():
            rss_gauge.labels(worker).set(sample.rss_bytes)
            managed_gauge.labels(worker).set(sample.managed_bytes)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/timeline")
    def timeline(worker: str | None = None) -> list[dict[str, Any]]:
        return store.timeline(worker)

    @app.get("/api/chunks/{task_key:path}")
    def chunk(task_key: str) -> dict[str, Any]:
        meta = store.chunk(task_key)
        if meta is None:
            raise HTTPException(status_code=404, detail="unknown task key")
        return meta.model_dump()

    @app.get("/api/graph/{run_id}")
    def graph(run_id: str) -> dict[str, Any]:
        return store.graph(run_id)

    @app.get("/api/deaths")
    def deaths() -> list[dict[str, Any]]:
        return store.deaths()

    return app
