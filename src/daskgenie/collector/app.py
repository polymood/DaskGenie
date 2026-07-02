"""FastAPI collector: ingest endpoints, Prometheus ``/metrics``, and the query
API the UI reads from.

Metrics are deliberately useful on their own — point Grafana at ``/metrics``
and you get per-worker memory and a death counter without ever opening the
custom UI. Schema-version mismatches are rejected at the door with 409 so a
stale plugin can't quietly corrupt the store.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from daskgenie.collector.store import Store
from daskgenie.common.schemas import (
    SCHEMA_VERSION,
    DeathEvent,
    GraphUpload,
    RunCreate,
    RunInfo,
    SampleBatch,
)


def create_app(store: Store | None = None, static_dir: str | Path | None = None) -> FastAPI:
    app = FastAPI(title="DaskGenie Collector", version="0.1.0")
    store = store or Store()
    static_dir = static_dir or os.environ.get("DASKGENIE_STATIC_DIR")

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

    # -- ingest -------------------------------------------------------------

    @app.post("/ingest/samples")
    def ingest_samples(batch: SampleBatch) -> dict[str, int]:
        _reject_stale(batch.schema_version)
        store.ensure_run(batch.run_id)
        store.add_samples(batch)
        samples_counter.inc(len(batch.samples))
        for s in batch.samples:
            rss_gauge.labels(batch.worker).set(s.rss_bytes)
            managed_gauge.labels(batch.worker).set(s.managed_bytes)
        return {"samples": len(batch.samples), "chunks": len(batch.chunks)}

    @app.post("/ingest/graph")
    def ingest_graph(upload: GraphUpload) -> dict[str, int]:
        _reject_stale(upload.schema_version)
        store.ensure_run(upload.run_id)
        store.add_graph(upload)
        return {"layers": len(upload.layers)}

    @app.post("/ingest/death")
    def ingest_death(event: DeathEvent) -> dict[str, str]:
        _reject_stale(event.schema_version)
        store.ensure_run(event.run_id)
        # The scheduler only knows *which* tasks were in-flight; the chunk sizes
        # were captured worker-side and already live here. Join them now so the
        # stored post-mortem answers "which chunk killed this worker" directly.
        enriched = list(event.suspect_chunks)
        for key in event.suspect_keys:
            enriched.extend(store.chunks_for(event.run_id, key))
        store.add_death(event.model_copy(update={"suspect_chunks": enriched}))
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

    # -- run management -----------------------------------------------------

    @app.post("/api/runs")
    def create_run(body: RunCreate) -> RunInfo:
        return store.create_run(body.name)

    @app.get("/api/runs")
    def list_runs() -> list[RunInfo]:
        return store.list_runs()

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> RunInfo:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="unknown run")
        return run

    @app.delete("/api/runs/{run_id}")
    def delete_run(run_id: str) -> dict[str, bool]:
        return {"deleted": store.delete_run(run_id)}

    # -- run-scoped query ---------------------------------------------------

    @app.get("/api/runs/{run_id}/timeline")
    def timeline(run_id: str, worker: str | None = None) -> list[dict[str, Any]]:
        return store.timeline(run_id, worker)

    @app.get("/api/runs/{run_id}/chunks/{task_key:path}")
    def chunks(run_id: str, task_key: str) -> list[dict[str, Any]]:
        return [c.model_dump() for c in store.chunks_for(run_id, task_key)]

    @app.get("/api/runs/{run_id}/graph")
    def graph(run_id: str) -> dict[str, Any]:
        return store.graph(run_id)

    @app.get("/api/runs/{run_id}/deaths")
    def deaths(run_id: str) -> list[dict[str, Any]]:
        return store.deaths(run_id)

    # Serve the built SPA (if present) as a catch-all *after* the API routes, so
    # /api, /ingest, /metrics win and any other path falls through to index.html
    # for client-side routing. Absent in dev (Vite serves the SPA) and in tests.
    if static_dir and Path(static_dir).is_dir():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="spa")

    return app
