"""TimescaleDB-backed event store — the default collector backend.

Mirrors the public surface of :class:`daskgenie.collector.store.Store` (the
SQLite backend kept for tests/dev) so the FastAPI app is storage-agnostic:
``create_app`` takes whichever one ``make_store`` selected. Timescale is a
Postgres extension, so this is plain ``psycopg`` with three hypertables on the
high-rate time-series tables (samples / task_spans / alloc_sites /
worker_status); everything else is ordinary Postgres.

Concurrency mirrors the SQLite store: one connection guarded by a lock. At the
collector's ingest rate that is plenty; the upgrade path is a psycopg pool
behind this same API.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any

import psycopg

from daskgenie.common.schemas import (
    AllocationSite,
    ChunkMeta,
    DeathEvent,
    GraphUpload,
    MemorySample,
    RunInfo,
    SampleBatch,
)

_SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    origin TEXT NOT NULL DEFAULT '',
    origin_ip TEXT NOT NULL DEFAULT ''
);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS origin_ip TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS samples (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    rss_bytes BIGINT NOT NULL,
    managed_bytes BIGINT NOT NULL,
    executing_keys TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_run_worker_ts ON samples(run_id, worker, timestamp);

CREATE TABLE IF NOT EXISTS chunks (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    task_key TEXT NOT NULL,
    shape TEXT NOT NULL,
    dtype TEXT NOT NULL,
    nbytes BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_run_task_key ON chunks(run_id, task_key);

CREATE TABLE IF NOT EXISTS graph_layers (
    run_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    filename TEXT NOT NULL,
    lineno INTEGER NOT NULL,
    code_snippet TEXT NOT NULL,
    PRIMARY KEY (run_id, layer)
);

CREATE TABLE IF NOT EXISTS graph_deps (
    run_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    dep TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_nodes (
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    layer TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gnodes_run ON graph_nodes(run_id);

CREATE TABLE IF NOT EXISTS graph_edges (
    run_id TEXT NOT NULL,
    src TEXT NOT NULL,
    dst TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gedges_run ON graph_edges(run_id);

CREATE TABLE IF NOT EXISTS graph_meta (
    run_id TEXT PRIMARY KEY,
    task_count INTEGER NOT NULL,
    truncated INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS deaths (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    worker TEXT NOT NULL,
    suspect_keys TEXT NOT NULL,
    suspect_chunks TEXT NOT NULL,
    suspect_sites TEXT NOT NULL,
    suspected_oom INTEGER NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deaths_run ON deaths(run_id, timestamp);

CREATE TABLE IF NOT EXISTS task_spans (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    layer TEXT NOT NULL,
    start DOUBLE PRECISION NOT NULL,
    "end" DOUBLE PRECISION NOT NULL,
    worker TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spans_run_start ON task_spans(run_id, start);

CREATE TABLE IF NOT EXISTS alloc_sites (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    filename TEXT NOT NULL,
    lineno INTEGER NOT NULL,
    function TEXT NOT NULL,
    hwm_bytes BIGINT NOT NULL,
    n_allocations BIGINT NOT NULL,
    task_key TEXT NOT NULL,
    layer TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alloc_run ON alloc_sites(run_id);
CREATE INDEX IF NOT EXISTS idx_alloc_run_task ON alloc_sites(run_id, task_key);

CREATE TABLE IF NOT EXISTS task_memory (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    layer TEXT NOT NULL,
    worker TEXT NOT NULL,
    peak_rss_delta BIGINT NOT NULL,
    top_sites TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_taskmem_run ON task_memory(run_id);

CREATE TABLE IF NOT EXISTS worker_status (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    rss_bytes BIGINT NOT NULL,
    managed_bytes BIGINT NOT NULL,
    memory_limit BIGINT NOT NULL,
    cpu DOUBLE PRECISION NOT NULL,
    nthreads INTEGER NOT NULL,
    executing INTEGER NOT NULL,
    ready INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wstatus_run_worker_ts ON worker_status(run_id, worker, timestamp);

CREATE TABLE IF NOT EXISTS alloc_stacks (
    id BIGSERIAL,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    frames TEXT NOT NULL,
    hwm_bytes BIGINT NOT NULL,
    n_allocations BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stacks_run_worker ON alloc_stacks(run_id, worker);
"""

# Turn the high-rate tables into hypertables. Wrapped individually because
# create_hypertable errors if the table already has rows and isn't yet a
# hypertable; if_not_exists + migrate_data keeps re-runs idempotent.
_HYPERTABLES = [
    ("samples", "timestamp"),
    ("task_spans", "start"),
    ("alloc_sites", "ts"),
    ("alloc_stacks", "ts"),
    ("worker_status", "timestamp"),
]


def _split_statements(script: str) -> list[str]:
    """Split a simple DDL script into individual statements. The schema has no
    semicolons inside statements (no functions/dollar-quoting), so splitting on
    ';' is safe."""
    return [s.strip() for s in script.split(";") if s.strip()]


class TimescaleStore:
    def __init__(self, dsn: str) -> None:
        # autocommit so a single failing statement can't poison the shared
        # connection for every later request; the multi-insert writes below wrap
        # their statements in an explicit transaction for atomicity.
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._lock = threading.Lock()
        with self._lock:
            # psycopg3's execute() runs a single command, so the schema (a
            # multi-statement script) must be applied statement by statement.
            for statement in _split_statements(_SCHEMA):
                self._conn.execute(statement)
            for table, col in _HYPERTABLES:
                try:
                    self._conn.execute(
                        "SELECT create_hypertable(%s, %s, if_not_exists => TRUE, "
                        "migrate_data => TRUE)",
                        (table, col),
                    )
                except psycopg.Error:
                    pass

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- runs ---------------------------------------------------------------

    def create_run(self, name: str = "", origin: str = "", origin_ip: str = "") -> RunInfo:
        run_id = uuid.uuid4().hex[:12]
        created = time.time()
        name = name or f"run-{run_id}"
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs (id, name, created_at, origin, origin_ip) "
                "VALUES (%s, %s, %s, %s, %s)",
                (run_id, name, created, origin, origin_ip),
            )
            self._conn.commit()
        return RunInfo(id=run_id, name=name, created_at=created, origin=origin, origin_ip=origin_ip)

    def ensure_run(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs (id, name, created_at) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO NOTHING",
                (run_id, f"run-{run_id}", time.time()),
            )
            self._conn.commit()

    def list_runs(self) -> list[RunInfo]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, name, created_at, origin, origin_ip FROM runs ORDER BY created_at DESC"
            ).fetchall()
            runs = []
            for r in rows:
                runs.append(
                    RunInfo(
                        id=r[0],
                        name=r[1],
                        created_at=r[2],
                        origin=r[3],
                        origin_ip=r[4],
                        counts=self._counts(r[0]),
                    )
                )
        return runs

    def get_run(self, run_id: str) -> RunInfo | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT id, name, created_at, origin, origin_ip FROM runs WHERE id = %s",
                (run_id,),
            ).fetchone()
            if r is None:
                return None
            counts = self._counts(run_id)
        return RunInfo(
            id=r[0], name=r[1], created_at=r[2], origin=r[3], origin_ip=r[4], counts=counts
        )

    def _counts(self, run_id: str) -> dict[str, int]:
        # caller holds the lock
        return {
            "samples": self._scalar("SELECT COUNT(*) FROM samples WHERE run_id = %s", run_id),
            "deaths": self._scalar("SELECT COUNT(*) FROM deaths WHERE run_id = %s", run_id),
            "workers": self._scalar(
                "SELECT COUNT(DISTINCT worker) FROM samples WHERE run_id = %s", run_id
            ),
        }

    def delete_run(self, run_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM runs WHERE id = %s", (run_id,))
            for table in (
                "samples",
                "chunks",
                "graph_layers",
                "graph_deps",
                "graph_nodes",
                "graph_edges",
                "graph_meta",
                "deaths",
                "task_spans",
                "alloc_sites",
                "alloc_stacks",
                "task_memory",
                "worker_status",
            ):
                self._conn.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))  # noqa: S608
            self._conn.commit()
        return (cur.rowcount or 0) > 0

    def _scalar(self, sql: str, *params: object) -> int:
        row = self._conn.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    # -- ingest -------------------------------------------------------------

    def add_samples(self, batch: SampleBatch) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO samples (run_id, worker, timestamp, rss_bytes, managed_bytes, "
                "executing_keys) VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (
                        batch.run_id,
                        batch.worker,
                        s.timestamp,
                        s.rss_bytes,
                        s.managed_bytes,
                        json.dumps(s.executing_keys),
                    )
                    for s in batch.samples
                ],
            )
            cur.executemany(
                "INSERT INTO chunks (run_id, task_key, shape, dtype, nbytes) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (batch.run_id, c.task_key, json.dumps(list(c.shape)), c.dtype, c.nbytes)
                    for c in batch.chunks
                ],
            )
            cur.executemany(
                'INSERT INTO task_spans (run_id, key, layer, start, "end", worker) '
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [(batch.run_id, s.key, s.layer, s.start, s.end, s.worker) for s in batch.spans],
            )
            cur.executemany(
                "INSERT INTO alloc_sites (run_id, worker, ts, filename, lineno, function, "
                "hwm_bytes, n_allocations, task_key, layer) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    (
                        batch.run_id,
                        ep.worker,
                        ep.end,
                        site.filename,
                        site.lineno,
                        site.function,
                        site.hwm_bytes,
                        site.n_allocations,
                        site.task_key,
                        site.layer,
                    )
                    for ep in batch.epochs
                    for site in ep.sites
                ],
            )
            cur.executemany(
                "INSERT INTO alloc_stacks (run_id, worker, ts, frames, hwm_bytes, n_allocations) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (
                        batch.run_id,
                        ep.worker,
                        ep.end,
                        json.dumps([[f.function, f.filename, f.lineno] for f in st.frames]),
                        st.hwm_bytes,
                        st.n_allocations,
                    )
                    for ep in batch.epochs
                    for st in ep.stacks
                ],
            )
            cur.executemany(
                "INSERT INTO task_memory (run_id, key, layer, worker, peak_rss_delta, top_sites) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (
                        batch.run_id,
                        tm.key,
                        tm.layer,
                        tm.worker,
                        tm.peak_rss_delta,
                        json.dumps([s.model_dump() for s in tm.top_sites]),
                    )
                    for tm in batch.task_memory
                ],
            )
            cur.executemany(
                "INSERT INTO worker_status (run_id, worker, timestamp, rss_bytes, managed_bytes, "
                "memory_limit, cpu, nthreads, executing, ready) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    (
                        batch.run_id,
                        st.worker,
                        st.timestamp,
                        st.rss_bytes,
                        st.managed_bytes,
                        st.memory_limit,
                        st.cpu,
                        st.nthreads,
                        st.executing,
                        st.ready,
                    )
                    for st in batch.statuses
                ],
            )
            self._conn.commit()

    def add_graph(self, upload: GraphUpload) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO graph_layers (run_id, layer, filename, lineno, code_snippet) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT(run_id, layer) DO UPDATE SET "
                "filename=excluded.filename, lineno=excluded.lineno, "
                "code_snippet=excluded.code_snippet",
                [
                    (upload.run_id, ly.layer, ly.filename, ly.lineno, ly.code_snippet)
                    for ly in upload.layers
                ],
            )
            cur.execute("DELETE FROM graph_deps WHERE run_id = %s", (upload.run_id,))
            cur.executemany(
                "INSERT INTO graph_deps (run_id, layer, dep) VALUES (%s, %s, %s)",
                [
                    (upload.run_id, layer, dep)
                    for layer, deps in upload.layer_dependencies.items()
                    for dep in deps
                ],
            )
            if upload.nodes or upload.edges or upload.task_count:
                cur.execute("DELETE FROM graph_nodes WHERE run_id = %s", (upload.run_id,))
                cur.execute("DELETE FROM graph_edges WHERE run_id = %s", (upload.run_id,))
                cur.executemany(
                    "INSERT INTO graph_nodes (run_id, key, layer) VALUES (%s, %s, %s)",
                    [(upload.run_id, n.key, n.layer) for n in upload.nodes],
                )
                cur.executemany(
                    "INSERT INTO graph_edges (run_id, src, dst) VALUES (%s, %s, %s)",
                    [(upload.run_id, src, dst) for src, dst in upload.edges],
                )
                cur.execute(
                    "INSERT INTO graph_meta (run_id, task_count, truncated) VALUES (%s, %s, %s) "
                    "ON CONFLICT(run_id) DO UPDATE SET task_count=excluded.task_count, "
                    "truncated=excluded.truncated",
                    (upload.run_id, upload.task_count, int(upload.truncated)),
                )
            self._conn.commit()

    def add_death(self, event: DeathEvent) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO deaths (run_id, timestamp, worker, suspect_keys, suspect_chunks, "
                "suspect_sites, suspected_oom, reason) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    event.run_id,
                    event.timestamp,
                    event.worker,
                    json.dumps(event.suspect_keys),
                    json.dumps([c.model_dump() for c in event.suspect_chunks]),
                    json.dumps([s.model_dump() for s in event.suspect_sites]),
                    int(event.suspected_oom),
                    event.reason,
                ),
            )
            self._conn.commit()

    # -- query (all scoped to a run) ---------------------------------------

    def timeline(
        self, run_id: str, worker: str | None = None, limit: int = 10000
    ) -> list[dict[str, Any]]:
        with self._lock:
            if worker is None:
                rows = self._conn.execute(
                    "SELECT worker, timestamp, rss_bytes, managed_bytes, executing_keys "
                    "FROM samples WHERE run_id = %s ORDER BY timestamp DESC LIMIT %s",
                    (run_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT worker, timestamp, rss_bytes, managed_bytes, executing_keys "
                    "FROM samples WHERE run_id = %s AND worker = %s "
                    "ORDER BY timestamp DESC LIMIT %s",
                    (run_id, worker, limit),
                ).fetchall()
        return [
            {
                "worker": r[0],
                "timestamp": r[1],
                "rss_bytes": r[2],
                "managed_bytes": r[3],
                "executing_keys": json.loads(r[4]),
            }
            for r in rows
        ]

    def chunks_for(self, run_id: str, task_key: str) -> list[ChunkMeta]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_key, shape, dtype, nbytes FROM chunks "
                "WHERE run_id = %s AND task_key = %s ORDER BY nbytes DESC",
                (run_id, task_key),
            ).fetchall()
        return [
            ChunkMeta(task_key=r[0], shape=tuple(json.loads(r[1])), dtype=r[2], nbytes=r[3])
            for r in rows
        ]

    def sites_for(self, run_id: str, task_key: str) -> list[AllocationSite]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT filename, lineno, function, MAX(hwm_bytes) AS hwm, "
                "SUM(n_allocations) AS na, MAX(task_key) AS tk, MAX(layer) AS ly "
                "FROM alloc_sites WHERE run_id = %s AND task_key = %s "
                "GROUP BY filename, lineno, function ORDER BY hwm DESC",
                (run_id, task_key),
            ).fetchall()
        return [
            AllocationSite(
                filename=r[0],
                lineno=r[1],
                function=r[2],
                hwm_bytes=int(r[3] or 0),
                n_allocations=int(r[4] or 0),
                task_key=r[5],
                layer=r[6],
            )
            for r in rows
        ]

    def alloc_sites(
        self,
        run_id: str,
        limit: int = 500,
        start: float | None = None,
        end: float | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["run_id = %s"]
        params: list[Any] = [run_id]
        if start is not None:
            clauses.append("ts >= %s")
            params.append(start)
        if end is not None:
            clauses.append("ts <= %s")
            params.append(end)
        where = " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(
                "SELECT filename, lineno, function, MAX(hwm_bytes) AS hwm, "  # noqa: S608
                "SUM(n_allocations) AS na, string_agg(DISTINCT layer, ',') AS layers "
                f"FROM alloc_sites WHERE {where} GROUP BY filename, lineno, function "
                "ORDER BY hwm DESC LIMIT %s",
                (*params, limit),
            ).fetchall()
        return [
            {
                "filename": r[0],
                "lineno": r[1],
                "function": r[2],
                "hwm_bytes": int(r[3] or 0),
                "n_allocations": int(r[4] or 0),
                "layers": [x for x in (r[5] or "").split(",") if x],
            }
            for r in rows
        ]

    def flamegraph(
        self,
        run_id: str,
        worker: str | None = None,
        start: float | None = None,
        end: float | None = None,
        limit: int = 400,
    ) -> dict[str, Any]:
        clauses = ["run_id = %s"]
        params: list[Any] = [run_id]
        if worker:
            clauses.append("worker = %s")
            params.append(worker)
        if start is not None:
            clauses.append("ts >= %s")
            params.append(start)
        if end is not None:
            clauses.append("ts <= %s")
            params.append(end)
        where = " AND ".join(clauses)
        with self._lock:
            workers = [
                r[0]
                for r in self._conn.execute(
                    "SELECT DISTINCT worker FROM alloc_stacks WHERE run_id = %s ORDER BY worker",
                    (run_id,),
                ).fetchall()
            ]
            rows = self._conn.execute(
                f"SELECT frames, MAX(hwm_bytes) AS hwm, SUM(n_allocations) AS na "  # noqa: S608
                f"FROM alloc_stacks WHERE {where} GROUP BY frames ORDER BY hwm DESC LIMIT %s",
                (*params, limit),
            ).fetchall()
        stacks = [
            {
                "frames": [
                    {"function": f[0], "filename": f[1], "lineno": f[2]} for f in json.loads(r[0])
                ],
                "hwm_bytes": int(r[1] or 0),
                "n_allocations": int(r[2] or 0),
            }
            for r in rows
        ]
        return {"workers": workers, "stacks": stacks}

    def alloc_timeline(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, layer, SUM(hwm_bytes) AS bytes FROM alloc_sites "
                "WHERE run_id = %s GROUP BY ts, layer ORDER BY ts",
                (run_id,),
            ).fetchall()
        # Postgres SUM(bigint) is numeric -> psycopg Decimal -> JSON string; cast
        # to int so the dashboard gets real numbers (not string-concatenated ones).
        return [
            {"ts": r[0], "layer": r[1] or "(unattributed)", "bytes": int(r[2] or 0)} for r in rows
        ]

    def task_memory(self, run_id: str, limit: int = 2000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, MAX(layer) AS layer, MAX(worker) AS worker, "
                "MAX(peak_rss_delta) AS peak, "
                "(array_agg(top_sites ORDER BY peak_rss_delta DESC))[1] AS top "
                "FROM task_memory WHERE run_id = %s GROUP BY key "
                "ORDER BY peak DESC LIMIT %s",
                (run_id, limit),
            ).fetchall()
        return [
            {
                "key": r[0],
                "layer": r[1],
                "worker": r[2],
                "peak_rss_delta": int(r[3] or 0),
                "top_sites": json.loads(r[4]) if r[4] else [],
            }
            for r in rows
        ]

    def worker_status(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT ON (worker) worker, timestamp, rss_bytes, managed_bytes, "
                "memory_limit, cpu, nthreads, executing, ready FROM worker_status "
                "WHERE run_id = %s ORDER BY worker, timestamp DESC",
                (run_id,),
            ).fetchall()
        cols = [
            "worker",
            "timestamp",
            "rss_bytes",
            "managed_bytes",
            "memory_limit",
            "cpu",
            "nthreads",
            "executing",
            "ready",
        ]
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def graph(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            layers = self._conn.execute(
                "SELECT layer, filename, lineno, code_snippet FROM graph_layers WHERE run_id = %s",
                (run_id,),
            ).fetchall()
            deps = self._conn.execute(
                "SELECT layer, dep FROM graph_deps WHERE run_id = %s", (run_id,)
            ).fetchall()
            gnodes = self._conn.execute(
                "SELECT key, layer FROM graph_nodes WHERE run_id = %s", (run_id,)
            ).fetchall()
            gedges = self._conn.execute(
                "SELECT src, dst FROM graph_edges WHERE run_id = %s", (run_id,)
            ).fetchall()
            meta = self._conn.execute(
                "SELECT task_count, truncated FROM graph_meta WHERE run_id = %s", (run_id,)
            ).fetchone()
        dep_map: dict[str, list[str]] = {}
        for d in deps:
            dep_map.setdefault(d[0], []).append(d[1])
        return {
            "run_id": run_id,
            "layers": [
                {"layer": r[0], "filename": r[1], "lineno": r[2], "code_snippet": r[3]}
                for r in layers
            ],
            "layer_dependencies": dep_map,
            "nodes": [{"key": n[0], "layer": n[1]} for n in gnodes],
            "edges": [[e[0], e[1]] for e in gedges],
            "task_count": meta[0] if meta else 0,
            "truncated": bool(meta[1]) if meta else False,
        }

    def deaths(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT timestamp, worker, suspect_keys, suspect_chunks, suspect_sites, "
                "suspected_oom, reason FROM deaths WHERE run_id = %s ORDER BY timestamp DESC",
                (run_id,),
            ).fetchall()
        return [
            {
                "timestamp": r[0],
                "worker": r[1],
                "suspect_keys": json.loads(r[2]),
                "suspect_chunks": json.loads(r[3]),
                "suspect_sites": json.loads(r[4]),
                "suspected_oom": bool(r[5]),
                "reason": r[6],
            }
            for r in rows
        ]

    def spans(self, run_id: str, limit: int = 20000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                'SELECT key, layer, start, "end", worker FROM task_spans '
                "WHERE run_id = %s ORDER BY start LIMIT %s",
                (run_id, limit),
            ).fetchall()
        return [
            {"key": r[0], "layer": r[1], "start": r[2], "end": r[3], "worker": r[4]} for r in rows
        ]

    def layer_stats(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                'SELECT layer, COUNT(*) AS n, SUM("end" - start) AS total, '
                'MAX("end" - start) AS longest FROM task_spans WHERE run_id = %s '
                "GROUP BY layer ORDER BY total DESC",
                (run_id,),
            ).fetchall()
        return [
            {
                "layer": r[0],
                "count": r[1],
                "total_seconds": r[2] or 0.0,
                "longest_seconds": r[3] or 0.0,
            }
            for r in rows
        ]

    def latest_memory_by_worker(self) -> dict[str, MemorySample]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT ON (worker) worker, timestamp, rss_bytes, managed_bytes, "
                "executing_keys FROM samples ORDER BY worker, timestamp DESC"
            ).fetchall()
        return {
            r[0]: MemorySample(
                timestamp=r[1],
                rss_bytes=r[2],
                managed_bytes=r[3],
                executing_keys=json.loads(r[4]),
            )
            for r in rows
        }
