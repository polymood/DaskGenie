"""SQLite-backed event store.

SQLite by default keeps the tool self-contained — no server to stand up, one
file to hand someone (and one Docker volume to persist). The public methods
here are the whole storage contract; a TimescaleDB backend (spec stretch goal)
would reimplement this same surface.

Everything is scoped to a ``run`` — one cluster session's worth of data — so
the dashboard can list runs and drill into each independently.

Concurrency: uvicorn serves requests across a thread pool, so we open the
connection with ``check_same_thread=False`` and serialise every access through
one lock. At the collector's ingest rate a single connection is plenty; if it
ever isn't, the upgrade path is a connection pool behind this same API.
ponytail: single-connection + global lock, pool it only if write throughput bites.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Protocol

from daskgenie.common.schemas import (
    AllocationSite,
    ChunkMeta,
    DeathEvent,
    GraphUpload,
    MemorySample,
    RunInfo,
    SampleBatch,
)


class StoreProtocol(Protocol):
    """The storage contract shared by the SQLite :class:`Store` (tests/dev) and
    the Postgres/Timescale ``TimescaleStore`` (default in Docker). ``create_app``
    is typed against this so it stays backend-agnostic.
    """

    def create_run(self, name: str = ..., origin: str = ..., origin_ip: str = ...) -> RunInfo: ...
    def ensure_run(self, run_id: str) -> None: ...
    def list_runs(self) -> list[RunInfo]: ...
    def get_run(self, run_id: str) -> RunInfo | None: ...
    def delete_run(self, run_id: str) -> bool: ...
    def add_samples(self, batch: SampleBatch) -> None: ...
    def add_graph(self, upload: GraphUpload) -> None: ...
    def add_death(self, event: DeathEvent) -> None: ...
    def timeline(
        self, run_id: str, worker: str | None = ..., limit: int = ...
    ) -> list[dict[str, Any]]: ...
    def chunks_for(self, run_id: str, task_key: str) -> list[ChunkMeta]: ...
    def sites_for(self, run_id: str, task_key: str) -> list[AllocationSite]: ...
    def alloc_sites(
        self,
        run_id: str,
        limit: int = ...,
        start: float | None = ...,
        end: float | None = ...,
    ) -> list[dict[str, Any]]: ...
    def flamegraph(
        self,
        run_id: str,
        worker: str | None = ...,
        start: float | None = ...,
        end: float | None = ...,
        limit: int = ...,
    ) -> dict[str, Any]: ...
    def task_memory(self, run_id: str, limit: int = ...) -> list[dict[str, Any]]: ...
    def alloc_timeline(self, run_id: str) -> list[dict[str, Any]]: ...
    def worker_status(self, run_id: str) -> list[dict[str, Any]]: ...
    def graph(self, run_id: str) -> dict[str, Any]: ...
    def deaths(self, run_id: str) -> list[dict[str, Any]]: ...
    def spans(self, run_id: str, limit: int = ...) -> list[dict[str, Any]]: ...
    def layer_stats(self, run_id: str) -> list[dict[str, Any]]: ...
    def latest_memory_by_worker(self) -> dict[str, MemorySample]: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at REAL NOT NULL,
    origin TEXT NOT NULL DEFAULT '',
    origin_ip TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    timestamp REAL NOT NULL,
    rss_bytes INTEGER NOT NULL,
    managed_bytes INTEGER NOT NULL,
    executing_keys TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_run_worker_ts ON samples(run_id, worker, timestamp);

-- One row per (consuming task, input chunk): a task can hold several inputs,
-- and worker-side dedup already prevents duplicates, so no unique constraint.
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    task_key TEXT NOT NULL,
    shape TEXT NOT NULL,
    dtype TEXT NOT NULL,
    nbytes INTEGER NOT NULL
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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    worker TEXT NOT NULL,
    suspect_keys TEXT NOT NULL,
    suspect_chunks TEXT NOT NULL,
    suspect_sites TEXT NOT NULL DEFAULT '[]',
    suspected_oom INTEGER NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deaths_run ON deaths(run_id, timestamp);

CREATE TABLE IF NOT EXISTS task_spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    layer TEXT NOT NULL,
    start REAL NOT NULL,
    end REAL NOT NULL,
    worker TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spans_run_start ON task_spans(run_id, start);

-- One row per (memray epoch, hot source line). ``ts`` is the epoch end; a line
-- appears once per epoch it was live in, so the peak-per-line query takes the
-- MAX(hwm_bytes) across epochs, not a sum (epochs are disjoint time windows).
CREATE TABLE IF NOT EXISTS alloc_sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ts REAL NOT NULL,
    filename TEXT NOT NULL,
    lineno INTEGER NOT NULL,
    function TEXT NOT NULL,
    hwm_bytes INTEGER NOT NULL,
    n_allocations INTEGER NOT NULL,
    task_key TEXT NOT NULL,
    layer TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alloc_run ON alloc_sites(run_id);
CREATE INDEX IF NOT EXISTS idx_alloc_run_task ON alloc_sites(run_id, task_key);

CREATE TABLE IF NOT EXISTS task_memory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    layer TEXT NOT NULL,
    worker TEXT NOT NULL,
    peak_rss_delta INTEGER NOT NULL,
    top_sites TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_taskmem_run ON task_memory(run_id);

CREATE TABLE IF NOT EXISTS worker_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    timestamp REAL NOT NULL,
    rss_bytes INTEGER NOT NULL,
    managed_bytes INTEGER NOT NULL,
    memory_limit INTEGER NOT NULL,
    cpu REAL NOT NULL,
    nthreads INTEGER NOT NULL,
    executing INTEGER NOT NULL,
    ready INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wstatus_run_worker_ts ON worker_status(run_id, worker, timestamp);

-- One row per (epoch, unique call stack): the full root->leaf frames as JSON
-- plus the high-water-mark bytes, for the per-worker flamegraph / tree.
CREATE TABLE IF NOT EXISTS alloc_stacks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ts REAL NOT NULL,
    frames TEXT NOT NULL,
    hwm_bytes INTEGER NOT NULL,
    n_allocations INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stacks_run_worker ON alloc_stacks(run_id, worker);
"""


def make_store(dsn: str | None = None, db: str | Path = "daskgenie.db") -> StoreProtocol:
    """Pick the collector backend. A Postgres/Timescale ``dsn`` (from
    ``DASKGENIE_DSN``) selects :class:`TimescaleStore` — the default in Docker;
    otherwise the self-contained SQLite :class:`Store`, which is also what the
    test suite uses (``:memory:``). psycopg is imported lazily so SQLite-only
    installs don't need it.
    """
    if dsn:
        from daskgenie.collector.store_tsdb import TimescaleStore

        return TimescaleStore(dsn)
    return Store(db)


class Store:
    def __init__(self, path: str | Path = "daskgenie.db") -> None:
        # ":memory:" is honoured for tests; a path persists across restarts.
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            # Migrate DBs created before origin tracking existed.
            for col in ("origin", "origin_ip"):
                try:
                    self._conn.execute(
                        f"ALTER TABLE runs ADD COLUMN {col} TEXT NOT NULL DEFAULT ''"
                    )
                except sqlite3.OperationalError:
                    pass  # column already exists
            self._conn.commit()

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
                "INSERT INTO runs (id, name, created_at, origin, origin_ip) VALUES (?, ?, ?, ?, ?)",
                (run_id, name, created, origin, origin_ip),
            )
            self._conn.commit()
        return RunInfo(id=run_id, name=name, created_at=created, origin=origin, origin_ip=origin_ip)

    def ensure_run(self, run_id: str) -> None:
        """Create a placeholder run row if data arrives for an unknown run_id.

        Keeps ingest robust if a plugin outlives (or races) the run's creation.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO runs (id, name, created_at) VALUES (?, ?, ?)",
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
                counts = {
                    "samples": self._scalar(
                        "SELECT COUNT(*) FROM samples WHERE run_id = ?", r["id"]
                    ),
                    "deaths": self._scalar("SELECT COUNT(*) FROM deaths WHERE run_id = ?", r["id"]),
                    "workers": self._scalar(
                        "SELECT COUNT(DISTINCT worker) FROM samples WHERE run_id = ?", r["id"]
                    ),
                }
                runs.append(
                    RunInfo(
                        id=r["id"],
                        name=r["name"],
                        created_at=r["created_at"],
                        origin=r["origin"],
                        origin_ip=r["origin_ip"],
                        counts=counts,
                    )
                )
        return runs

    def get_run(self, run_id: str) -> RunInfo | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT id, name, created_at, origin, origin_ip FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if r is None:
                return None
            counts = {
                "samples": self._scalar("SELECT COUNT(*) FROM samples WHERE run_id = ?", run_id),
                "deaths": self._scalar("SELECT COUNT(*) FROM deaths WHERE run_id = ?", run_id),
                "workers": self._scalar(
                    "SELECT COUNT(DISTINCT worker) FROM samples WHERE run_id = ?", run_id
                ),
            }
        return RunInfo(
            id=r["id"],
            name=r["name"],
            created_at=r["created_at"],
            origin=r["origin"],
            origin_ip=r["origin_ip"],
            counts=counts,
        )

    def delete_run(self, run_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
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
                self._conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))  # noqa: S608
            self._conn.commit()
        return cur.rowcount > 0

    def _scalar(self, sql: str, *params: object) -> int:
        # caller holds the lock
        return int(self._conn.execute(sql, params).fetchone()[0])

    # -- ingest -------------------------------------------------------------

    def add_samples(self, batch: SampleBatch) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO samples (run_id, worker, timestamp, rss_bytes, managed_bytes, "
                "executing_keys) VALUES (?, ?, ?, ?, ?, ?)",
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
            self._conn.executemany(
                "INSERT INTO chunks (run_id, task_key, shape, dtype, nbytes) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (batch.run_id, c.task_key, json.dumps(list(c.shape)), c.dtype, c.nbytes)
                    for c in batch.chunks
                ],
            )
            self._conn.executemany(
                "INSERT INTO task_spans (run_id, key, layer, start, end, worker) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(batch.run_id, s.key, s.layer, s.start, s.end, s.worker) for s in batch.spans],
            )
            self._conn.executemany(
                "INSERT INTO alloc_sites (run_id, worker, ts, filename, lineno, function, "
                "hwm_bytes, n_allocations, task_key, layer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            self._conn.executemany(
                "INSERT INTO alloc_stacks (run_id, worker, ts, frames, hwm_bytes, n_allocations) "
                "VALUES (?, ?, ?, ?, ?, ?)",
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
            self._conn.executemany(
                "INSERT INTO task_memory (run_id, key, layer, worker, peak_rss_delta, top_sites) "
                "VALUES (?, ?, ?, ?, ?, ?)",
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
            self._conn.executemany(
                "INSERT INTO worker_status (run_id, worker, timestamp, rss_bytes, managed_bytes, "
                "memory_limit, cpu, nthreads, executing, ready) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        with self._lock:
            self._conn.executemany(
                "INSERT INTO graph_layers (run_id, layer, filename, lineno, code_snippet) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id, layer) DO UPDATE SET "
                "filename=excluded.filename, lineno=excluded.lineno, "
                "code_snippet=excluded.code_snippet",
                [
                    (upload.run_id, ly.layer, ly.filename, ly.lineno, ly.code_snippet)
                    for ly in upload.layers
                ],
            )
            self._conn.execute("DELETE FROM graph_deps WHERE run_id = ?", (upload.run_id,))
            self._conn.executemany(
                "INSERT INTO graph_deps (run_id, layer, dep) VALUES (?, ?, ?)",
                [
                    (upload.run_id, layer, dep)
                    for layer, deps in upload.layer_dependencies.items()
                    for dep in deps
                ],
            )
            # Replace any previously-uploaded task graph for this run.
            if upload.nodes or upload.edges or upload.task_count:
                self._conn.execute("DELETE FROM graph_nodes WHERE run_id = ?", (upload.run_id,))
                self._conn.execute("DELETE FROM graph_edges WHERE run_id = ?", (upload.run_id,))
                self._conn.executemany(
                    "INSERT INTO graph_nodes (run_id, key, layer) VALUES (?, ?, ?)",
                    [(upload.run_id, n.key, n.layer) for n in upload.nodes],
                )
                self._conn.executemany(
                    "INSERT INTO graph_edges (run_id, src, dst) VALUES (?, ?, ?)",
                    [(upload.run_id, src, dst) for src, dst in upload.edges],
                )
                self._conn.execute(
                    "INSERT INTO graph_meta (run_id, task_count, truncated) VALUES (?, ?, ?) "
                    "ON CONFLICT(run_id) DO UPDATE SET task_count=excluded.task_count, "
                    "truncated=excluded.truncated",
                    (upload.run_id, upload.task_count, int(upload.truncated)),
                )
            self._conn.commit()

    def add_death(self, event: DeathEvent) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO deaths (run_id, timestamp, worker, suspect_keys, suspect_chunks, "
                "suspect_sites, suspected_oom, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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
                    "FROM samples WHERE run_id = ? ORDER BY timestamp DESC LIMIT ?",
                    (run_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT worker, timestamp, rss_bytes, managed_bytes, executing_keys "
                    "FROM samples WHERE run_id = ? AND worker = ? ORDER BY timestamp DESC LIMIT ?",
                    (run_id, worker, limit),
                ).fetchall()
        return [
            {
                "worker": r["worker"],
                "timestamp": r["timestamp"],
                "rss_bytes": r["rss_bytes"],
                "managed_bytes": r["managed_bytes"],
                "executing_keys": json.loads(r["executing_keys"]),
            }
            for r in rows
        ]

    def chunks_for(self, run_id: str, task_key: str) -> list[ChunkMeta]:
        """Every input chunk recorded for a task — one task can hold several."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_key, shape, dtype, nbytes FROM chunks "
                "WHERE run_id = ? AND task_key = ? ORDER BY nbytes DESC",
                (run_id, task_key),
            ).fetchall()
        return [
            ChunkMeta(
                task_key=r["task_key"],
                shape=tuple(json.loads(r["shape"])),
                dtype=r["dtype"],
                nbytes=r["nbytes"],
            )
            for r in rows
        ]

    def sites_for(self, run_id: str, task_key: str) -> list[AllocationSite]:
        """Deep allocation lines recorded for a task — the peak (MAX) per line
        across the epochs that overlapped it. Feeds the death-site join.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT filename, lineno, function, MAX(hwm_bytes) AS hwm, "
                "SUM(n_allocations) AS na, task_key, layer FROM alloc_sites "
                "WHERE run_id = ? AND task_key = ? "
                "GROUP BY filename, lineno, function ORDER BY hwm DESC",
                (run_id, task_key),
            ).fetchall()
        return [
            AllocationSite(
                filename=r["filename"],
                lineno=r["lineno"],
                function=r["function"],
                hwm_bytes=r["hwm"],
                n_allocations=r["na"] or 0,
                task_key=r["task_key"],
                layer=r["layer"],
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
        """Per-source-line deep memory, peak bytes descending — the headline of
        the deep Memory view. Peak = MAX(hwm_bytes) across epochs (disjoint
        windows), so a line held across time isn't double-counted. A ``start``/
        ``end`` window scopes it to a moment (what was allocating at a spike).
        """
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if start is not None:
            clauses.append("ts >= ?")
            params.append(start)
        if end is not None:
            clauses.append("ts <= ?")
            params.append(end)
        where = " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(
                "SELECT filename, lineno, function, MAX(hwm_bytes) AS hwm, "  # noqa: S608
                "SUM(n_allocations) AS na, "
                f"GROUP_CONCAT(DISTINCT layer) AS layers FROM alloc_sites WHERE {where} "
                "GROUP BY filename, lineno, function ORDER BY hwm DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [
            {
                "filename": r["filename"],
                "lineno": r["lineno"],
                "function": r["function"],
                "hwm_bytes": r["hwm"],
                "n_allocations": r["na"] or 0,
                "layers": [x for x in (r["layers"] or "").split(",") if x],
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
        """Per-unique-call-stack peak bytes for the flamegraph. Peak = MAX across
        epochs (disjoint windows) of the same stack. Optionally scoped to one
        worker and/or a time window (the "over time" selector).
        """
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if worker:
            clauses.append("worker = ?")
            params.append(worker)
        if start is not None:
            clauses.append("ts >= ?")
            params.append(start)
        if end is not None:
            clauses.append("ts <= ?")
            params.append(end)
        where = " AND ".join(clauses)
        with self._lock:
            workers = [
                r[0]
                for r in self._conn.execute(
                    "SELECT DISTINCT worker FROM alloc_stacks WHERE run_id = ? ORDER BY worker",
                    (run_id,),
                ).fetchall()
            ]
            rows = self._conn.execute(
                f"SELECT frames, MAX(hwm_bytes) AS hwm, SUM(n_allocations) AS na "  # noqa: S608
                f"FROM alloc_stacks WHERE {where} GROUP BY frames ORDER BY hwm DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        stacks = [
            {
                "frames": [
                    {"function": f[0], "filename": f[1], "lineno": f[2]}
                    for f in json.loads(r["frames"])
                ],
                "hwm_bytes": r["hwm"],
                "n_allocations": r["na"] or 0,
            }
            for r in rows
        ]
        return {"workers": workers, "stacks": stacks}

    def alloc_timeline(self, run_id: str) -> list[dict[str, Any]]:
        """Per-(epoch, layer) high-water-mark bytes — a memory-over-time series
        grouped by task layer, so you can see which layer's memory grows when.
        Rows with no layer attribution are bucketed as ``(unattributed)``.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, layer, SUM(hwm_bytes) AS bytes FROM alloc_sites "
                "WHERE run_id = ? GROUP BY ts, layer ORDER BY ts",
                (run_id,),
            ).fetchall()
        return [
            {"ts": r["ts"], "layer": r["layer"] or "(unattributed)", "bytes": r["bytes"]}
            for r in rows
        ]

    def task_memory(self, run_id: str, limit: int = 2000) -> list[dict[str, Any]]:
        """Per-task peak RSS delta + dominant allocation lines, largest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, layer, worker, MAX(peak_rss_delta) AS peak, top_sites "
                "FROM task_memory WHERE run_id = ? GROUP BY key "
                "ORDER BY peak DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [
            {
                "key": r["key"],
                "layer": r["layer"],
                "worker": r["worker"],
                "peak_rss_delta": r["peak"],
                "top_sites": json.loads(r["top_sites"]),
            }
            for r in rows
        ]

    def worker_status(self, run_id: str) -> list[dict[str, Any]]:
        """The most recent heartbeat per worker — the live Workers table."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT s.worker, s.timestamp, s.rss_bytes, s.managed_bytes, s.memory_limit, "
                "s.cpu, s.nthreads, s.executing, s.ready FROM worker_status s "
                "JOIN (SELECT worker, MAX(timestamp) AS mt FROM worker_status "
                "WHERE run_id = ? GROUP BY worker) m "
                "ON s.worker = m.worker AND s.timestamp = m.mt WHERE s.run_id = ? "
                "ORDER BY s.worker",
                (run_id, run_id),
            ).fetchall()
        return [dict(r) for r in rows]

    def graph(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            layers = self._conn.execute(
                "SELECT layer, filename, lineno, code_snippet FROM graph_layers WHERE run_id = ?",
                (run_id,),
            ).fetchall()
            deps = self._conn.execute(
                "SELECT layer, dep FROM graph_deps WHERE run_id = ?",
                (run_id,),
            ).fetchall()
            gnodes = self._conn.execute(
                "SELECT key, layer FROM graph_nodes WHERE run_id = ?", (run_id,)
            ).fetchall()
            gedges = self._conn.execute(
                "SELECT src, dst FROM graph_edges WHERE run_id = ?", (run_id,)
            ).fetchall()
            meta = self._conn.execute(
                "SELECT task_count, truncated FROM graph_meta WHERE run_id = ?", (run_id,)
            ).fetchone()
        dep_map: dict[str, list[str]] = {}
        for d in deps:
            dep_map.setdefault(d["layer"], []).append(d["dep"])
        return {
            "run_id": run_id,
            "layers": [dict(r) for r in layers],
            "layer_dependencies": dep_map,
            "nodes": [{"key": n["key"], "layer": n["layer"]} for n in gnodes],
            "edges": [[e["src"], e["dst"]] for e in gedges],
            "task_count": meta["task_count"] if meta else 0,
            "truncated": bool(meta["truncated"]) if meta else False,
        }

    def deaths(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT timestamp, worker, suspect_keys, suspect_chunks, suspect_sites, "
                "suspected_oom, reason FROM deaths WHERE run_id = ? ORDER BY timestamp DESC",
                (run_id,),
            ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "worker": r["worker"],
                "suspect_keys": json.loads(r["suspect_keys"]),
                "suspect_chunks": json.loads(r["suspect_chunks"]),
                "suspect_sites": json.loads(r["suspect_sites"]),
                "suspected_oom": bool(r["suspected_oom"]),
                "reason": r["reason"],
            }
            for r in rows
        ]

    def spans(self, run_id: str, limit: int = 20000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, layer, start, end, worker FROM task_spans "
                "WHERE run_id = ? ORDER BY start LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [
            {
                "key": r["key"],
                "layer": r["layer"],
                "start": r["start"],
                "end": r["end"],
                "worker": r["worker"],
            }
            for r in rows
        ]

    def layer_stats(self, run_id: str) -> list[dict[str, Any]]:
        """Per-layer task counts and total/max execution time — the 'progress'
        breakdown, aggregated in SQL so the payload stays small."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT layer, COUNT(*) AS n, SUM(end - start) AS total, "
                "MAX(end - start) AS longest FROM task_spans WHERE run_id = ? "
                "GROUP BY layer ORDER BY total DESC",
                (run_id,),
            ).fetchall()
        return [
            {
                "layer": r["layer"],
                "count": r["n"],
                "total_seconds": r["total"] or 0.0,
                "longest_seconds": r["longest"] or 0.0,
            }
            for r in rows
        ]

    def latest_memory_by_worker(self) -> dict[str, MemorySample]:
        """Most recent sample per worker across all runs — feeds Prometheus."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT s.worker, s.timestamp, s.rss_bytes, s.managed_bytes, s.executing_keys "
                "FROM samples s JOIN (SELECT worker, MAX(timestamp) AS mt FROM samples "
                "GROUP BY worker) m ON s.worker = m.worker AND s.timestamp = m.mt"
            ).fetchall()
        return {
            r["worker"]: MemorySample(
                timestamp=r["timestamp"],
                rss_bytes=r["rss_bytes"],
                managed_bytes=r["managed_bytes"],
                executing_keys=json.loads(r["executing_keys"]),
            )
            for r in rows
        }
