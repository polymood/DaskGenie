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
from typing import Any

from daskgenie.common.schemas import (
    ChunkMeta,
    DeathEvent,
    GraphUpload,
    MemorySample,
    RunInfo,
    SampleBatch,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at REAL NOT NULL
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
    suspected_oom INTEGER NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deaths_run ON deaths(run_id, timestamp);
"""


class Store:
    def __init__(self, path: str | Path = "daskgenie.db") -> None:
        # ":memory:" is honoured for tests; a path persists across restarts.
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- runs ---------------------------------------------------------------

    def create_run(self, name: str = "") -> RunInfo:
        run_id = uuid.uuid4().hex[:12]
        created = time.time()
        name = name or f"run-{run_id}"
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs (id, name, created_at) VALUES (?, ?, ?)",
                (run_id, name, created),
            )
            self._conn.commit()
        return RunInfo(id=run_id, name=name, created_at=created)

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
                "SELECT id, name, created_at FROM runs ORDER BY created_at DESC"
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
                    RunInfo(id=r["id"], name=r["name"], created_at=r["created_at"], counts=counts)
                )
        return runs

    def get_run(self, run_id: str) -> RunInfo | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT id, name, created_at FROM runs WHERE id = ?", (run_id,)
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
        return RunInfo(id=r["id"], name=r["name"], created_at=r["created_at"], counts=counts)

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
                "suspected_oom, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.run_id,
                    event.timestamp,
                    event.worker,
                    json.dumps(event.suspect_keys),
                    json.dumps([c.model_dump() for c in event.suspect_chunks]),
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
                "SELECT timestamp, worker, suspect_keys, suspect_chunks, suspected_oom, "
                "reason FROM deaths WHERE run_id = ? ORDER BY timestamp DESC",
                (run_id,),
            ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "worker": r["worker"],
                "suspect_keys": json.loads(r["suspect_keys"]),
                "suspect_chunks": json.loads(r["suspect_chunks"]),
                "suspected_oom": bool(r["suspected_oom"]),
                "reason": r["reason"],
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
