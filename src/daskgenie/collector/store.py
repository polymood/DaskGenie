"""SQLite-backed event store.

SQLite by default keeps the tool self-contained — no server to stand up, one
file to hand someone. The public methods here are the whole storage contract;
a TimescaleDB backend (spec stretch goal) would reimplement this same surface.

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
from pathlib import Path

from daskgenie.common.schemas import (
    ChunkMeta,
    DeathEvent,
    GraphUpload,
    MemorySample,
    SampleBatch,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker TEXT NOT NULL,
    timestamp REAL NOT NULL,
    rss_bytes INTEGER NOT NULL,
    managed_bytes INTEGER NOT NULL,
    executing_keys TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_worker_ts ON samples(worker, timestamp);

-- One row per (consuming task, input chunk): a task can hold several inputs,
-- and worker-side dedup already prevents duplicates, so no unique constraint.
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key TEXT NOT NULL,
    shape TEXT NOT NULL,
    dtype TEXT NOT NULL,
    nbytes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_task_key ON chunks(task_key);

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

CREATE TABLE IF NOT EXISTS deaths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    worker TEXT NOT NULL,
    suspect_keys TEXT NOT NULL,
    suspect_chunks TEXT NOT NULL,
    suspected_oom INTEGER NOT NULL,
    reason TEXT NOT NULL
);
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

    # -- ingest -------------------------------------------------------------

    def add_samples(self, batch: SampleBatch) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO samples (worker, timestamp, rss_bytes, managed_bytes, "
                "executing_keys) VALUES (?, ?, ?, ?, ?)",
                [
                    (
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
                "INSERT INTO chunks (task_key, shape, dtype, nbytes) VALUES (?, ?, ?, ?)",
                [(c.task_key, json.dumps(list(c.shape)), c.dtype, c.nbytes) for c in batch.chunks],
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
            self._conn.commit()

    def add_death(self, event: DeathEvent) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO deaths (timestamp, worker, suspect_keys, suspect_chunks, "
                "suspected_oom, reason) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.timestamp,
                    event.worker,
                    json.dumps(event.suspect_keys),
                    json.dumps([c.model_dump() for c in event.suspect_chunks]),
                    int(event.suspected_oom),
                    event.reason,
                ),
            )
            self._conn.commit()

    # -- query --------------------------------------------------------------

    def timeline(self, worker: str | None = None, limit: int = 10000) -> list[dict[str, object]]:
        with self._lock:
            if worker is None:
                rows = self._conn.execute(
                    "SELECT worker, timestamp, rss_bytes, managed_bytes, executing_keys "
                    "FROM samples ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT worker, timestamp, rss_bytes, managed_bytes, executing_keys "
                    "FROM samples WHERE worker = ? ORDER BY timestamp DESC LIMIT ?",
                    (worker, limit),
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

    def chunks_for(self, task_key: str) -> list[ChunkMeta]:
        """Every input chunk recorded for a task — one task can hold several."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_key, shape, dtype, nbytes FROM chunks WHERE task_key = ? "
                "ORDER BY nbytes DESC",
                (task_key,),
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

    def graph(self, run_id: str) -> dict[str, object]:
        with self._lock:
            layers = self._conn.execute(
                "SELECT layer, filename, lineno, code_snippet FROM graph_layers WHERE run_id = ?",
                (run_id,),
            ).fetchall()
            deps = self._conn.execute(
                "SELECT layer, dep FROM graph_deps WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        dep_map: dict[str, list[str]] = {}
        for d in deps:
            dep_map.setdefault(d["layer"], []).append(d["dep"])
        return {
            "run_id": run_id,
            "layers": [dict(r) for r in layers],
            "layer_dependencies": dep_map,
        }

    def deaths(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT timestamp, worker, suspect_keys, suspect_chunks, suspected_oom, "
                "reason FROM deaths ORDER BY timestamp DESC"
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
        """Most recent sample per worker — feeds the Prometheus gauges."""
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
