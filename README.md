<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/polymood/DaskGenie@develop/web/public/logo.svg" width="88" alt="DaskGenie logo" />
</p>

<h1 align="center">DaskGenie</h1>

<p align="center">
  <a href="https://pypi.org/project/daskgenie/"><img src="https://img.shields.io/pypi/v/daskgenie.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/daskgenie/"><img src="https://img.shields.io/pypi/pyversions/daskgenie.svg" alt="Python versions"></a>
  <a href="https://github.com/polymood/DaskGenie/actions/workflows/ci.yml"><img src="https://github.com/polymood/DaskGenie/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/polymood/DaskGenie/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/polymood/DaskGenie.svg" alt="License"></a>
</p>

A memory profiler and live dashboard for Dask that ties a worker's memory back to
the **line of your code** that caused it.

DaskGenie fuses [memray](https://github.com/bloomberg/memray)-deep allocation
tracing with Dask's task graph and streams it to a real-time dashboard. When a
worker dies from an out-of-memory error, you can see the suspect task, the chunk
it was holding, and the exact source line that allocated the array that killed
it — not just "a worker disappeared". It works on `dask.distributed` and on the
local (threaded / processes / synchronous) schedulers, across `dask.array`,
`dask.dataframe`, `dask.bag`, `dask.delayed`, and xarray on Zarr/NetCDF.

## Features

- **Source attribution.** Maps every Dask task-graph layer back to the user
  source line that built it — the call site in *your* code, never into
  `dask`/`numpy`/`xarray` internals.
- **Deep memory (memray, as a library).** Opt-in per-run allocation tracing,
  epoch-rotated and folded to the first line of your code, so the dashboard shows
  `job.py:42 build = 12.8 GB` and a per-worker flamegraph / memray-style tree —
  you never touch a capture file.
- **Worker-death post-mortem.** A scheduler plugin records the tasks in flight
  when a worker vanishes; the collector joins in the chunk metadata and the
  allocation lines at the high-water mark to answer *which chunk, which line*.
- **Real-time dashboard.** A Next.js app streaming over WebSocket: live worker
  table, a zoomable task stream (global + per-worker), the whole task-graph DAG,
  memory-over-time with a click-to-inspect spike explorer, per-layer allocations
  over time, and the deep flamegraph.
- **Any scheduler.** `register()` installs worker + scheduler plugins on
  `dask.distributed`; `LocalProfiler` hooks the callback API for the threaded,
  processes, and synchronous schedulers.
- **Team-friendly.** Every run records the machine (hostname + IP) that opened
  it, so a shared collector becomes one place to see everyone's runs.
- **Prometheus + TimescaleDB.** The collector exposes `/metrics` for Grafana and
  stores to TimescaleDB (or self-contained SQLite for local use / tests).

## Installation

```bash
pip install daskgenie
```

Optional extras:

```bash
pip install 'daskgenie[deep]'       # memray-backed deep memory profiling
pip install 'daskgenie[collector]'  # the FastAPI collector service (server side)
pip install 'daskgenie[examples]'   # xarray, zarr, pandas, ... for the examples
```

DaskGenie requires Python 3.11 or newer. Deep memory profiling needs memray
(Linux/macOS + CPython); where it isn't importable the profiler silently
degrades to lightweight RSS/managed-memory sampling.

## Quick start

Bring up the dashboard stack — the **collector** (API + `/metrics` + WebSocket),
**TimescaleDB**, and the **Next.js dashboard** — with Docker:

```bash
docker compose up -d --build
# dashboard → http://localhost:3000      collector API → http://localhost:8765
```

Then point a job at the collector. Each `register()` opens a **run** that appears
live on the dashboard:

```python
from distributed import Client, LocalCluster
import daskgenie as dg
import daskgenie.client as genie

client = Client(LocalCluster(processes=True))
run_id = genie.register(client, "http://localhost:8765", run_name="nightly ETL", deep=True)

with dg.track() as source_map:
    result = build_pipeline()                 # your dask work
genie.upload_graph("http://localhost:8765", run_id, source_map, collection=result)

result.compute()
```

Open the run and explore it as it runs:

- **Overview** — live stats, memory-over-time, the hottest allocation line.
- **Timeline** — a large, zoomable memory chart; click any point to see what was
  running and which source lines were allocating at that instant, plus a stacked
  per-layer allocation timeline.
- **Workers** — a live, native-Dask-style table (RSS vs limit, CPU, threads,
  executing/ready).
- **Task stream** — global + per-worker task lanes on a zoomable time axis.
- **Graph** — the real connected task DAG (canvas for large graphs), coloured by
  layer, death-suspect nodes highlighted; click a node for its source and chunks.
- **Memory** — the deep view: allocation flamegraph, peak bytes by source line,
  and peak memory by task.
- **Post-mortem** — each worker death: the allocation lines at the high-water
  mark, the suspect task, its source line, and the chunk it was holding.

## Local schedulers

For the non-distributed schedulers (`.compute(scheduler="threads"|"processes"|
"synchronous")`, the default for bare dask collections) use `LocalProfiler`,
which hooks Dask's callback API instead of installing cluster plugins:

```python
import daskgenie as dg

with dg.track() as source_map:
    result = build_pipeline()

with dg.LocalProfiler("http://localhost:8765", run_name="threaded job",
                      source_map=source_map, collection=result, deep=True) as prof:
    result.compute(scheduler="threads")
# prof.run_id shows up in the dashboard like any other run
```

## The post-mortem: which chunk killed this worker

With `register()` active, a scheduler plugin watches for worker deaths. On a
death it records the tasks that were in flight; the collector joins in the chunk
metadata the worker had already reported and (with `deep=True`) the allocation
lines at the high-water mark. See it end to end on a `LocalCluster` whose worker
is really OOM-killed:

```bash
docker compose up -d --build
uv run --extra demo --extra deep python examples/deep_oom.py
```

OOM vs. clean shutdown is a heuristic: the scheduler doesn't tell a plugin *why*
a worker left, so DaskGenie flags a *suspected* OOM only when tasks were in
flight at an unexpected removal, and never over-claims.

## Configuration

Everything is configured through environment variables:

| Variable | Component | Purpose |
| --- | --- | --- |
| `DASKGENIE_DSN` | collector | Postgres/TimescaleDB DSN; selects the Timescale backend (default in Docker). |
| `DASKGENIE_DB` | collector | SQLite path (or `:memory:`) when no DSN is set. |
| `DASKGENIE_HOST` / `DASKGENIE_PORT` | collector | Bind address (default `127.0.0.1:8765`). |
| `COLLECTOR_URL` | dashboard | Where the Next.js server proxies `/api` (e.g. `http://collector:8765`). |
| `NEXT_PUBLIC_COLLECTOR_WS` | dashboard | WebSocket base the browser connects to (default `ws://<host>:8765`). |

The `register()` and `LocalProfiler` calls take `deep=`, `sample_interval=`,
`flush_interval=`, and `deep_epoch_seconds=` to trade overhead for resolution.

## Notes and limitations

- **Source attribution** is strongest for `dask.array` and `dask.delayed`. For
  `dask.dataframe` (which builds graphs through dask-expr) and xarray, the heavy
  work runs inside library/C code, so per-line allocations fold to framework
  frames — the graph, memory, per-layer and flamegraph views still work, but
  layer→line mapping is sparse there.
- **memray** is Linux/macOS + CPython only, and a single tracker runs per
  process; deep mode is opt-in and costs roughly 1.5–2× runtime.
- On a **hard OOM kill** the worker's in-flight memray epoch can die before it
  flushes, so a specific post-mortem's line attribution is best-effort — the
  Memory tab remains the reliable place to see the culprit line.
- The **OOM label** is a heuristic, not a certainty (see the post-mortem note).
- **TimescaleDB** is the default store in Docker; SQLite is the zero-setup
  backend used locally and by the test suite.

## Examples

Runnable scripts covering the breadth of Dask live in
[`examples/`](./examples) — distributed OOMs, the deep-memory demo, a big
minutes-long pipeline, a self-limiting crash, and one per collection type
(`dask.delayed`, `dask.dataframe`, `dask.bag`, xarray on Zarr and NetCDF). See
[`examples/README.md`](./examples/README.md).

## Development

```bash
uv sync --group dev --extra collector --extra deep
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy src/

# collector + dashboard separately (dashboard proxies /api to the collector)
uv run python -m daskgenie.collector --port 8765          # terminal 1
cd web && npm install && npm run dev                      # terminal 2 → :3000
```

## How source attribution works

`HighLevelGraph.from_collections` is the one classmethod almost every Dask
collection operation calls to register a new named graph layer. `track()`
patches it for the duration of the `with` block: each time a new layer appears,
it walks the call stack outward until it finds the first frame that isn't inside
`dask`/`distributed`/`xarray`/`numpy`/`zarr`/`daskgenie` or a `site-packages`
install — that's your call site. The deep memory engine reuses the same
library-path filter to fold memray stacks to the first user frame. The
library-path filter is configurable via `track(extra_library_paths=[...])`.

## License

MIT — see [LICENSE](./LICENSE).
