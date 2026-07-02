# DaskGenie

A code-to-graph-to-memory profiler for Dask: when a worker dies from an OOM,
know exactly which chunk killed it and which line of your code produced it.

Built incrementally, each stage proven before the next is built. See
**Status** below for what's real today.

## Status

- [x] **GraphCapture.** Maps Dask task-graph layers back to the user source
      line that built them. No UI. This is the de-risking step: prove source
      attribution works before building anything on top of it.
- [x] **WorkerPlugin + Collector.** Per-task RSS / managed-memory sampling
      with input chunk metadata, batched to a FastAPI collector that stores to
      SQLite, exposes Prometheus `/metrics`, and serves a query API.
- [x] **SchedulerPlugin — the v1 success criterion.** Tracks which tasks are
      in flight on which worker and, on a worker death, records the suspect
      tasks; the collector joins their chunk metadata so `curl /api/deaths`
      answers *"which chunk killed this worker, and what code produced it."*
- [x] **Dashboard (Next.js) + Docker.** A standalone dashboard (light theme)
      over the collector API: runs list, per-run overview, post-mortem with
      syntax-highlighted source, memory timeline, and the task-graph DAG.
      `docker compose up` runs collector + dashboard, persisted on a volume.
- [x] **Any scheduler.** `LocalProfiler` (Dask callback API) profiles memory +
      per-task chunks on the threaded/synchronous/processes schedulers, not
      just `dask.distributed`. Runnable `examples/`.
- [ ] Next: aligned execution-ordered view, memory flamegraphs, on-demand
      single-task memray, TimescaleDB backend.

## The dashboard (always-on, via Docker)

Two services — the **collector** (JSON API + Prometheus `/metrics` + SQLite,
port 8765) and the **Next.js dashboard** (port 3000) — both persisted and
restart-on-failure:

```bash
docker compose up -d --build
# dashboard → http://localhost:3000      collector API → http://localhost:8765
```

Then point any job at the collector and profile — each `register()` opens a
**run** that appears on the dashboard:

```python
from distributed import Client, LocalCluster
import daskgenie as dg
import daskgenie.client as genie

client = Client(LocalCluster(processes=True))
run_id = genie.register(client, "http://localhost:8765", run_name="nightly ETL")

with dg.track() as source_map:
    result = build_pipeline()
genie.upload_graph("http://localhost:8765", run_id, source_map)
result.compute()
```

The dashboard lists every run (worker/sample/death counts, one-click delete).
Open a run for:

- **Overview** — headline stats and the memory-over-time chart.
- **Post-mortem** — each worker death: suspect task, source line (syntax
  highlighted), and the chunk it was holding (`(4000, 4000) float64 = 128 MB`).
- **Memory** — per-worker RSS timeline.
- **Task graph** — the layer DAG, with nodes that were in flight at a death
  highlighted.

### Developing the dashboard

The dashboard is a Next.js (App Router, TypeScript) app in `web/`. Run the
collector and the dev server separately; the dashboard proxies `/api` to the
collector (`COLLECTOR_URL`, default `http://127.0.0.1:8765`):

```bash
uv run python -m daskgenie.collector --port 8765          # terminal 1
cd web && npm install && npm run dev                      # terminal 2 → http://localhost:3000
```

## Quickstart (GraphCapture)

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync --group dev
uv run --extra demo python examples/graph_source_map.py
```

This prints the layer → source-location map `GraphCapture` recovered from a
pipeline (chunked read → rechunk-merge → per-block op):

```
layer                                         source
----------------------------------------------------------------------------------------------------
random_sample-...                             examples/graph_source_map.py:15  x = da.random.random(...)
rechunk-merge-...                             examples/graph_source_map.py:16  y = x.rechunk((8000, 8000))
lambda-...                                    examples/graph_source_map.py:17  return y.map_blocks(...).sum()
```

Each layer name is the exact key Dask uses internally; each source location
points at the *user's* call site, not into `dask`/`numpy`/`xarray` internals.
See [`examples/`](./examples) for the full set of runnable examples.

## Any scheduler, not just distributed

On `dask.distributed`, `register()` installs worker + scheduler plugins. For the
**local** schedulers (`scheduler="threads"|"synchronous"|"processes"` — the
default for bare dask arrays/dataframes) use `LocalProfiler`, which hooks Dask's
callback API to sample memory and per-task output chunks:

```python
import daskgenie as dg

with dg.track() as source_map:
    result = build_pipeline()

with dg.LocalProfiler("http://localhost:8765", run_name="threaded job",
                      source_map=source_map) as prof:
    result.compute(scheduler="threads")
# prof.run_id shows up in the dashboard like any other run
```

### Using it in your own code

```python
import daskgenie as dg

with dg.track() as layer_map:
    ds = open_dataset(...)          # your pipeline
    result = ds.rechunk(...).compute()

for layer_name, loc in layer_map.items():
    print(layer_name, "->", f"{loc.filename}:{loc.lineno}", loc.code_snippet)
```

Or decorate a function you want tracked instead of wrapping a block:

```python
@dg.watch
def build_pipeline():
    ...
```

Results from `track()` and `@watch` accumulate into the same map; read it
anytime with `dg.get_layer_map()`.

## Profiling a live cluster (WorkerPlugin + Collector)

Start the collector (SQLite-backed, serves ingest + `/metrics` + query API):

```bash
uv sync --group dev --extra collector
uv run python -m daskgenie.collector --port 8765          # --db PATH to persist
```

Then, in your job, install the profiler plugin on the cluster and (optionally)
push the source map so memory lines up with the code that produced it:

```python
from distributed import Client, LocalCluster
import daskgenie as dg
import daskgenie.client as genie

cluster = LocalCluster(n_workers=4, processes=True)
client = Client(cluster)

run_id = genie.register(client, "http://127.0.0.1:8765")   # opens a run

with dg.track() as layer_map:
    result = build_pipeline()            # your xarray-on-Zarr work
genie.upload_graph("http://127.0.0.1:8765", run_id, layer_map)

result.compute()
```

Read it back with plain HTTP (everything is scoped to the `run_id`):

```bash
curl 'http://127.0.0.1:8765/api/runs'                       # list runs
curl "http://127.0.0.1:8765/api/runs/$RUN/timeline"         # memory over time
curl "http://127.0.0.1:8765/api/runs/$RUN/deaths"           # worker-death post-mortems
curl 'http://127.0.0.1:8765/metrics'                        # Prometheus: point Grafana here
```

The `/metrics` endpoint exposes per-worker RSS and managed-memory gauges plus
sample/death counters, so existing Grafana setups get value without the custom
UI. Every payload between the plugins and collector is a versioned pydantic
model (`daskgenie.common.schemas`); the plugins never import collector code.

## The post-mortem: which chunk killed this worker

When `register()` is active, a scheduler plugin watches for worker deaths. On a
death it records the tasks that were in flight on that worker as suspects, and
the collector joins in the chunk metadata the worker had already reported — so
the post-mortem tells you the worker, the suspect task, the chunk it was
holding, and (via the source map) the line that produced it.

See it end to end on a LocalCluster whose worker is really OOM-killed, then open
the run's **Post-mortem** tab in the dashboard:

```bash
docker compose up -d --build
uv run --extra demo python examples/distributed_oom.py
```

OOM vs. clean shutdown is a heuristic, not a certainty: the scheduler doesn't
tell a plugin *why* a worker left, so DaskGenie flags a *suspected* OOM only
when tasks were in flight at an unexpected removal, and never over-claims.

## Development

```bash
uv sync --group dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy src/
```

## How source attribution works

`HighLevelGraph.from_collections` is the one classmethod almost every Dask
collection operation (array, dataframe, and by extension xarray, since it
wraps dask arrays) calls to register a new named graph layer. `track()`
patches it for the duration of the `with` block: each time a new layer name
appears, it walks the call stack outward until it finds the first frame that
isn't inside `dask`/`distributed`/`xarray`/`numpy`/`zarr`/`daskgenie` or a
`site-packages` install — that's the user's call site. The library-path
filter is configurable via `track(extra_library_paths=[...])` for teams with
their own internal wrapper libraries.
