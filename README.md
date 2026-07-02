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
- [ ] React UI (post-mortem view, aligned execution-ordered view, graph
      heatmap).

## Quickstart (GraphCapture)

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync --group dev
uv run python demo/oom_pipeline.py
```

This builds a pipeline shaped like a real xarray-on-Zarr workload (chunked
read → rechunk-merge → per-block op) and prints the layer → source-location
map `GraphCapture` recovered from it:

```
layer                                         source
----------------------------------------------------------------------------------------------------
random_sample-...                             demo/oom_pipeline.py:24  return da.random.random(...)
rechunk-merge-...                              demo/oom_pipeline.py:32  return arr.rechunk((8000, 8000))
double_precision_blowup-...                    demo/oom_pipeline.py:48  z = y.map_blocks(...)
```

Each layer name is the exact key Dask uses internally; each source location
points at the *user's* call site, not into `dask`/`numpy`/`xarray` internals.

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

genie.register(client, "http://127.0.0.1:8765")   # per-task memory sampling

with dg.track() as layer_map:
    result = build_pipeline()            # your xarray-on-Zarr work
genie.upload_graph("http://127.0.0.1:8765", "run-1", layer_map)

result.compute()
```

Read it back with plain HTTP:

```bash
curl 'http://127.0.0.1:8765/api/timeline'      # per-worker memory over time
curl 'http://127.0.0.1:8765/api/chunks/<task-key>'   # (shape, dtype, nbytes)
curl 'http://127.0.0.1:8765/api/deaths'        # worker-death post-mortems
curl 'http://127.0.0.1:8765/metrics'           # Prometheus: point Grafana here
```

The `/metrics` endpoint exposes per-worker RSS and managed-memory gauges plus
sample/death counters, so existing Grafana setups get value without the custom
UI. Every payload between the plugins and collector is a versioned pydantic
model (`daskgenie.common.schemas`); the plugins never import collector code.

## The post-mortem: which chunk killed this worker

When `register()` is active, a scheduler plugin watches for worker deaths. On a
death it records the tasks that were in flight on that worker as suspects, and
the collector joins in the chunk metadata the worker had already reported — so
`/api/deaths` tells you the worker, the suspect task, the chunk it was holding,
and (via the source map) the line that produced it.

See it end to end on a LocalCluster whose worker is really OOM-killed:

```bash
uv run --extra collector --extra demo python demo/oom_death.py
```

```
POST-MORTEM: which chunk killed which worker, and what code produced it
==============================================================================
worker died: tcp://127.0.0.1:39119
reason:      abrupt removal with 2 task(s) in flight (suspected OOM, ...)

  in-flight task: ('sum-c9daa81c...', 1, 1)
  source line:    demo/oom_death.py:48  return persisted.map_blocks(blowup, ...).sum()
  chunk held:     (4000, 4000) float64 = 128 MB
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
