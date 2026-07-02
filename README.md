# DaskGenie

A code-to-graph-to-memory profiler for Dask: when a worker dies from an OOM,
know exactly which chunk killed it and which line of your code produced it.

Built incrementally, each stage proven before the next is built. See
**Status** below for what's real today.

## Status

- [x] **GraphCapture.** Maps Dask task-graph layers back to the user source
      line that built them. No UI. This is the de-risking step: prove source
      attribution works before building anything on top of it.
- [ ] WorkerPlugin + Collector (memory sampling, chunk metadata, `/metrics`,
      SQLite).
- [ ] SchedulerPlugin (worker-death attribution — the v1 success criterion:
      "which chunk killed this worker").
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
