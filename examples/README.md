# Examples

| Script | Needs | Shows |
| --- | --- | --- |
| `graph_source_map.py` | nothing | Source attribution only — layer → the line that built it. |
| `local_scheduler.py` | running collector | Memory + chunks + graph for a **local** scheduler (threaded/synchronous/processes). |
| `distributed_oom.py` | running collector | A real worker OOM and its post-mortem on `dask.distributed`. |
| `deep_oom.py` | running collector + `deep` extra | Deep memory (memray): the **source line** that allocated the OOM array, live Workers/Task-stream/Memory tabs. |
| `big_pipeline_oom.py` | running collector + `deep` extra | A realistic **minutes-long** multi-stage pipeline (FFT + matmul), a **large task graph** (~4.5k nodes), streamed live, ending in a real worker OOM the Memory tab attributes to `upsample_block`. |
| `threaded_big.py` | running collector + `deep` extra | **Threaded scheduler** (no workers): a pipeline that *hoards* every result in memory instead of spilling to Zarr — the RSS staircase + the accumulation line, the "should have gone to disk" signature. |
| `threaded_crash.py` | running collector + `deep` extra | Threaded scheduler that **actually crashes** (self-limiting `MemoryError`): the memory curve climbing to the wall and the line that got it there. |
| `dask_delayed.py` | running collector + `examples` + `deep` extras | **dask.delayed** custom ETL DAG on a distributed cluster — per-line memory on `transform_shard`. |
| `dask_dataframe.py` | running collector + `examples` + `deep` extras | **dask.dataframe** groupby-aggregation + shuffle (`set_index` rolling) on a synthetic timeseries. |
| `dask_bag.py` | running collector + `examples` + `deep` extras | **dask.bag** record/JSON aggregation on the **processes** scheduler (per-process memory). |
| `xarray_zarr.py` | running collector + `examples` + `deep` extras | **xarray on Zarr**: a chunked cube streamed from a Zarr store → climatology + anomaly (bounded memory, the read-don't-hoard pattern). |
| `xarray_netcdf.py` | running collector + `examples` + `deep` extras | **xarray on NetCDF** (HDF5/h5netcdf): open with Dask chunks, rolling-mean detrend + reduce. |

## Setup

The first example is standalone. The other two report to a running collector —
start the stack once:

```bash
docker compose up -d --build          # collector :8765, dashboard :3000
```

Then run an example and open the dashboard at http://localhost:3000:

```bash
uv run --extra demo python examples/local_scheduler.py
uv run --extra demo python examples/distributed_oom.py
uv run --extra demo --extra deep python examples/deep_oom.py
uv run --extra demo --extra deep python examples/big_pipeline_oom.py   # ~2-4 min, big graph, real OOM
uv run --extra demo --extra deep python examples/threaded_big.py       # threaded, memory-hoard demo

# Across Dask's collections (need the `examples` extra: xarray, zarr, pandas, ...)
uv run --extra examples --extra deep python examples/dask_delayed.py
uv run --extra examples --extra deep python examples/dask_dataframe.py
uv run --extra examples --extra deep python examples/dask_bag.py
uv run --extra examples --extra deep python examples/xarray_zarr.py
uv run --extra examples --extra deep python examples/xarray_netcdf.py
```

> Source-line attribution is strongest for `dask.array` and `dask.delayed`. For
> `dask.dataframe` (dask-expr) and xarray the heavy work runs inside library C
> code, so allocations fold to framework frames — the **flamegraph** still shows
> the full call tree, and the graph/memory views work throughout.

(No Docker? Run the collector directly with
`uv run python -m daskgenie.collector --port 8765` and the dashboard dev server
from `web/` — see the top-level README.)
