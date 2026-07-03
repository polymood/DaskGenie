# Examples

Runnable scripts covering the breadth of Dask live in the
[`examples/`](https://github.com/polymood/DaskGenie/tree/develop/examples)
directory.

| Script | Shows |
| --- | --- |
| `graph_source_map.py` | Source attribution only — layer → the line that built it. |
| `local_scheduler.py` | Memory + chunks + graph for a local scheduler. |
| `distributed_oom.py` | A real worker OOM and its post-mortem on `dask.distributed`. |
| `deep_oom.py` | Deep memory (memray): the source line that allocated the OOM array. |
| `big_pipeline_oom.py` | A minutes-long multi-stage pipeline, a large task graph, a real OOM. |
| `threaded_big.py` | Threaded scheduler hoarding memory instead of spilling to Zarr. |
| `threaded_crash.py` | Threaded scheduler that actually crashes (self-limiting `MemoryError`). |
| `dask_delayed.py` | `dask.delayed` custom ETL DAG on a distributed cluster. |
| `dask_dataframe.py` | `dask.dataframe` groupby + shuffle. |
| `dask_bag.py` | `dask.bag` aggregation on the processes scheduler. |
| `xarray_zarr.py` | xarray on Zarr — a chunked cube → anomaly. |
| `xarray_netcdf.py` | xarray on NetCDF (HDF5) — rolling detrend. |

## Running them

Start the stack once, then run an example and open the dashboard:

```bash
docker compose up -d --build

uv run --extra demo --extra deep python examples/deep_oom.py
uv run --extra examples --extra deep python examples/xarray_zarr.py
```

!!! note "Attribution across collections"
    Source-line attribution is strongest for `dask.array` and `dask.delayed`. For
    `dask.dataframe` (dask-expr) and xarray the heavy work runs inside library C
    code, so allocations fold to framework frames — the flamegraph still shows
    the full call tree, and the graph / memory views work throughout.
