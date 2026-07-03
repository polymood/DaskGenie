# Local schedulers

The distributed worker/scheduler plugins only exist on `dask.distributed`. For
the local schedulers (`.compute(scheduler="threads"|"processes"|"synchronous")`,
the default for bare dask collections) use `LocalProfiler`, which hooks Dask's
callback API instead of installing cluster plugins.

```python
import daskgenie as dg

with dg.track() as source_map:
    result = build_pipeline()

with dg.LocalProfiler(
    "http://localhost:8765",
    run_name="threaded job",
    source_map=source_map,
    collection=result,
    deep=True,
) as prof:
    result.compute(scheduler="threads")

# prof.run_id shows up in the dashboard like any other run
```

`LocalProfiler` samples process RSS over time, tags each sample with the tasks
running at that instant, records the shape/dtype/nbytes of each task's output
chunk, and — with `deep=True` — runs the same memray engine as the distributed
path. There is no worker-death attribution here: a local scheduler has no workers
to lose, so an OOM simply kills the process (the memory-over-time curve up to that
point is still captured, flushed once per second).

See [`examples/xarray_zarr.py`](examples.md) and the other threaded examples.
