# Deep memory (memray)

Pass `deep=True` to profile *inside* the memory. On each worker DaskGenie drives
[memray](https://github.com/bloomberg/memray) **as a library** — a short capture
rotated every few seconds, read back and folded to the first line of *your* code
responsible for each high-water-mark allocation. You never see or handle a
capture file; only the aggregates are pushed.

```python
# distributed
run_id = genie.register(client, "http://localhost:8765", deep=True)

# local schedulers
with dg.LocalProfiler(url, source_map=smap, collection=result, deep=True) as prof:
    result.compute(scheduler="threads")
```

Needs the `deep` extra (`pip install 'daskgenie[deep]'`). Deep mode costs roughly
1.5–2× runtime, so it is opt-in per run.

## What you get

- **Peak bytes by source line** — `job.py:42 build = 12.8 GB`.
- **A per-worker flamegraph** — the full call stack (root → leaf) for each
  allocation, aggregated into an icicle you can zoom into; framework frames are
  dimmed so your code stands out. This is the memray "tree" read, on your code.
- **Per-layer allocations over time** and **per-task peak memory**.
- On a worker death, the allocation lines at the **high-water mark** in the
  post-mortem.

## How it works

The engine runs a background thread of **epochs**: a memray `Tracker` captures to
a throwaway temp file for a few seconds, then stops, is read back with
`FileReader`, folded to the high-water-mark bytes per user source line (and full
call stacks for the flamegraph), and the file is deleted before the next epoch
starts. Rotation keeps each capture tiny and makes attribution time-resolved.
Everything is guarded — a memray failure degrades to Tier-1 sampling and never
crashes or OOMs the job.

## Limitations

- **memray** is Linux/macOS + CPython only; a single tracker runs per process.
- Source-line attribution is strongest for `dask.array` and `dask.delayed`. For
  `dask.dataframe` (dask-expr) and xarray the heavy work runs inside library/C
  code, so per-line allocations fold to framework frames — the flamegraph still
  shows the full tree.
- On a **hard OOM kill** the in-flight epoch can die before it flushes, so a
  specific post-mortem's line attribution is best-effort. The **Memory** tab
  remains the reliable place to see the culprit line.
