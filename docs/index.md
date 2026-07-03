# DaskGenie

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

- **Source attribution** — every task-graph layer mapped back to the user source
  line that built it, never into `dask`/`numpy`/`xarray` internals.
- **Deep memory (memray, as a library)** — per-line high-water-mark attribution
  and a per-worker flamegraph / memray-style tree. Opt-in `deep=True`.
- **Worker-death post-mortem** — which chunk, and which line, killed the worker.
- **Real-time dashboard** — a Next.js app streaming over WebSocket: live workers,
  a zoomable task stream, the whole task-graph DAG, memory-over-time with a
  click-to-inspect spike explorer, per-layer allocations over time, and the
  flamegraph.
- **Any scheduler** — distributed plugins or the local-scheduler `LocalProfiler`.
- **Team-friendly** — runs record the machine (hostname + IP) that opened them.
- **Prometheus + TimescaleDB** — `/metrics` for Grafana, TimescaleDB or SQLite.

## Where to next

<div class="grid cards" markdown>

- :material-download: **[Installation](installation.md)** — `pip install daskgenie` and the extras.
- :material-rocket-launch: **[Quick start](quickstart.md)** — bring up the stack and profile a job.
- :material-memory: **[Deep memory](deep-memory.md)** — the memray engine and flamegraph.
- :material-view-dashboard: **[The dashboard](dashboard.md)** — every tab explained.
- :material-cog: **[Configuration](configuration.md)** — environment variables and knobs.
- :material-code-braces: **[API reference](api.md)** — the public Python API.

</div>
