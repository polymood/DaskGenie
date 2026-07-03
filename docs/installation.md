# Installation

```bash
pip install daskgenie
```

DaskGenie requires **Python 3.11 or newer**.

## Optional extras

```bash
pip install 'daskgenie[deep]'       # memray-backed deep memory profiling
pip install 'daskgenie[collector]'  # the FastAPI collector service (server side)
pip install 'daskgenie[examples]'   # xarray, zarr, pandas, ... for the examples
```

| Extra | Pulls in | For |
| --- | --- | --- |
| `deep` | `memray` | Line-level allocation tracing and the flamegraph. |
| `collector` | `fastapi`, `uvicorn`, `psycopg`, `websockets`, ... | Running the collector service yourself (usually you run it via Docker instead). |
| `examples` | `xarray`, `zarr`, `pandas`, `pyarrow`, `h5netcdf`, ... | The runnable example scripts. |
| `demo` | `distributed`, `numpy` | The minimal distributed demos. |

!!! note "memray platform support"
    Deep memory profiling needs memray, which is **Linux/macOS + CPython** only.
    Where memray can't be imported, the profiler silently degrades to lightweight
    RSS / managed-memory sampling — you still get memory-over-time and per-chunk
    metadata, just not per-line attribution.

## The dashboard stack

The collector, TimescaleDB, and the web dashboard run as containers — you don't
pip-install them. See [Quick start](quickstart.md):

```bash
docker compose up -d --build
```
