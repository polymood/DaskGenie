# Quick start

## 1. Bring up the dashboard stack

The **collector** (API + `/metrics` + WebSocket), **TimescaleDB**, and the
**Next.js dashboard** run with Docker:

```bash
docker compose up -d --build
# dashboard → http://localhost:3000      collector API → http://localhost:8765
```

## 2. Point a job at the collector

Each `register()` opens a **run** that appears live on the dashboard:

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

- `register()` installs the worker + scheduler plugins and returns the `run_id`.
- `dg.track()` captures the map from graph layers to the source lines that built
  them.
- `upload_graph(..., collection=result)` sends the source map **and** the concrete
  task graph so the dashboard can draw it.
- `deep=True` turns on the memray engine (needs the `deep` extra).

Open [http://localhost:3000](http://localhost:3000), pick the run, and explore it
as it runs. See [The dashboard](dashboard.md) for every tab.

## Local schedulers

Not on `dask.distributed`? Use [`LocalProfiler`](local-schedulers.md) for the
threaded / processes / synchronous schedulers.

## Seeing a real OOM

```bash
docker compose up -d --build
uv run --extra demo --extra deep python examples/deep_oom.py
```

Then open the run's **Memory** and **Post-mortem** tabs — DaskGenie names the
source line that allocated the array that killed the worker.
