# Examples

| Script | Needs | Shows |
| --- | --- | --- |
| `graph_source_map.py` | nothing | Source attribution only — layer → the line that built it. |
| `local_scheduler.py` | running collector | Memory + chunks + graph for a **local** scheduler (threaded/synchronous/processes). |
| `distributed_oom.py` | running collector | A real worker OOM and its post-mortem on `dask.distributed`. |

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
```

(No Docker? Run the collector directly with
`uv run python -m daskgenie.collector --port 8765` and the dashboard dev server
from `web/` — see the top-level README.)
