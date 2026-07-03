# Configuration

## Environment variables

| Variable | Component | Purpose |
| --- | --- | --- |
| `DASKGENIE_DSN` | collector | Postgres/TimescaleDB DSN; selects the Timescale backend (default in Docker). |
| `DASKGENIE_DB` | collector | SQLite path (or `:memory:`) when no DSN is set. |
| `DASKGENIE_HOST` / `DASKGENIE_PORT` | collector | Bind address (default `127.0.0.1:8765`). |
| `COLLECTOR_URL` | dashboard | Where the Next.js server proxies `/api` (e.g. `http://collector:8765`). |
| `NEXT_PUBLIC_COLLECTOR_WS` | dashboard | WebSocket base the browser connects to (default `ws://<host>:8765`). |

## Profiler knobs

`register()` and `LocalProfiler` accept:

| Argument | Default | Effect |
| --- | --- | --- |
| `deep` | `False` | Enable the memray deep-memory engine. |
| `sample_interval` | `0.2` | Seconds between RSS/status samples. |
| `flush_interval` | `0.5` | Seconds between pushes to the collector. Keep it well under how fast a worker OOMs so the killer chunk's metadata is sent before the process dies. |
| `deep_epoch_seconds` | `5.0` | memray rotation window; smaller = finer time resolution, more overhead. |

## Storage backend

The collector defaults to **TimescaleDB** in Docker (set by `DASKGENIE_DSN` in
`docker-compose.yml`); without a DSN it uses the self-contained SQLite store —
which is also what the test suite runs on. Point it at any Postgres/Timescale
instance with `DASKGENIE_DSN=postgresql://user:pass@host:5432/db`.

## Prometheus

The collector exposes `/metrics` with per-worker RSS and managed-memory gauges
plus sample/death counters, so existing Grafana setups get value without the
custom UI.
