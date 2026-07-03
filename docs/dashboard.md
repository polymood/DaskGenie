# The dashboard

The dashboard lists every run (with the machine that opened it) and updates live
over a WebSocket while a job runs. Open a run for these tabs:

## Overview
Live stats (workers, tasks, deaths, peak RSS), memory-over-time, and the hottest
allocation line.

## Timeline
A large, zoomable memory-over-time chart. **Click any point** to pin that instant
and see what was running (with source lines) and which lines were allocating in
that window. Below it, a stacked **per-layer allocation timeline**.

Navigation on every chart: scroll to zoom the time axis, drag to pan, shift-drag
to box-zoom.

## Workers
A live, native-Dask-style table: per-worker RSS against the memory limit, managed
memory, CPU, threads, and executing/ready counts.

## Task stream
Task rectangles coloured by layer on a zoomable time axis — an **ALL TASKS**
global lane (all workers packed together) on top and **PER WORKER** lanes below.

## Graph
The real connected task DAG. Small graphs use an interactive view; large ones
render the whole graph on a pan/zoom canvas (no collapsing to a blob). Hover a
node to light up its edges; click for its source line and chunk sizes.

## Memory
The deep view (needs `deep=True`): an allocation **flamegraph** (per-worker,
click to zoom, framework frames dimmed so your code stands out), a **peak bytes
by source line** table, and **peak memory by task**.

## Post-mortem
Each worker death: the allocation lines at the high-water mark, the suspect task,
its source line (syntax highlighted), and the chunk it was holding
(`(4000, 4000) float64 = 128 MB`).

!!! info "OOM is a heuristic"
    The scheduler doesn't tell a plugin *why* a worker left. DaskGenie flags a
    *suspected* OOM only when tasks were in flight at an unexpected removal, and
    never over-claims.
