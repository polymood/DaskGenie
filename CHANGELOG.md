# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.2] - 2026-07-06

First cleanly-published release. (`0.1.0` was an early upload and `0.1.1` a failed
publish before the version was bumped; `0.1.2` is the stable packaging of the
features below.)

### Added

- **Deep memory profiling** with memray driven as a library: per-source-line
  high-water-mark attribution and full call stacks for a per-worker flamegraph /
  memray-style tree. Opt-in `deep=True`; degrades to lightweight sampling where
  memray isn't available.
- **Real-time dashboard** (Next.js) streaming over WebSocket: live Workers table,
  global + per-worker task stream, whole-graph canvas DAG, memory-over-time with
  a click-to-inspect spike explorer, per-layer allocations over time, and the
  deep flamegraph. Collapsible sidebar, in-app modals, Dask warm palette + logo.
- **TimescaleDB** backend (hypertables) as the default collector store behind a
  `StoreProtocol`, with SQLite kept for local use and tests. Prometheus
  `/metrics` retained.
- **Worker-death post-mortem** joining suspect tasks with chunk metadata and the
  allocation lines at the high-water mark.
- **Origin tracking** — each run records the client hostname and IP.
- **Examples** across Dask: distributed/deep OOM demos, a minutes-long pipeline,
  a self-limiting crash, and one per collection type (`dask.delayed`,
  `dask.dataframe`, `dask.bag`, xarray on Zarr and NetCDF).
- Packaging (PyPI metadata, MIT license), CI, and release workflows.

[Unreleased]: https://github.com/polymood/DaskGenie/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/polymood/DaskGenie/releases/tag/v0.1.2
