"""Profile an **xarray-on-Zarr** pipeline (threaded scheduler).

The canonical geoscience workflow: a chunked data cube (time × lat × lon) stored
in Zarr, opened lazily, and reduced (climatology + anomaly). This is exactly the
"read from disk, don't hoard in RAM" pattern — DaskGenie shows the memory staying
bounded because the cube streams from Zarr, and attributes compute memory to the
anomaly line.

Needs the ``examples`` + ``deep`` extras. Start the stack, then:

    uv run --extra examples --extra deep python examples/xarray_zarr.py
"""

from __future__ import annotations

import os
import tempfile

import numpy as np
import xarray as xr

import daskgenie as dg

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
NT = int(os.environ.get("DG_NT", "365"))
NY = int(os.environ.get("DG_NY", "720"))
NX = int(os.environ.get("DG_NX", "1440"))


def build_cube_zarr(path: str) -> None:
    # a synthetic "daily temperature" cube, chunked and written to Zarr
    data = xr.DataArray(
        np.random.default_rng(0).random((NT, NY, NX), dtype=np.float32) * 30,
        dims=("time", "lat", "lon"),
        coords={"time": np.arange(NT)},
        name="t2m",
    ).chunk({"time": 30, "lat": NY, "lon": NX})
    data.to_dataset().to_zarr(path, mode="w")


def main() -> None:
    tmp = os.path.join(tempfile.gettempdir(), "daskgenie_cube.zarr")
    print(f"writing a {NT}×{NY}×{NX} cube to {tmp} ...")
    build_cube_zarr(tmp)

    with dg.track() as source_map:
        ds = xr.open_zarr(tmp)
        clim = ds.t2m.mean("time")  # climatology
        anomaly = ds.t2m - clim  # broadcast anomaly (the compute memory)
        result = (anomaly**2).mean().data  # a dask array scalar

    with dg.LocalProfiler(
        COLLECTOR,
        run_name="xarray + Zarr anomaly",
        source_map=source_map,
        collection=result,
        deep=True,
        deep_epoch_seconds=2.0,
    ) as prof:
        print(f"run {prof.run_id}: computing the anomaly variance...")
        val = result.compute(scheduler="threads", num_workers=4)
    print(f"done — result={float(val):.4f}; open the dashboard run {prof.run_id!r}")


if __name__ == "__main__":
    main()
