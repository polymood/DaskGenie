"""Profile an **xarray-on-NetCDF** pipeline (threaded scheduler).

Same shape as the Zarr example but over a NetCDF file (HDF5 via ``h5netcdf``) —
the format most climate/ocean data actually ships in. Opens the file with Dask
chunks, computes a rolling-mean smoothing and a reduction, and DaskGenie
attributes the memory to your lines.

Needs the ``examples`` + ``deep`` extras. Start the stack, then:

    uv run --extra examples --extra deep python examples/xarray_netcdf.py
"""

from __future__ import annotations

import os
import tempfile

import numpy as np
import xarray as xr

import daskgenie as dg

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
NT = int(os.environ.get("DG_NT", "200"))
NY = int(os.environ.get("DG_NY", "512"))
NX = int(os.environ.get("DG_NX", "1024"))


def build_netcdf(path: str) -> None:
    ds = xr.DataArray(
        np.random.default_rng(1).random((NT, NY, NX), dtype=np.float32),
        dims=("time", "lat", "lon"),
        name="ssh",
    ).to_dataset()
    ds.to_netcdf(path, engine="h5netcdf")


def main() -> None:
    tmp = os.path.join(tempfile.gettempdir(), "daskgenie_field.nc")
    print(f"writing a {NT}×{NY}×{NX} NetCDF to {tmp} ...")
    build_netcdf(tmp)

    with dg.track() as source_map:
        ds = xr.open_dataset(tmp, engine="h5netcdf", chunks={"time": 20})
        smooth = ds.ssh.rolling(time=5, min_periods=1).mean()  # temporal smoothing
        detrended = ds.ssh - smooth
        result = detrended.var(dim="time").mean().data

    with dg.LocalProfiler(
        COLLECTOR,
        run_name="xarray + NetCDF smoothing",
        source_map=source_map,
        collection=result,
        deep=True,
        deep_epoch_seconds=2.0,
    ) as prof:
        print(f"run {prof.run_id}: computing detrended variance...")
        val = result.compute(scheduler="threads", num_workers=4)
    print(f"done — result={float(val):.5f}; open the dashboard run {prof.run_id!r}")


if __name__ == "__main__":
    main()
