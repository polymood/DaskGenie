"""Profile a **dask.bag** pipeline (processes scheduler).

Bags are Dask's tool for messy, record-oriented data (JSON logs, text). This
simulates a stream of event records, parses/filters/aggregates them (a word/tag
frequency count via ``foldby``), and profiles it on the multiprocessing
scheduler — so DaskGenie shows per-process memory, not just threads.

Needs the ``examples`` + ``deep`` extras. Start the stack, then:

    uv run --extra examples --extra deep python examples/dask_bag.py
"""

from __future__ import annotations

import os
import random

import daskgenie as dg

COLLECTOR = os.environ.get("DG_COLLECTOR", "http://localhost:8765")
N = int(os.environ.get("DG_N", "4000000"))
PARTS = int(os.environ.get("DG_PARTS", "32"))

TAGS = ["error", "warn", "info", "debug", "trace", "fatal", "audit", "metric"]


def make_record(i: int) -> dict:
    rng = random.Random(i)
    return {"id": i, "tag": rng.choice(TAGS), "value": rng.random(), "ok": rng.random() > 0.2}


def main() -> None:
    import dask.bag as db

    with dg.track() as source_map:
        bag = db.from_sequence(range(N), npartitions=PARTS).map(make_record)
        kept = bag.filter(lambda r: r["ok"])
        # frequency of each tag among kept records
        counts = kept.foldby("tag", lambda acc, _r: acc + 1, 0, lambda a, b: a + b)
        result = counts

    with dg.LocalProfiler(
        COLLECTOR,
        run_name="dask.bag log aggregation",
        source_map=source_map,
        deep=True,
        deep_epoch_seconds=2.0,
    ) as prof:
        print(f"run {prof.run_id}: folding {N:,} records over {PARTS} partitions...")
        out = dict(result.compute(scheduler="processes", num_workers=4))
    print(f"done — tag counts: {out}\nopen the dashboard run {prof.run_id!r}")


if __name__ == "__main__":
    main()
