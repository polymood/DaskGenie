"""The smallest DaskGenie example: map task-graph layers back to the source
lines that built them. No collector, no cluster, no dashboard — just run it.

    uv run --extra demo python examples/graph_source_map.py
"""

from __future__ import annotations

import dask.array as da

import daskgenie as dg


def build() -> da.Array:
    x = da.random.random((8000, 8000), chunks=(500, 500))
    y = x.rechunk((8000, 8000))
    return y.map_blocks(lambda b: b * 2).sum()


def main() -> None:
    with dg.track() as source_map:
        build()

    print(f"{'layer':<45} source")
    print("-" * 100)
    for layer, loc in sorted(source_map.items(), key=lambda kv: kv[1].lineno):
        print(f"{layer:<45} {loc.filename}:{loc.lineno}  {loc.code_snippet}")


if __name__ == "__main__":
    main()
