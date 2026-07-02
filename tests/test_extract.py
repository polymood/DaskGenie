from __future__ import annotations

import dask.array as da

from daskgenie.graphcapture.extract import extract_graph


def test_extract_graph_matches_high_level_graph_layers() -> None:
    x = da.ones((100, 100), chunks=(10, 10))
    y = x.rechunk((50, 50))

    info = extract_graph(y)
    hlg = y.__dask_graph__()

    assert set(info.layers) == set(hlg.layers)
    for name in info.layers:
        assert set(info.keys_by_layer[name]) == set(hlg.layers[name].keys())


def test_extract_graph_dependencies_are_consistent_with_hlg() -> None:
    x = da.ones((100, 100), chunks=(10, 10))
    y = x.rechunk((50, 50))
    z = y.map_blocks(lambda b: b * 2)

    info = extract_graph(z)
    hlg = z.__dask_graph__()

    for name in info.layers:
        assert info.layer_dependencies[name] == frozenset(hlg.dependencies.get(name, ()))
