"""Pull layer names, per-layer keys, and layer dependencies out of a Dask
collection's HighLevelGraph. Thin wrapper — the HighLevelGraph API already
gives us this; we just shape it into something easy to join against the
source map and, later, memory samples.
"""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass

from dask.highlevelgraph import HighLevelGraph


@dataclass(frozen=True, slots=True)
class GraphInfo:
    layers: tuple[str, ...]
    layer_dependencies: dict[str, frozenset[str]]
    keys_by_layer: dict[str, tuple[Hashable, ...]]


def extract_graph(collection: object) -> GraphInfo:
    """Extract layer/key/dependency structure from any Dask collection
    (dask.array, dask.dataframe, or xarray objects backed by dask arrays).
    """
    dask_graph = collection.__dask_graph__()  # type: ignore[attr-defined]
    hlg = (
        dask_graph
        if isinstance(dask_graph, HighLevelGraph)
        else HighLevelGraph.from_collections("root", dict(dask_graph), [])
    )
    layers = tuple(hlg.layers)
    layer_dependencies: dict[str, frozenset[str]] = {
        name: frozenset(hlg.dependencies.get(name, ())) for name in layers
    }
    keys_by_layer: dict[str, tuple[Hashable, ...]] = {
        name: tuple(layer.keys()) for name, layer in hlg.layers.items()
    }
    return GraphInfo(
        layers=layers, layer_dependencies=layer_dependencies, keys_by_layer=keys_by_layer
    )
