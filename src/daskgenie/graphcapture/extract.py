"""Pull layer names, per-layer keys, and layer dependencies out of a Dask
collection's HighLevelGraph. Thin wrapper — the HighLevelGraph API already
gives us this; we just shape it into something easy to join against the
source map and, later, memory samples.
"""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass, field

from dask.core import get_dependencies
from dask.highlevelgraph import HighLevelGraph


@dataclass(frozen=True, slots=True)
class GraphInfo:
    layers: tuple[str, ...]
    layer_dependencies: dict[str, frozenset[str]]
    keys_by_layer: dict[str, tuple[Hashable, ...]]


@dataclass(frozen=True, slots=True)
class TaskGraph:
    """The concrete task-level graph Dask builds: one node per task key, edges
    for every data dependency. ``layer`` on each node is the key's layer name
    (``key[0]`` for tuple keys), which joins directly to the source map.
    """

    nodes: list[tuple[str, str]] = field(default_factory=list)  # (key, layer)
    edges: list[tuple[str, str]] = field(default_factory=list)  # (src_key, dst_key)
    task_count: int = 0
    truncated: bool = False  # True if the real graph exceeded max_nodes


def _layer_of(key: object) -> str:
    return str(key[0]) if isinstance(key, tuple) and key else str(key)


def extract_task_graph(collection: object, *, max_nodes: int = 8000) -> TaskGraph:
    """Build the full task graph from a collection, capped so a huge graph can't
    blow up the payload or the browser. The dashboard renders task graphs up to
    this size on a pan/zoom canvas; above it, callers fall back to the layer
    view (``truncated=True``).
    """
    from daskgenie.common.arrays import key_str

    dask_graph = collection.__dask_graph__()  # type: ignore[attr-defined]
    count = len(dask_graph)
    if count > max_nodes:
        return TaskGraph(task_count=count, truncated=True)

    flat = dict(dask_graph)
    nodes = [(key_str(k), _layer_of(k)) for k in flat]
    edges = [
        (key_str(dep), key_str(k)) for k in flat for dep in get_dependencies(flat, k) if dep in flat
    ]
    return TaskGraph(nodes=nodes, edges=edges, task_count=count, truncated=False)


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
