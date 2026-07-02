"""Describe a materialized array-like as ChunkMeta. Shared by the distributed
worker plugin and the local-scheduler callback so both measure chunks the same
way.
"""

from __future__ import annotations

from daskgenie.common.schemas import ChunkMeta


def key_str(key: object) -> str:
    # Dask keys are often tuples like ("rechunk-merge-abc", 0, 1); str() gives a
    # stable, join-friendly form that matches the graph/source map.
    return key if isinstance(key, str) else str(key)


def layer_of(key: object) -> str:
    # A task's layer name is the first element of its tuple key, which matches
    # the HighLevelGraph layer names and joins to the source map.
    return str(key[0]) if isinstance(key, tuple) and key else str(key)


def describe_array(key: object, data: object) -> ChunkMeta | None:
    """Return ChunkMeta for a numpy/dask-chunk-like object, else None."""
    shape = getattr(data, "shape", None)
    dtype = getattr(data, "dtype", None)
    nbytes = getattr(data, "nbytes", None)
    if shape is None or nbytes is None:
        return None
    try:
        return ChunkMeta(
            task_key=key_str(key),
            shape=tuple(int(d) for d in shape),
            dtype=str(dtype),
            nbytes=int(nbytes),
        )
    except (TypeError, ValueError):
        return None
