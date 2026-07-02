"""Schemas and constants shared across process boundaries.

Plugins (worker/scheduler) and the collector never import each other's
internals — they only exchange the pydantic models defined here. This is the
one module both sides are allowed to depend on.
"""

from daskgenie.common.schemas import (
    SCHEMA_VERSION,
    ChunkMeta,
    DeathEvent,
    GraphLayer,
    GraphUpload,
    MemorySample,
    SampleBatch,
)

__all__ = [
    "SCHEMA_VERSION",
    "ChunkMeta",
    "DeathEvent",
    "GraphLayer",
    "GraphUpload",
    "MemorySample",
    "SampleBatch",
]
