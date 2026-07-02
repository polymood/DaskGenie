"""Client-side glue: install the worker plugin on a cluster and push the
captured ``layer -> source`` map to the collector.

This is the one place user code touches all three of GraphCapture, the worker
plugin, and the collector, so it lives at the top level rather than inside any
one component's package.
"""

from __future__ import annotations

import json
import urllib.request

from distributed import Client

from daskgenie.common.schemas import GraphLayer, GraphUpload, RunCreate
from daskgenie.graphcapture import GraphInfo, SourceLocation
from daskgenie.scheduler_plugin import DeathAttributionPlugin
from daskgenie.worker_plugin import MemoryProfilerPlugin


def register(
    client: Client,
    collector_url: str,
    *,
    run_name: str = "",
    sample_interval: float = 0.2,
    flush_interval: float = 0.5,
) -> str:
    """Open a run and install both profiler plugins on the cluster.

    Returns the ``run_id`` — pass it to :func:`upload_graph` and use it to find
    this session in the dashboard. Every sample, chunk, and death event from
    this cluster session is tagged with it.

    ``flush_interval`` bounds how long chunk metadata can sit unsent on a
    worker; keep it well under how fast your workers OOM, or the killer chunk's
    metadata dies with the process before it is pushed.
    """
    run_id = _create_run(collector_url, run_name)
    client.register_plugin(
        MemoryProfilerPlugin(
            collector_url,
            run_id,
            sample_interval=sample_interval,
            flush_interval=flush_interval,
        )
    )
    client.register_plugin(DeathAttributionPlugin(collector_url, run_id))
    return run_id


def _create_run(collector_url: str, name: str) -> str:
    req = urllib.request.Request(
        f"{collector_url.rstrip('/')}/api/runs",
        data=RunCreate(name=name).model_dump_json().encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        return str(json.load(resp)["id"])


def upload_graph(
    collector_url: str,
    run_id: str,
    layer_map: dict[str, SourceLocation],
    graph_info: GraphInfo | None = None,
) -> None:
    """Push the source map (and optional dependency edges) to the collector."""
    layers = [
        GraphLayer(
            layer=name,
            filename=loc.filename,
            lineno=loc.lineno,
            code_snippet=loc.code_snippet,
        )
        for name, loc in layer_map.items()
    ]
    deps = (
        {layer: sorted(d) for layer, d in graph_info.layer_dependencies.items()}
        if graph_info is not None
        else {}
    )
    upload = GraphUpload(run_id=run_id, layers=layers, layer_dependencies=deps)
    req = urllib.request.Request(
        f"{collector_url.rstrip('/')}/ingest/graph",
        data=upload.model_dump_json().encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10):  # noqa: S310
        pass
