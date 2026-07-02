"""Tiny HTTP helpers for talking to the collector, with no heavy dependencies
(stdlib ``urllib`` only). Both the distributed client glue and the local
callback profiler use these, so neither drags in the other's deps.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Mapping, Sequence

from daskgenie.common.schemas import GraphLayer, GraphUpload, SampleBatch
from daskgenie.graphcapture import GraphInfo, SourceLocation


def _post(url: str, payload: bytes, *, timeout: float = 10.0) -> bytes:
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return bytes(resp.read())


def create_run(collector_url: str, name: str = "") -> str:
    """Open a run on the collector and return its id."""
    body = json.dumps({"name": name}).encode("utf-8")
    raw = _post(f"{collector_url.rstrip('/')}/api/runs", body)
    return str(json.loads(raw)["id"])


def post_sample_batch(collector_url: str, batch: SampleBatch, *, timeout: float = 5.0) -> None:
    _post(
        f"{collector_url.rstrip('/')}/ingest/samples",
        batch.model_dump_json().encode("utf-8"),
        timeout=timeout,
    )


def upload_graph(
    collector_url: str,
    run_id: str,
    layer_map: Mapping[str, SourceLocation],
    graph_info: GraphInfo | None = None,
) -> None:
    """Push the ``layer -> source`` map (and optional dependency edges) to a run."""
    layers = [
        GraphLayer(
            layer=name,
            filename=loc.filename,
            lineno=loc.lineno,
            code_snippet=loc.code_snippet,
        )
        for name, loc in layer_map.items()
    ]
    deps: dict[str, list[str]] = (
        {layer: sorted(d) for layer, d in graph_info.layer_dependencies.items()}
        if graph_info is not None
        else {}
    )
    upload = GraphUpload(run_id=run_id, layers=layers, layer_dependencies=deps)
    _post(f"{collector_url.rstrip('/')}/ingest/graph", upload.model_dump_json().encode("utf-8"))


__all__: Sequence[str] = ["create_run", "post_sample_batch", "upload_graph"]
