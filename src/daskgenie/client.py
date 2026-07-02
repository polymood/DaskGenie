"""Client-side glue for ``dask.distributed``: install the profiler plugins on a
cluster. Graph/source upload lives in :mod:`daskgenie.report` (no distributed
dependency) and is re-exported here for convenience.
"""

from __future__ import annotations

from distributed import Client

from daskgenie.report import create_run, upload_graph
from daskgenie.scheduler_plugin import DeathAttributionPlugin
from daskgenie.worker_plugin import MemoryProfilerPlugin

__all__ = ["register", "upload_graph"]


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
    run_id = create_run(collector_url, run_name)
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
