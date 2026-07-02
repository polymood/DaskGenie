"""The collector service: ingest, storage, Prometheus metrics, and query API."""

from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store

__all__ = ["create_app", "Store"]
