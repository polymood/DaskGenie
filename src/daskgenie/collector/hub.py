"""In-process pub/sub bridging sync ingest to async WebSocket subscribers.

Ingest endpoints run in FastAPI's threadpool (they're ``def``, not ``async``);
the ``/ws`` endpoint runs on the event loop. So ``publish`` is called from a
worker thread and must hand the event to the loop safely
(``call_soon_threadsafe``), which then fans it out to each subscriber's
``asyncio.Queue``. Queues are bounded: a slow browser drops its oldest frame
rather than growing the collector's memory — the same "never be the thing that
OOMs" discipline the plugins follow.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any


class Hub:
    def __init__(self, max_queue: int = 2000) -> None:
        self._subs: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._max_queue = max_queue

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the serving event loop (called from FastAPI startup)."""
        self._loop = loop

    def subscribe(self, run_id: str) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._max_queue)
        with self._lock:
            self._subs.setdefault(run_id, set()).add(q)
        return q

    def unsubscribe(self, run_id: str, q: asyncio.Queue[dict[str, Any]]) -> None:
        with self._lock:
            subs = self._subs.get(run_id)
            if subs is not None:
                subs.discard(q)
                if not subs:
                    self._subs.pop(run_id, None)

    def publish(self, run_id: str, event: dict[str, Any]) -> None:
        """Fan ``event`` out to this run's subscribers. Safe from any thread;
        a no-op if nobody is watching or the loop isn't bound yet.
        """
        loop = self._loop
        if loop is None:
            return
        with self._lock:
            if run_id not in self._subs:
                return
        try:
            loop.call_soon_threadsafe(self._deliver, run_id, event)
        except RuntimeError:  # loop closed during shutdown
            pass

    def _deliver(self, run_id: str, event: dict[str, Any]) -> None:
        # Runs on the event loop.
        with self._lock:
            queues = list(self._subs.get(run_id, ()))
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow consumer: drop its oldest frame to make room for the new.
                try:
                    q.get_nowait()
                    q.put_nowait(event)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
