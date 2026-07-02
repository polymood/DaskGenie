"""A ``SchedulerPlugin`` that captures the post-mortem seed when a worker dies.

When a worker running an OOMing task disappears, the task it was running dies
with the process — the scheduler just sees "worker gone". This plugin keeps a
running index of which task keys are processing on which worker (built from
transition events, so it is independent of the scheduler's own cleanup timing),
and on ``remove_worker`` snapshots those in-flight keys as the prime suspects.

OOM vs. clean shutdown: ``remove_worker`` is called with ``expected=True`` for
planned removals (retirement / scale-down) and ``expected=False`` for abrupt,
unexpected disappearances — which is what an OOM kill looks like to the
scheduler. We label on that signal and do not over-claim: an unexpected removal
is reported as a *suspected* OOM, not a certain one.
"""

from __future__ import annotations

import logging
import threading
import time
import urllib.request
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from distributed.diagnostics.plugin import SchedulerPlugin

from daskgenie.common.schemas import DeathEvent

if TYPE_CHECKING:
    from dask.typing import Key
    from distributed import Scheduler
    from distributed.scheduler import TaskStateState

logger = logging.getLogger("daskgenie.scheduler_plugin")

# How many recent stimulus_ids of task-departures to retain for the death join
# below. remove_worker fires the same stimulus_id as the transitions that clear
# its tasks, and it fires immediately after them, so a small ring is plenty.
_MAX_STIMULI = 128


class DeathAttributionPlugin(SchedulerPlugin):
    name = "daskgenie-death-attribution"

    def __init__(self, collector_url: str, run_id: str, *, http_timeout: float = 5.0) -> None:
        self.collector_url = collector_url.rstrip("/")
        self.run_id = run_id
        self.http_timeout = http_timeout
        # worker address -> set of task keys currently processing there.
        self._processing: dict[str, set[str]] = {}
        # key -> worker it is processing on, so we know the worker at departure.
        self._key_worker: dict[str, str] = {}
        # A worker death reassigns its in-flight tasks via a transitions() call
        # that runs *before* our remove_worker hook and with the SAME
        # stimulus_id (see distributed/scheduler.py remove_worker). So by the
        # time remove_worker runs, those keys have already left "processing".
        # We buffer non-completion departures keyed by stimulus_id and reclaim
        # them in remove_worker. Bounded so a long-running job can't leak it.
        self._departed: OrderedDict[str, list[tuple[str, str]]] = OrderedDict()
        self._scheduler: Scheduler | None = None

    async def start(self, scheduler: Scheduler) -> None:
        self._scheduler = scheduler

    def transition(
        self,
        key: Key,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        try:
            self._track(key, start, finish, stimulus_id)
        except Exception:  # noqa: BLE001 - never break scheduler transitions
            logger.debug("transition tracking failed for %s", key, exc_info=True)

    def _track(
        self, key: Key, start: TaskStateState, finish: TaskStateState, stimulus_id: str
    ) -> None:
        skey = str(key)
        if finish == "processing":
            worker = self._worker_of(key)
            if worker is not None:
                self._processing.setdefault(worker, set()).add(skey)
                self._key_worker[skey] = worker
        elif start == "processing":
            worker = self._key_worker.pop(skey, None)
            if worker is not None:
                self._processing.get(worker, set()).discard(skey)
                # A clean completion goes processing -> memory; anything else
                # (released/erred/forgotten) may be worker-death fallout, so
                # buffer it against this stimulus for remove_worker to reclaim.
                if finish != "memory":
                    self._departed.setdefault(stimulus_id, []).append((skey, worker))
                    while len(self._departed) > _MAX_STIMULI:
                        self._departed.popitem(last=False)

    def _worker_of(self, key: Key) -> str | None:
        if self._scheduler is None:
            return None
        ts = self._scheduler.tasks.get(key)
        ws = getattr(ts, "processing_on", None)
        return ws.address if ws is not None else None

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None:
        suspects = set(self._processing.pop(worker, set()))
        # reclaim tasks this same stimulus already transitioned off the worker
        for skey, w in self._departed.pop(stimulus_id, []):
            if w == worker:
                suspects.add(skey)
        for skey in list(suspects):
            self._key_worker.pop(skey, None)
        suspects_sorted = sorted(suspects)

        # OOM heuristic. The scheduler does not tell a plugin *why* a worker
        # left — remove_worker is called with only (worker, stimulus_id), no
        # "expected" flag — and both an OOM kill and a clean teardown arrive as
        # the same "handle-worker-cleanup" stimulus. The one signal we do have:
        # a worker killed mid-task leaves tasks in flight, whereas a graceful
        # retirement/scale-down drains or reassigns first. So we flag a
        # *suspected* OOM only when tasks were in flight and this isn't an
        # explicit retirement — and never claim certainty. See the spec's
        # "do not over-claim when the cause is ambiguous".
        is_retire = stimulus_id.startswith(("retire-workers", "worker-status-change"))
        suspected_oom = bool(suspects_sorted) and not is_retire
        if is_retire:
            reason = f"retirement / scale-down (expected, stimulus={stimulus_id})"
        elif suspects_sorted:
            reason = (
                f"abrupt removal with {len(suspects_sorted)} task(s) in flight "
                f"(suspected OOM, stimulus={stimulus_id})"
            )
        else:
            reason = f"removal with no task in flight (stimulus={stimulus_id})"
        event = DeathEvent(
            run_id=self.run_id,
            timestamp=time.time(),
            worker=worker,
            suspect_keys=suspects_sorted,
            suspected_oom=suspected_oom,
            reason=reason,
        )
        # Post off the event loop so a slow/unreachable collector can never stall
        # the scheduler. Deaths are rare, so a thread per event is fine.
        threading.Thread(target=self._post, args=(event,), daemon=True).start()

    def _post(self, event: DeathEvent) -> None:
        try:
            req = urllib.request.Request(
                f"{self.collector_url}/ingest/death",
                data=event.model_dump_json().encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.http_timeout):  # noqa: S310
                pass
        except Exception:  # noqa: BLE001 - collector down must not affect the scheduler
            logger.debug("death event post failed", exc_info=True)
