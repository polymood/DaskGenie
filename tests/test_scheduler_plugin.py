"""Unit tests for the death-attribution tracking logic, driven with fake
scheduler state so they run without a cluster. The tricky part these lock down
is the race: the scheduler transitions a dead worker's tasks out of
``processing`` (same stimulus_id) *before* calling remove_worker, so suspects
must be reclaimed from the stimulus-keyed departure buffer.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from daskgenie.scheduler_plugin.plugin import DeathAttributionPlugin


class _FakeScheduler:
    """Minimal stand-in exposing ``tasks[key].processing_on.address``."""

    def __init__(self) -> None:
        self.tasks: dict[Any, Any] = {}

    def place(self, key: str, worker: str) -> None:
        self.tasks[key] = SimpleNamespace(processing_on=SimpleNamespace(address=worker))

    def unplace(self, key: str) -> None:
        self.tasks[key] = SimpleNamespace(processing_on=None)


def _plugin(monkeypatch: Any) -> tuple[DeathAttributionPlugin, list[Any]]:
    plugin = DeathAttributionPlugin("http://collector")
    posted: list[Any] = []
    # capture the event instead of firing a background HTTP thread
    monkeypatch.setattr(plugin, "_post", posted.append)
    monkeypatch.setattr(
        "daskgenie.scheduler_plugin.plugin.threading.Thread",
        lambda target, args, daemon: SimpleNamespace(start=lambda: target(*args)),
    )
    return plugin, posted


def test_suspect_reclaimed_after_pre_removal_transition(monkeypatch: Any) -> None:
    plugin, posted = _plugin(monkeypatch)
    sched = _FakeScheduler()
    plugin._scheduler = sched  # type: ignore[assignment]  # start() is async; set directly

    # task starts processing on w1
    sched.place("taskA", "tcp://w1")
    plugin.transition("taskA", "waiting", "processing", stimulus_id="update-graph-1")

    # worker dies: scheduler first releases the task (same stimulus as removal),
    # clearing it from the live processing set...
    sched.unplace("taskA")
    plugin.transition("taskA", "processing", "released", stimulus_id="handle-worker-cleanup-9")
    # ...then calls remove_worker with that same stimulus_id
    plugin.remove_worker(sched, "tcp://w1", stimulus_id="handle-worker-cleanup-9")  # type: ignore[arg-type]

    assert len(posted) == 1
    event = posted[0]
    assert event.worker == "tcp://w1"
    assert event.suspect_keys == ["taskA"]  # reclaimed despite the earlier release
    assert event.suspected_oom is True


def test_clean_completion_is_not_a_suspect(monkeypatch: Any) -> None:
    plugin, posted = _plugin(monkeypatch)
    sched = _FakeScheduler()
    plugin._scheduler = sched  # type: ignore[assignment]  # start() is async; set directly

    sched.place("taskA", "tcp://w1")
    plugin.transition("taskA", "waiting", "processing", stimulus_id="s1")
    # normal success: processing -> memory
    plugin.transition("taskA", "processing", "memory", stimulus_id="s2")
    plugin.remove_worker(sched, "tcp://w1", stimulus_id="handle-worker-cleanup-1")  # type: ignore[arg-type]

    assert posted[0].suspect_keys == []
    assert posted[0].suspected_oom is False  # no task in flight -> not an OOM


def test_retirement_not_flagged_as_oom(monkeypatch: Any) -> None:
    plugin, posted = _plugin(monkeypatch)
    sched = _FakeScheduler()
    plugin._scheduler = sched  # type: ignore[assignment]  # start() is async; set directly

    sched.place("taskA", "tcp://w1")
    plugin.transition("taskA", "waiting", "processing", stimulus_id="s1")
    sched.unplace("taskA")
    plugin.transition("taskA", "processing", "released", stimulus_id="retire-workers-1")
    plugin.remove_worker(sched, "tcp://w1", stimulus_id="retire-workers-1")  # type: ignore[arg-type]

    # even with a task in flight, an explicit retirement must not over-claim OOM
    assert posted[0].suspected_oom is False
