"""Run.wait's polling cadence.

A flat 2s poll meant a run the server finished in 200ms was reported two
seconds later — measured end to end at ~2.07s against a server span of
~0.20s. These pin the backoff so that does not come back.
"""
from __future__ import annotations

import pytest

from brokoli.client import Run


class _FakeClient:
    """Answers `running` a fixed number of times, then `success`."""

    def __init__(self, pending_polls: int):
        self.pending_polls = pending_polls
        self.polls = 0

    def _request(self, method, path, **kw):
        self.polls += 1
        status = "success" if self.polls > self.pending_polls else "running"
        return {"id": "run-1", "status": status}


@pytest.fixture
def sleeps(monkeypatch):
    recorded: list[float] = []
    monkeypatch.setattr("brokoli.client.time.sleep", recorded.append)
    return recorded


def test_terminal_on_first_poll_never_sleeps(sleeps):
    assert Run(_FakeClient(0), "run-1").wait()["status"] == "success"
    assert sleeps == []


def test_first_wait_is_short_not_the_ceiling(sleeps):
    Run(_FakeClient(1), "run-1").wait()
    assert sleeps and sleeps[0] == pytest.approx(0.05)


def test_backoff_grows_and_is_capped(sleeps):
    Run(_FakeClient(25), "run-1").wait(poll_interval=2.0)
    assert sleeps[0] < sleeps[1] < sleeps[2]
    assert max(sleeps) <= 2.0
    assert sleeps[-1] == pytest.approx(2.0)


def test_initial_never_exceeds_the_ceiling(sleeps):
    Run(_FakeClient(2), "run-1").wait(poll_interval=0.01, initial_poll_interval=5.0)
    assert max(sleeps) <= 0.01


def test_long_runs_still_poll_at_the_ceiling(sleeps):
    """Backoff must not increase load on long-running jobs."""
    Run(_FakeClient(40), "run-1").wait(poll_interval=2.0)
    assert sum(sleeps) >= 2.0 * (len(sleeps) - 10)
