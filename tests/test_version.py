"""The version people see is the version we released (#78)."""

import re
from pathlib import Path

import brokoli


def test_version_matches_pyproject_when_installed():
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    m = re.search(r'^version = "([^"]+)"', pyproject.read_text(), re.M)
    assert m, "pyproject version not found"
    # In CI the package is imported from the source tree (dev fallback) or
    # an installed dist; either way it must never be a stale hand-written
    # number that matches neither.
    assert brokoli.__version__ in (m.group(1), "0.0.0.dev0")
    assert brokoli.__version__ != "0.4.0" or m.group(1) == "0.4.0"
