"""Canonical rendering matches core's cross-SDK vectors (#982).

The vectors are vendored from core's
docs/schema/fixtures/ir-canonicalization/rendering.json. Each input is JSON
text; parsed with Python's own json module and rendered canonically, it must
give exactly the expected text, which is what the TypeScript SDK produces
from the same input.
"""

from __future__ import annotations

import json
import os
import random
import struct

import pytest

from brokoli.ir import canonical_json, canonical_number

_FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "ir_canonicalization", "rendering.json"
)

with open(_FIXTURE, encoding="utf-8") as f:
    _CASES = json.load(f)["cases"]


@pytest.mark.parametrize("case", _CASES, ids=[c["name"] for c in _CASES])
def test_renders_each_core_vector(case):
    assert canonical_json(json.loads(case["input"])) == case["canonical"]


def test_renders_what_json_dumps_did_for_everything_but_floats():
    value = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "a",
                "config": {
                    "n": 3,
                    "flag": True,
                    "none": None,
                    "text": 'café   "q"',
                    "list": [],
                    "map": {},
                },
            }
        ],
        "edges": [{"from": "a", "to": "b"}],
        "big": 2**70,
        "neg": -12,
    }
    old = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    assert canonical_json(value) == old


def test_every_float_reads_back_as_itself():
    rng = random.Random(982)
    for _ in range(20000):
        value = struct.unpack("<d", struct.pack("<Q", rng.getrandbits(64)))[0]
        if value != value or value in (float("inf"), float("-inf")):
            continue
        assert float(canonical_number(value)) == value, value


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_refuses_non_finite_numbers(value):
    with pytest.raises(ValueError, match="Out of range float values"):
        canonical_json({"value": value})
