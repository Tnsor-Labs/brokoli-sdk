"""Pipeline/@task-level wiring for ADR-032 rollout step 3: the inferred
node interface and pipeline parameters actually reach to_json(), the IR
version bumps only when they're used, an explicit interface= wins
outright, and a genuine cross-task parameter collision raises rather
than silently keeping whichever declaration came first.

Pure inference-function behavior (TypedDict/dataclass mapping, warnings,
unresolvable references, ...) is covered in tests/test_interface_inference.py.
"""

from __future__ import annotations

from typing import TypedDict

import pytest

from brokoli import Pipeline, task
from brokoli.exceptions import PipelineError
from brokoli.pipeline import IR_VERSION, TASK_INTERFACE_IR_VERSION


class InputRow(TypedDict):
    id: int
    amount: float


class ScoredRow(TypedDict):
    id: int
    score: float


def test_typed_task_bumps_ir_version_and_carries_node_interface_and_parameters():
    with Pipeline("typed", pipeline_id="typed") as p:

        @task
        def score(rows: list[InputRow], threshold: float = 0.5) -> list[ScoredRow]:
            return rows

        score()

    data = p.to_json()
    assert data["ir_version"] == TASK_INTERFACE_IR_VERSION
    assert data["parameters"] == {
        "threshold": {"type": {"kind": "float64"}, "required": False, "default": 0.5}
    }
    node = data["nodes"][0]
    assert node["interface"]["contract"] == "brokoli.task-interface/v1"
    assert node["interface"]["inputs"]["input"]["value"]["row"]["kind"] == "record"


def test_untyped_task_stays_at_ir_2_0_with_no_interface_or_parameters_keys():
    with Pipeline("plain", pipeline_id="plain") as p:

        @task
        def clean(rows):
            return [r for r in rows if r.get("amount", 0) > 0]

        clean()

    data = p.to_json()
    assert data["ir_version"] == IR_VERSION
    assert "parameters" not in data
    assert "interface" not in data["nodes"][0]


def test_explicit_interface_wins_outright_over_inference():
    custom = {
        "contract": "brokoli.task-interface/v1",
        "inputs": {"input": {"value": {"kind": "dataset", "row": {"kind": "unknown"}}}},
        "outputs": {"result": {"value": {"kind": "scalar", "type": {"kind": "int64"}}}},
    }

    with Pipeline("override", pipeline_id="override") as p:

        @task(interface=custom)
        def weird(rows: list[InputRow]):
            return 1

        weird()

    data = p.to_json()
    assert data["nodes"][0]["interface"] == custom


def test_explicit_interface_does_not_suppress_parameter_inference():
    custom = {
        "contract": "brokoli.task-interface/v1",
        "inputs": {"input": {"value": {"kind": "dataset", "row": {"kind": "unknown"}}}},
        "outputs": {"result": {"value": {"kind": "dataset", "row": {"kind": "unknown"}}}},
    }

    with Pipeline("override-params", pipeline_id="override-params") as p:

        @task(interface=custom)
        def f(rows, threshold: float = 0.5):
            return rows

        f()

    data = p.to_json()
    assert data["parameters"] == {
        "threshold": {"type": {"kind": "float64"}, "required": False, "default": 0.5}
    }


def test_two_tasks_sharing_a_parameter_name_with_the_same_declaration_is_fine():
    with Pipeline("shared", pipeline_id="shared") as p:

        @task
        def a(rows, threshold: float = 0.5):
            return rows

        @task
        def b(rows, threshold: float = 0.5):
            return rows

        a()
        b()

    data = p.to_json()
    assert data["parameters"] == {
        "threshold": {"type": {"kind": "float64"}, "required": False, "default": 0.5}
    }


def test_two_tasks_with_conflicting_parameter_declarations_raises():
    with Pipeline("collision", pipeline_id="collision"):

        @task
        def a(rows, threshold: float = 0.5):
            return rows

        @task
        def b(rows, threshold: str = "x"):
            return rows

        a()
        with pytest.raises(PipelineError, match="threshold"):
            b()
