"""Cross-SDK differential conformance (ADR-032 section 14, rollout step 3).

Loads the core-owned fixtures vendored under
tests/fixtures/task_interface_differential/ and asserts that a real
Python declaration matching each vector's own `python` field compiles,
via infer_task_interface, to exactly that vector's
expected_node_interface/expected_pipeline_parameters. TypeScript's own
SDK repo runs the equivalent test against the same fixture files.
"""

from __future__ import annotations

import json
import os
from typing import List, Literal, Optional, TypedDict

import pytest

from brokoli.interface_inference import infer_task_interface

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "task_interface_differential")


def _load(name: str) -> dict:
    with open(os.path.join(_FIXTURE_DIR, f"{name}.json")) as f:
        return json.load(f)


# -- Real Python declarations matching each fixture's own `python` field --


class _DatasetRecordInput(TypedDict):
    id: int
    amount: float


class _DatasetRecordOutput(TypedDict):
    id: int
    score: float


def _dataset_record_score(
    rows: List[_DatasetRecordInput],
) -> List[_DatasetRecordOutput]:
    return []


class _NullableRow(TypedDict):
    name: str
    nickname: Optional[str]


def _nullable_greet(rows: List[_NullableRow]) -> List[_NullableRow]:
    return rows


class _Address(TypedDict):
    street: str
    city: str


class _Customer(TypedDict):
    id: int
    address: _Address


def _nested_enrich(rows: List[_Customer]) -> List[_Customer]:
    return rows


class _StatusRow(TypedDict):
    status: Literal["pending", "done", "failed"]


def _enum_classify(rows: List[_StatusRow]) -> List[_StatusRow]:
    return rows


def _required_parameter_export(rows: List[dict], region: str):
    return None


def _defaulted_parameter_score(rows: List[dict], threshold: float = 0.5):
    return None


@pytest.mark.parametrize(
    "fixture_name, func",
    [
        ("dataset-record", _dataset_record_score),
        ("nullable-required-field", _nullable_greet),
        ("nested-record", _nested_enrich),
        ("enum-field", _enum_classify),
        ("required-parameter", _required_parameter_export),
        ("defaulted-parameter", _defaulted_parameter_score),
    ],
)
def test_python_declaration_matches_the_core_owned_vector(fixture_name, func):
    fixture = _load(fixture_name)
    result = infer_task_interface(func)
    assert result.node_interface == fixture["expected_node_interface"]
    assert result.parameters == (fixture["expected_pipeline_parameters"] or {})
