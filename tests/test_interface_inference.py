"""Unit tests for brokoli.interface_inference (ADR-032 rollout step 3).

Direct tests of infer_task_interface() -- pipeline/decorator-level
wiring (interface= override, parameter collisions, IR version bump) is
covered in tests/test_decorators.py and tests/test_feature_preflight.py.

Every TypedDict/dataclass/class used as a type hint here is declared at
MODULE level, not inside the test function: typing.get_type_hints()
resolves string annotations via the function's own __globals__ (ADR-032
section 8 rule 7 -- no arbitrary-module imports during discovery), which
does not see a class defined in the enclosing test function's local
scope. A class nested inside a test body would make get_type_hints()
report an unresolvable forward reference regardless of how the
inference code itself behaves -- exercising the wrong thing.
"""

from __future__ import annotations

import dataclasses
import warnings
from typing import Dict, List, Literal, Optional, TypedDict, Union

import pytest

from brokoli.interface_inference import infer_task_interface


class InputRow(TypedDict):
    id: int
    amount: float


class ScoredRow(TypedDict):
    id: int
    score: float


@dataclasses.dataclass
class RowDataclass:
    id: int
    label: str = "x"


class NotATypedDict:
    x: int


class NullableRow(TypedDict):
    name: str
    nickname: Optional[str]


class Address(TypedDict):
    street: str
    city: str


class Customer(TypedDict):
    id: int
    address: Address


class StatusRow(TypedDict):
    status: Literal["pending", "done", "failed"]


class CollectionRow(TypedDict):
    tags: List[str]
    counts: Dict[str, int]


class RowWithUnmappableField(TypedDict):
    id: int
    weird: NotATypedDict


def test_typed_dict_rows_and_return_infer_a_record_interface():
    def score(rows: list[InputRow]) -> list[ScoredRow]:
        return []

    result = infer_task_interface(score)
    assert result.node_interface == {
        "contract": "brokoli.task-interface/v1",
        "inputs": {
            "input": {
                "value": {
                    "kind": "dataset",
                    "row": {
                        "kind": "record",
                        "fields": [
                            {"name": "id", "type": {"kind": "int64"}, "required": True},
                            {"name": "amount", "type": {"kind": "float64"}, "required": True},
                        ],
                        "additional_fields": False,
                    },
                }
            }
        },
        "outputs": {
            "result": {
                "value": {
                    "kind": "dataset",
                    "row": {
                        "kind": "record",
                        "fields": [
                            {"name": "id", "type": {"kind": "int64"}, "required": True},
                            {"name": "score", "type": {"kind": "float64"}, "required": True},
                        ],
                        "additional_fields": False,
                    },
                }
            }
        },
    }
    assert result.parameters == {}


def test_dataclass_rows_infer_a_record_with_default_field_optional():
    def transform(rows: list[RowDataclass]) -> list[RowDataclass]:
        return rows

    result = infer_task_interface(transform)
    fields = result.node_interface["inputs"]["input"]["value"]["row"]["fields"]
    by_name = {f["name"]: f for f in fields}
    assert by_name["id"]["required"] is True
    assert by_name["label"]["required"] is False


def test_no_annotation_infers_nothing_and_never_warns():
    def clean(rows):
        return [r for r in rows if r.get("amount", 0) > 0]

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = infer_task_interface(clean)
    assert result.node_interface is None
    assert result.parameters == {}


def test_list_of_plain_dict_stays_unknown_without_warning():
    # A deliberate "not describing this" signal, not a failed richer type
    # -- must stay silent (ADR-032 section 13).
    def export(rows: list[dict], region: str):
        return None

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = infer_task_interface(export)
    assert result.node_interface is None
    assert result.parameters == {"region": {"type": {"kind": "string"}, "required": True}}


def test_unresolvable_forward_reference_warns_and_falls_back_to_unknown():
    def broken(rows: "NoSuchTypeAnywhere"):  # noqa: F821
        return rows

    with pytest.warns(UserWarning, match="could not resolve type hints"):
        result = infer_task_interface(broken)
    assert result.node_interface is None
    assert result.parameters == {}


def test_unmappable_native_type_warns_and_falls_back_to_unknown():
    def weird(rows: list[NotATypedDict]):
        return rows

    with pytest.warns(UserWarning, match="could not infer a portable type"):
        result = infer_task_interface(weird)
    assert result.node_interface is None


def test_keyword_with_default_becomes_optional_pipeline_parameter():
    def score(rows: list[InputRow], threshold: float = 0.5) -> list[ScoredRow]:
        return rows

    result = infer_task_interface(score)
    assert result.parameters == {
        "threshold": {"type": {"kind": "float64"}, "required": False, "default": 0.5}
    }


def test_keyword_without_default_becomes_required_pipeline_parameter():
    def export(rows: list[dict], region: str):
        return None

    result = infer_task_interface(export)
    assert result.parameters == {"region": {"type": {"kind": "string"}, "required": True}}


def test_keyword_with_unmappable_annotation_is_skipped_not_guessed():
    def weird(rows: list[dict], mode: Union[int, str] = 1):
        return None

    with pytest.warns(UserWarning):
        result = infer_task_interface(weird)
    assert result.parameters == {}


def test_optional_field_maps_to_nullable_not_optional_presence():
    def greet(rows: list[NullableRow]) -> list[NullableRow]:
        return rows

    result = infer_task_interface(greet)
    fields = result.node_interface["inputs"]["input"]["value"]["row"]["fields"]
    by_name = {f["name"]: f for f in fields}
    assert by_name["nickname"]["type"] == {"kind": "string", "nullable": True}
    assert by_name["nickname"]["required"] is True


def test_nested_typed_dict_recurses_into_a_nested_record():
    def enrich(rows: list[Customer]) -> list[Customer]:
        return rows

    result = infer_task_interface(enrich)
    fields = result.node_interface["inputs"]["input"]["value"]["row"]["fields"]
    address_field = next(f for f in fields if f["name"] == "address")
    assert address_field["type"]["kind"] == "record"
    assert {f["name"] for f in address_field["type"]["fields"]} == {"street", "city"}


def test_literal_string_field_maps_to_enum():
    def classify(rows: list[StatusRow]) -> list[StatusRow]:
        return rows

    result = infer_task_interface(classify)
    fields = result.node_interface["inputs"]["input"]["value"]["row"]["fields"]
    assert fields[0]["type"] == {"kind": "enum", "values": ["pending", "done", "failed"]}


def test_list_and_dict_fields_map_to_array_and_map():
    def f(rows: list[CollectionRow]) -> list[CollectionRow]:
        return rows

    result = infer_task_interface(f)
    fields = result.node_interface["inputs"]["input"]["value"]["row"]["fields"]
    by_name = {field["name"]: field for field in fields}
    assert by_name["tags"]["type"] == {"kind": "array", "items": {"kind": "string"}}
    assert by_name["counts"]["type"] == {
        "kind": "map",
        "keys": "string",
        "values": {"kind": "int64"},
    }


def test_unmappable_nested_field_fails_the_whole_record_closed():
    # A partially-honest closed record (missing a real field while
    # claiming additional_fields: false) is worse than admitting the
    # whole row is unknown.
    def f(rows: list[RowWithUnmappableField]) -> list[RowWithUnmappableField]:
        return rows

    with pytest.warns(UserWarning):
        result = infer_task_interface(f)
    assert result.node_interface is None
