"""Portable dataset-schema builders."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from brokoli.exceptions import PipelineError

_BPTD_KINDS = {
    "boolean",
    "int64",
    "float64",
    "decimal",
    "string",
    "bytes",
    "date",
    "timestamp",
    "duration",
    "json",
    "unknown",
    "enum",
    "array",
    "map",
    "record",
}
_ADDITIONAL_COLUMN_MODES = {"closed", "open", "unknown"}


def _validate_bptd(value: Any, path: str) -> None:
    if not isinstance(value, dict):
        raise PipelineError(f"{path} must be a BPTD descriptor object")
    kind = value.get("kind")
    if kind not in _BPTD_KINDS:
        raise PipelineError(f"{path} has an invalid BPTD kind {kind!r}")
    if kind == "enum":
        values = value.get("values")
        if not isinstance(values, list) or not values or not all(isinstance(item, str) for item in values):
            raise PipelineError(f"{path}.values must be a non-empty list of strings")
    elif kind == "array":
        _validate_bptd(value.get("items"), f"{path}.items")
    elif kind == "map":
        if value.get("keys", "string") != "string":
            raise PipelineError(f"{path}.keys must be 'string'")
        _validate_bptd(value.get("values"), f"{path}.values")
    elif kind == "record":
        fields = value.get("fields")
        if not isinstance(fields, list):
            raise PipelineError(f"{path}.fields must be a list")
        seen: set[str] = set()
        for index, field in enumerate(fields):
            field_path = f"{path}.fields[{index}]"
            if not isinstance(field, dict) or not isinstance(field.get("name"), str) or not field["name"]:
                raise PipelineError(f"{field_path} requires a non-empty field name")
            if field["name"] in seen:
                raise PipelineError(f"{field_path} duplicates field {field['name']!r}")
            seen.add(field["name"])
            _validate_bptd(field.get("type"), f"{field_path}.type")


def dataset_schema(
    columns: Mapping[str, Mapping[str, Any]],
    additional_columns: str = "unknown",
) -> dict[str, Any]:
    """Build a portable ``brokoli.dataset-schema/v1`` declaration.

    ``columns`` preserves insertion order and maps each output column name to
    one BPTD descriptor. Runtime data may contain undeclared columns only when
    ``additional_columns`` is ``open`` or ``unknown``.
    """
    if not isinstance(columns, Mapping):
        raise PipelineError("dataset_schema columns must be a mapping of name to BPTD descriptor")
    if additional_columns not in _ADDITIONAL_COLUMN_MODES:
        raise PipelineError(
            "dataset_schema additional_columns must be one of "
            f"{sorted(_ADDITIONAL_COLUMN_MODES)}, got {additional_columns!r}"
        )

    output_columns = []
    for name, descriptor in columns.items():
        if not isinstance(name, str) or not name:
            raise PipelineError(f"dataset_schema column name must be a non-empty string, got {name!r}")
        _validate_bptd(descriptor, f"column {name!r}")
        output_columns.append({"name": name, "type": dict(descriptor)})

    return {
        "contract": "brokoli.dataset-schema/v1",
        "columns": output_columns,
        "additional_columns": additional_columns,
    }


def join_dataset_schema(
    left: Mapping[str, Any] | None,
    right: Mapping[str, Any] | None,
    left_key: str,
    right_key: str,
    collision_policy: str = "prefix",
    right_alias: str = "",
) -> dict[str, Any] | None:
    """Derive a join output schema when both input schemas are authoritative."""
    if left is None or right is None:
        return None
    left_columns = left.get("columns")
    right_columns = right.get("columns")
    if not isinstance(left_columns, list) or not isinstance(right_columns, list):
        return None

    left_by_name = {column["name"]: column for column in left_columns}
    right_by_name = {column["name"]: column for column in right_columns}
    if left_key not in left_by_name or right_key not in right_by_name:
        raise PipelineError("declared join schemas do not contain both join keys")
    left_kind = left_by_name[left_key]["type"].get("kind")
    right_kind = right_by_name[right_key]["type"].get("kind")
    if left_kind != right_kind and left_kind != "unknown" and right_kind != "unknown":
        raise PipelineError(
            f"join keys {left_key!r} and {right_key!r} have incompatible declared types "
            f"{left_kind!r} and {right_kind!r}"
        )

    collisions = [
        column["name"]
        for column in right_columns
        if column["name"] in left_by_name and not (column["name"] == right_key and left_key == right_key)
    ]
    if collision_policy == "error" and collisions:
        raise PipelineError(f"join collision_policy='error' rejected columns: {', '.join(collisions)}")
    if collision_policy == "alias" and not right_alias.strip():
        raise PipelineError("join collision_policy='alias' requires right_alias")
    if collision_policy not in {"error", "prefix", "alias"}:
        raise PipelineError(f"unsupported join collision_policy {collision_policy!r}")

    output = [deepcopy(column) for column in left_columns]
    used = {column["name"] for column in output}
    for column in right_columns:
        name = column["name"]
        if name == right_key and left_key == right_key:
            continue
        output_name = name
        if collision_policy == "alias":
            output_name = f"{right_alias}_{name}"
        elif collision_policy == "prefix" and collisions:
            output_name = f"right_{name}"
            while output_name in used:
                output_name = f"right_{output_name}"
        if output_name in used:
            raise PipelineError(f"join output schema cannot represent column {output_name!r} uniquely")
        used.add(output_name)
        derived = deepcopy(column)
        derived["name"] = output_name
        output.append(derived)

    additional = "closed" if left.get("additional_columns") == right.get("additional_columns") == "closed" else "unknown"
    return {
        "contract": "brokoli.dataset-schema/v1",
        "columns": output,
        "additional_columns": additional,
    }
