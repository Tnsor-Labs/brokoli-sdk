"""Portable dataset-schema builders."""

from __future__ import annotations

from collections.abc import Mapping
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
