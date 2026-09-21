"""Language-neutral scalar expression builders for native dataset operators."""

from __future__ import annotations

from typing import Any

Expression = dict[str, Any]


def _expression(value: Any) -> Expression:
    if isinstance(value, dict) and isinstance(value.get("op"), str):
        return dict(value)
    return literal(value)


def column(*path: str) -> Expression:
    if not path or any(not isinstance(part, str) or not part for part in path):
        raise ValueError("column requires one or more non-empty string path segments")
    return {"op": "column", "path": list(path)}


def literal(value: Any) -> Expression:
    return {"op": "literal", "value": value}


def _binary(op: str, left: Any, right: Any) -> Expression:
    return {"op": op, "left": _expression(left), "right": _expression(right)}


def add(left: Any, right: Any) -> Expression:
    return _binary("add", left, right)


def subtract(left: Any, right: Any) -> Expression:
    return _binary("subtract", left, right)


def multiply(left: Any, right: Any) -> Expression:
    return _binary("multiply", left, right)


def divide(left: Any, right: Any) -> Expression:
    return _binary("divide", left, right)


def concat(left: Any, right: Any) -> Expression:
    return _binary("concat", left, right)


def coalesce(*values: Any) -> Expression:
    if not values:
        raise ValueError("coalesce requires at least one expression")
    return {"op": "coalesce", "args": [_expression(value) for value in values]}
