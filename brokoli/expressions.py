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


def _predicate(op: str, left: Any, right: Any) -> Expression:
    return _binary(op, left, right)


def eq(left: Any, right: Any) -> Expression:
    return _predicate("eq", left, right)


def neq(left: Any, right: Any) -> Expression:
    return _predicate("neq", left, right)


def lt(left: Any, right: Any) -> Expression:
    return _predicate("lt", left, right)


def lte(left: Any, right: Any) -> Expression:
    return _predicate("lte", left, right)


def gt(left: Any, right: Any) -> Expression:
    return _predicate("gt", left, right)


def gte(left: Any, right: Any) -> Expression:
    return _predicate("gte", left, right)


def is_null(value: Any) -> Expression:
    return {"op": "is_null", "arg": _expression(value)}


def logical_not(value: Any) -> Expression:
    return {"op": "not", "arg": _expression(value)}


def all_of(*values: Any) -> Expression:
    if not values:
        raise ValueError("and requires at least one expression")
    return {"op": "and", "args": [_expression(value) for value in values]}


def any_of(*values: Any) -> Expression:
    if not values:
        raise ValueError("or requires at least one expression")
    return {"op": "or", "args": [_expression(value) for value in values]}


def case_when(*branches: tuple[Any, Any], otherwise: Any = None) -> Expression:
    if not branches:
        raise ValueError("case_when requires at least one branch")
    return {
        "op": "case_when",
        "branches": [
            {"when": _expression(when), "then": _expression(then)} for when, then in branches
        ],
        "else": _expression(otherwise),
    }
