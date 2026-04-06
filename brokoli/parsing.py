"""Quality rule parser — converts string rules to structured dicts."""

from __future__ import annotations

import re
from typing import Any


class ParseError(Exception):
    """Raised when a quality rule string cannot be parsed."""

    def __init__(self, rule_string: str, reason: str) -> None:
        self.rule_string = rule_string
        super().__init__(f"Cannot parse rule '{rule_string}': {reason}")


# Pattern: function_name(args...)
_RULE_PATTERN = re.compile(r"^(\w+)\s*\(\s*(.*?)\s*\)$", re.DOTALL)


def parse_quality_rule(rule_str: str) -> dict[str, Any]:
    """Parse a quality rule string into a structured dict.

    Args:
        rule_str: Rule in format ``rule_name(args...)``.

    Returns:
        Dict with keys: ``column``, ``rule``, ``params``, ``on_failure``.

    Raises:
        ParseError: If the rule string is malformed.

    Examples::

        >>> parse_quality_rule("not_null(email)")
        {"column": "email", "rule": "not_null", "params": {}, "on_failure": "block"}

        >>> parse_quality_rule("min(age, 0)")
        {"column": "age", "rule": "min", "params": {"min": "0"}, "on_failure": "warn"}

        >>> parse_quality_rule("row_count(min=100, max=10000)")
        {"column": "", "rule": "row_count", "params": {"min": "100", "max": "10000"}, "on_failure": "block"}
    """
    rule_str = rule_str.strip()

    match = _RULE_PATTERN.match(rule_str)
    if not match:
        raise ParseError(rule_str, "expected format: rule_name(args...)")

    func_name = match.group(1)
    args_str = match.group(2)

    parser = _PARSERS.get(func_name)
    if parser is None:
        raise ParseError(rule_str, f"unknown rule '{func_name}'")

    return parser(func_name, args_str, rule_str)


def _split_args(args_str: str) -> list[str]:
    """Split comma-separated args, respecting quoted strings."""
    if not args_str:
        return []
    return [a.strip() for a in args_str.split(",")]


def _parse_column_only(func_name: str, args_str: str, raw: str) -> dict:
    """Parse rules that take a single column: not_null(col), unique(col)."""
    args = _split_args(args_str)
    if len(args) != 1 or not args[0]:
        raise ParseError(raw, f"{func_name}() requires exactly 1 argument: column name")
    return {
        "column": args[0],
        "rule": func_name,
        "params": {},
        "on_failure": "block",
    }


def _parse_column_value(func_name: str, args_str: str, raw: str) -> dict:
    """Parse rules that take column + value: min(col, val), max(col, val)."""
    args = _split_args(args_str)
    if len(args) < 2:
        raise ParseError(raw, f"{func_name}() requires 2 arguments: column, value")
    return {
        "column": args[0],
        "rule": func_name,
        "params": {func_name: args[1]},
        "on_failure": "warn",
    }


def _parse_range(func_name: str, args_str: str, raw: str) -> dict:
    """Parse range(column, min, max)."""
    args = _split_args(args_str)
    if len(args) < 3:
        raise ParseError(raw, "range() requires 3 arguments: column, min, max")
    return {
        "column": args[0],
        "rule": "range",
        "params": {"min": args[1], "max": args[2]},
        "on_failure": "warn",
    }


def _parse_kwargs(func_name: str, args_str: str, raw: str) -> dict:
    """Parse rules with keyword args: row_count(min=N, max=M)."""
    params: dict[str, str] = {}
    for part in _split_args(args_str):
        if "=" not in part:
            raise ParseError(raw, f"expected key=value, got '{part}'")
        key, value = part.split("=", 1)
        params[key.strip()] = value.strip()
    return {
        "column": "",
        "rule": func_name,
        "params": params,
        "on_failure": "block",
    }


def _parse_freshness(func_name: str, args_str: str, raw: str) -> dict:
    """Parse freshness(column, max_hours=N)."""
    args = _split_args(args_str)
    if not args:
        raise ParseError(raw, "freshness() requires at least a column name")

    column = args[0]
    params: dict[str, str] = {}
    for part in args[1:]:
        if "=" not in part:
            raise ParseError(raw, f"expected key=value after column, got '{part}'")
        key, value = part.split("=", 1)
        params[key.strip()] = value.strip()

    return {
        "column": column,
        "rule": "freshness",
        "params": params,
        "on_failure": "warn",
    }


def _parse_regex(func_name: str, args_str: str, raw: str) -> dict:
    """Parse regex(column, pattern)."""
    args = _split_args(args_str)  # Only split on first comma for pattern
    if len(args) < 2:
        raise ParseError(raw, "regex() requires 2 arguments: column, pattern")
    # Rejoin everything after column as the pattern (may contain commas)
    pattern = ",".join(args[1:]).strip()
    return {
        "column": args[0],
        "rule": "regex",
        "params": {"pattern": pattern},
        "on_failure": "warn",
    }


def _parse_type_check(func_name: str, args_str: str, raw: str) -> dict:
    """Parse type_check(column, expected_type)."""
    args = _split_args(args_str)
    if len(args) < 2:
        raise ParseError(raw, "type_check() requires 2 arguments: column, type")
    return {
        "column": args[0],
        "rule": "type_check",
        "params": {"expected_type": args[1]},
        "on_failure": "warn",
    }


# Dispatch table — maps rule name to parser function
_PARSERS = {
    "not_null": _parse_column_only,
    "unique": _parse_column_only,
    "min": _parse_column_value,
    "max": _parse_column_value,
    "range": _parse_range,
    "row_count": _parse_kwargs,
    "freshness": _parse_freshness,
    "regex": _parse_regex,
    "type_check": _parse_type_check,
}
