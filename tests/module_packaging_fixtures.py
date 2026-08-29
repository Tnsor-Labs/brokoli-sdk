"""Fixture module for module-packaging auto-detection tests (brokoli-sdk#3).

Deliberately a real, importable module (not functions defined inline inside
a test method) -- ``inspect.getsource``/``inspect.getmodule`` need a real
source file to find module-level constants, helpers, and imports.
"""

from __future__ import annotations

import json as _json_alias

# --- Module-level constants referenced by tasks below ---
API_BASE = "https://api.example.com"
DEFAULT_LIMIT = 100
TAGS = ["a", "b", "c"]


# --- Module-level helper referenced by a task below ---
def _normalize(row: dict, base: str) -> dict:
    row["_base"] = base
    return row


def _double_normalize(row: dict) -> dict:
    """Helper that itself calls another module-level helper (transitive)."""
    return _normalize(row, API_BASE)


# --- Task functions (undecorated -- decorated manually in tests via
#     ``task(fx.some_func)`` so each test controls the active Pipeline) ---


def clean_no_refs(rows):
    """References nothing outside its own body -- pins legacy behavior."""
    return [r for r in rows if r]


def clean_with_constant(rows):
    """References a single module-level constant."""
    return [{"base": API_BASE, **r} for r in rows]


def clean_with_helper(rows):
    """References a module-level helper, which itself references a
    module-level constant -- exercises transitive inclusion."""
    return [_normalize(r, API_BASE) for r in rows]


def clean_with_transitive_helper(rows):
    """References a helper that calls another helper that uses a constant."""
    return [_double_normalize(r) for r in rows]


def clean_with_import(rows):
    """References a name bound by a top-level import in this module."""
    return [_json_alias.dumps(r) for r in rows]


class _NotSerializable:
    """A plain class instance -- not JSON-serializable, not a function,
    not imported -- the case auto-detection must reject."""

    def __init__(self) -> None:
        self.value = 1


UNINCLUDABLE_INSTANCE = _NotSerializable()


def clean_with_bad_ref(rows):
    """References a module-level value that can't be auto-included."""
    return [{"v": UNINCLUDABLE_INSTANCE.value, **r} for r in rows]


def clean_with_builtin_only(rows):
    """Uses builtins (len, dict, sorted) -- these must NOT be treated as
    external references needing inclusion."""
    return sorted(rows, key=len)[: len(rows)]


INFINITY_THRESHOLD = float("inf")

NESTED_NAN = {"bounds": {"upper": float("nan")}}


def clean_with_inf_ref(rows):
    """References a module-level infinity -- json-dumpable by default but
    not emittable as a valid Python literal via repr()."""
    return [r for r in rows if r.get("score", 0) < INFINITY_THRESHOLD]


def clean_with_nested_nan_ref(rows):
    """Same failure nested inside an otherwise-serializable dict."""
    return [dict(r, **NESTED_NAN) for r in rows]


async def async_helper(rows):
    """An async helper a sync task might reference."""
    return rows


def clean_with_async_helper(rows):
    """A sync task referencing an async module-level helper."""
    return async_helper(rows)


def shared_helper(row):
    """Module-level helper captured through closures in tests."""
    return dict(row, seen=True)


def make_work_with_local_helper():
    """Factory whose task closes over a factory-local helper function."""

    def local_scale(row):
        return dict(row, scaled=True)

    def work(rows):
        return [local_scale(r) for r in rows]

    return work


import functools


@functools.lru_cache(maxsize=None)
def cached_lookup(key):
    """A decorated helper -- its decorator must survive packaging."""
    return {"a": 1, "b": 2}.get(key, 0)


def work_with_cached_helper(rows):
    """Task referencing a decorated helper."""
    return [dict(r, v=cached_lookup(r.get("k", "a"))) for r in rows]
