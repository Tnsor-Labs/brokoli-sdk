"""Normalized pipeline IR snapshots and semantic comparison helpers."""

from __future__ import annotations

import copy
import difflib
import hashlib
import json
from collections.abc import Mapping
from typing import Any, Optional


_SERVER_FIELDS = {
    "id",
    "source",
    "workspace_id",
    "org_id",
    "created_at",
    "updated_at",
}
_EMPTY_LIST_DEFAULTS = ("nodes", "edges", "tags", "depends_on", "dependency_rules")
_EMPTY_MAP_DEFAULTS = ("params", "hooks")


def _sort_key(value: Any) -> tuple[str, str]:
    """Return a total ordering key without constraining malformed IR shapes."""
    try:
        rendered = json.dumps(value, sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError, RecursionError):
        rendered = repr(value)
    return type(value).__name__, rendered


def _mapping_sort_key(value: Any, fields: tuple[str, ...]) -> tuple[Any, ...]:
    if not isinstance(value, Mapping):
        return (1, _sort_key(value))
    return (0,) + tuple(_sort_key(value.get(field)) for field in fields)


def normalize_ir(ir: Mapping[str, Any]) -> dict[str, Any]:
    """Return a narrow, deep-copied semantic representation of pipeline IR."""
    normalized = copy.deepcopy(dict(ir))

    for field in _SERVER_FIELDS:
        normalized.pop(field, None)

    for field in _EMPTY_LIST_DEFAULTS:
        if normalized.get(field) is None:
            normalized[field] = []
    for field in _EMPTY_MAP_DEFAULTS:
        if normalized.get(field) is None:
            normalized[field] = {}

    if normalized.get("webhook_url") == "":
        normalized.pop("webhook_url", None)
    if normalized.get("schedule_timezone") in (None, "", "UTC"):
        normalized.pop("schedule_timezone", None)
    if "webhook_token" in normalized:
        normalized["webhook_token"] = ""

    nodes = normalized.get("nodes")
    if isinstance(nodes, list):
        for index, node in enumerate(nodes):
            if not isinstance(node, Mapping):
                continue
            node_copy = dict(node)
            node_copy.pop("position", None)
            capabilities = node_copy.get("capabilities")
            if (capabilities is None or capabilities == []) and isinstance(
                node_copy.get("type"), str
            ):
                # Imported lazily so pipeline serialization can use this module
                # without creating a module import cycle.
                from brokoli.pipeline import _capabilities_for

                node_copy["capabilities"] = sorted(
                    _capabilities_for(node_copy["type"]), key=_sort_key
                )
            elif isinstance(capabilities, list):
                node_copy["capabilities"] = sorted(capabilities, key=_sort_key)
            nodes[index] = node_copy
        nodes.sort(key=lambda node: _mapping_sort_key(node, ("id",)))

    for field in ("tags", "depends_on"):
        values = normalized.get(field)
        if isinstance(values, list):
            values.sort(key=_sort_key)

    return normalized


def canonical_json(value: Any) -> str:
    """Render JSON canonically with exactly one trailing newline.

    The layout is ``json.dumps(value, indent=2, sort_keys=True,
    ensure_ascii=False)``: keys sorted by code point, two-space indent,
    literal UTF-8. Numbers follow the IR canonicalization spec (section 2,
    RFC 8785 section 3.2.2.3): a float is the shortest decimal that reads
    back as itself, laid out as JavaScript's ``JSON.stringify`` writes it
    (``0.00001``, ``1e-7``, ``1e+21``, ``5``). ``json.dumps`` writes some of
    those differently (``1e-05``, ``5.0``), which gave this SDK a different
    digest from the TypeScript SDK for the same pipeline. NaN and the
    infinities are refused.
    """
    return _render(value, 0) + "\n"


_INDENT = "  "


def _render(value: Any, level: int) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return int.__repr__(value)
    if isinstance(value, float):
        return canonical_number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    inner = _INDENT * (level + 1)
    if isinstance(value, Mapping):
        if not value:
            return "{}"
        items = sorted(((_render_key(k), v) for k, v in value.items()), key=lambda item: item[0])
        body = ",\n".join(f"{inner}{json.dumps(k, ensure_ascii=False)}: {_render(v, level + 1)}" for k, v in items)
        return "{\n" + body + "\n" + _INDENT * level + "}"
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        body = ",\n".join(inner + _render(v, level + 1) for v in value)
        return "[\n" + body + "\n" + _INDENT * level + "]"
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _render_key(key: Any) -> str:
    """A mapping key as JSON text, converted the way ``json.dumps`` does."""
    if isinstance(key, str):
        return key
    if key is None or isinstance(key, (bool, int)):
        return json.dumps(key)
    if isinstance(key, float):
        return canonical_number(key)
    raise TypeError(f"keys must be str, int, float, bool or None, not {type(key).__name__}")


def canonical_number(value: float) -> str:
    """A float as canonical JSON text (RFC 8785 section 3.2.2.3).

    The digits are ``repr``'s, which are the shortest that read back as the
    same float, choosing the closest and then the even one, as ECMAScript
    does; only the layout differs from ``repr``. ``-0.0`` is ``0``.
    """
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError(f"Out of range float values are not JSON compliant: {value!r}")
    if value == 0:
        return "0"
    if value < 0:
        return "-" + canonical_number(-value)
    mantissa, _, exponent = repr(value).partition("e")
    whole, _, fraction = mantissa.partition(".")
    digits = (whole + fraction).lstrip("0")
    point = len(whole) + (int(exponent) if exponent else 0)
    point -= len(whole + fraction) - len((whole + fraction).lstrip("0"))
    digits = digits.rstrip("0")
    count = len(digits)
    if count <= point <= 21:
        return digits + "0" * (point - count)
    if 0 < point <= 21:
        return digits[:point] + "." + digits[point:]
    if -6 < point <= 0:
        return "0." + "0" * -point + digits
    e = point - 1
    head = digits if count == 1 else digits[0] + "." + digits[1:]
    return f"{head}e{'-' if e < 0 else '+'}{abs(e)}"


def render_ir(ir: Mapping[str, Any]) -> str:
    """Normalize and canonically render pipeline IR."""
    return canonical_json(normalize_ir(ir))


def ir_digest(ir: Mapping[str, Any]) -> str:
    """A stable content digest of pipeline IR, as ``sha256:<hex>``.

    Hashes the canonical, normalized IR (:func:`render_ir`), so the digest
    identifies exactly what would be deployed while ignoring cosmetic churn
    (key order, layout positions, server-only fields). Two consequences make
    it an audit primitive:

    * a create and a later update of the same content produce the *same*
      digest -- ``normalize_ir`` strips ``id``/timestamps -- so a redeploy
      that changes nothing is recognizable as a no-op;
    * recording the digest of what was deployed lets you later verify a
      running pipeline against the source it was built from.
    """
    canonical = render_ir(ir).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def diff_ir(
    local: Mapping[str, Any],
    remote: Optional[Mapping[str, Any]],
    *,
    local_label: str = "local",
    remote_label: str = "server",
) -> str:
    """Return a unified canonical JSON diff from remote state to local state."""
    local_lines = render_ir(local).splitlines(keepends=True)
    remote_lines = [] if remote is None else render_ir(remote).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            remote_lines,
            local_lines,
            fromfile=remote_label,
            tofile=local_label,
        )
    )
