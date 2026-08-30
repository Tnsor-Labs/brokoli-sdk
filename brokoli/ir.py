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
    """Render JSON canonically with exactly one trailing newline."""
    return json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"


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
