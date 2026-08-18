"""Pipeline validation — catch errors at deploy time, not runtime."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Callable

from brokoli.pagination import VALID_PAGINATION_STRATEGIES
from brokoli.parsing import ParseError  # noqa: F401 — re-exported for consumers

REQUEST_TIMEOUT = 10

ALLOWED_DBT_COMMANDS = {"run", "test", "build", "seed", "snapshot", "compile", "debug", "clean"}

VALID_SOURCE_API_RESPONSES = {"dataset", "scalar", "artifact"}


class ValidationIssue:
    """A single validation issue."""

    def __init__(self, node_name: str, field: str, message: str, severity: str = "error") -> None:
        self.node_name = node_name
        self.field = field
        self.message = message
        self.severity = severity

    def __str__(self) -> str:
        prefix = "ERROR" if self.severity == "error" else "WARN"
        if self.node_name:
            return f"[{prefix}] {self.node_name}: {self.message}"
        return f"[{prefix}] Pipeline: {self.message}"


class ValidationResult:
    """Collection of validation errors/warnings."""

    def __init__(self) -> None:
        self.errors: list[ValidationIssue] = []
        self.warnings: list[ValidationIssue] = []

    def add_error(self, node_name: str, field: str, message: str) -> None:
        self.errors.append(ValidationIssue(node_name, field, message, "error"))

    def add_warning(self, node_name: str, field: str, message: str) -> None:
        self.warnings.append(ValidationIssue(node_name, field, message, "warning"))

    @property
    def valid(self) -> bool:
        return len(self.errors) == 0

    def print_report(self) -> None:
        for e in self.errors:
            print(f"  ✗ {e}")
        for w in self.warnings:
            print(f"  ! {w}")
        if self.valid and not self.warnings:
            print("  ✓ All checks passed")


# ── Per-node-type validators ──────────────────────────────────────────────────
#
# Each validator receives (node_name, config, result) and adds errors/warnings.
# Early-return style: check required fields and bail.


def _validate_source_db(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("query"):
        result.add_error(name, "query", "Source DB requires a 'query'")
    if not config.get("conn_id") and not config.get("uri"):
        result.add_error(name, "conn_id", "Source DB requires 'conn_id' or 'uri'")


def _validate_source_api(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("url"):
        result.add_error(name, "url", "Source API requires a 'url'")

    response = config.get("response", "dataset")
    if response not in VALID_SOURCE_API_RESPONSES:
        result.add_error(
            name,
            "response",
            f"Source API 'response' must be one of {sorted(VALID_SOURCE_API_RESPONSES)}, "
            f"got {response!r}",
        )

    records = config.get("records")
    value_path = config.get("value_path")
    if records is not None and value_path is not None:
        result.add_error(
            name,
            "records",
            "Source API cannot set both 'records' and 'value_path' — "
            "'records' extracts a list for response='dataset', 'value_path' "
            "extracts a single value for response='scalar'; use only one",
        )

    pagination = config.get("pagination")
    if pagination is not None:
        if response != "dataset":
            result.add_error(
                name,
                "pagination",
                f"Source API 'pagination' requires response='dataset' (got response={response!r})",
            )
        if not isinstance(pagination, dict):
            result.add_error(
                name,
                "pagination",
                "Source API 'pagination' must be a dict-shaped config "
                "(build it with brokoli.pagination.offset_pages(...) or similar)",
            )
        else:
            strategy = pagination.get("strategy")
            if strategy not in VALID_PAGINATION_STRATEGIES:
                result.add_error(
                    name,
                    "pagination",
                    f"Unknown pagination strategy {strategy!r}. Must be one of "
                    f"{sorted(VALID_PAGINATION_STRATEGIES)}",
                )


def _validate_source_file(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("path"):
        result.add_error(name, "path", "Source File requires a 'path'")


def _validate_transform(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("rules"):
        result.add_warning(
            name, "rules", "Transform has no rules — will pass data through unchanged"
        )


def _validate_quality_check(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("rules"):
        result.add_error(name, "rules", "Quality Check requires at least one rule")


def _validate_code(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("script"):
        result.add_error(name, "script", "Code node requires a 'script'")

    # ``.expand()`` (brokoli-sdk#2) attaches an "expansion" policy block
    # to an otherwise-ordinary "code" node instead of introducing a new
    # node type -- validate its shape here alongside the rest of "code".
    expansion = config.get("expansion")
    if expansion is not None and not expansion.get("over"):
        result.add_error(
            name,
            "expansion",
            "expand() requires at least one keyword mapping a task "
            "parameter to a CollectionRef, e.g. .expand(file=files)",
        )


def _validate_union(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if config.get("mode") != "union":
        result.add_error(
            name,
            "mode",
            f"Union node currently only supports mode='union', got {config.get('mode')!r}",
        )


def _validate_dataset_transform(
    name: str, config: dict[str, Any], result: ValidationResult
) -> None:
    """Shared validator for the ``dataset_map``/``dataset_filter`` partition-transform nodes."""
    function = config.get("function")
    if not isinstance(function, dict) or not function.get("name"):
        result.add_error(name, "function", "requires a function reference with a 'name'")


def _validate_condition(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("expression") and not config.get("script"):
        result.add_error(name, "expression", "Condition node requires an 'expression' or 'script'")


# Enumerations transcribed from the core engine's switches; anything
# outside them is silently coerced server-side (join defaults to inner,
# sink_file to json), which is exactly the silent-mismatch class this
# validator exists to catch.
_JOIN_TYPES = {"inner", "left", "right", "full", "outer", "full_outer"}
_NOTIFY_TYPES = {"slack", "webhook"}
# sink_db and migrate both generate the write SQL from the rows and honor
# the same write modes.
_SINK_DB_MODES = {"append", "overwrite", "upsert"}
_MIGRATE_MODES = {"append", "overwrite", "upsert"}
_RETRY_BACKOFFS = {"fixed", "exponential", "linear"}
_SINK_FILE_FORMATS = {"csv", "json", "sql"}


def _validate_enum(name, config, key, allowed, result):
    value = config.get(key)
    if value is not None and value != "" and value not in allowed:
        result.add_error(
            name,
            key,
            f"'{key}' value {value!r} is not one of {sorted(allowed)} -- the "
            "server would silently coerce it rather than honor it",
        )


def _validate_sink_db(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("table"):
        result.add_error(name, "table", "Sink DB requires a 'table'")
    if not config.get("conn_id") and not config.get("uri"):
        result.add_error(name, "conn_id", "Sink DB requires 'conn_id' or 'uri'")
    _validate_enum(name, config, "mode", _SINK_DB_MODES, result)
    if config.get("mode") == "upsert" and not config.get("key_columns"):
        result.add_error(
            name,
            "key_columns",
            "Sink DB mode='upsert' requires 'key_columns' -- the column(s) a "
            "row collides on, e.g. key_columns=['id']",
        )


def _validate_sink_file(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("path"):
        result.add_error(name, "path", "Sink File requires a 'path'")
    _validate_enum(name, config, "format", _SINK_FILE_FORMATS, result)


def _validate_sink_api(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("url"):
        result.add_error(name, "url", "Sink API requires a 'url'")


def _validate_join(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("left_key") and not config.get("right_key"):
        result.add_error(name, "on", "Join requires join keys (left_key=... / right_key=...)")
    _validate_enum(name, config, "join_type", _JOIN_TYPES, result)


def _validate_dbt(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    command = config.get("command", "")
    if not command:
        result.add_error(name, "command", "dbt node requires a 'command'")
        return
    if command not in ALLOWED_DBT_COMMANDS:
        result.add_error(
            name,
            "command",
            f"dbt command '{command}' is not allowed. Must be one of: {', '.join(sorted(ALLOWED_DBT_COMMANDS))}",
        )


def _validate_notify(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("webhook_url"):
        result.add_error(name, "webhook_url", "Notify node requires a 'webhook_url'")
    _validate_enum(name, config, "notify_type", _NOTIFY_TYPES, result)


def _validate_migrate(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    # Match the config keys the migrate factory actually emits: source_uri /
    # dest_uri and source_conn_id / dest_conn_id. (The check previously looked
    # for target_* keys that never exist, so it always failed.)
    has_uris = config.get("source_uri") and config.get("dest_uri")
    has_conns = config.get("source_conn_id") and config.get("dest_conn_id")
    if not has_uris and not has_conns:
        result.add_error(
            name,
            "conn",
            "Migrate node requires 'source_uri' + 'target_uri' or 'source_conn_id' + 'target_conn_id'",
        )
    _validate_enum(name, config, "mode", _MIGRATE_MODES, result)
    if config.get("mode") == "upsert" and not config.get("key_columns"):
        result.add_error(
            name,
            "key_columns",
            "Migrate mode='upsert' requires 'key_columns' -- the column(s) a "
            "row collides on, e.g. key_columns=['id']",
        )


# Dispatch table: node type -> validator
_NODE_VALIDATORS: dict[str, Callable[[str, dict[str, Any], ValidationResult], None]] = {
    "source_db": _validate_source_db,
    "source_api": _validate_source_api,
    "source_file": _validate_source_file,
    "transform": _validate_transform,
    "quality_check": _validate_quality_check,
    "code": _validate_code,
    "condition": _validate_condition,
    "sink_db": _validate_sink_db,
    "sink_file": _validate_sink_file,
    "sink_api": _validate_sink_api,
    "join": _validate_join,
    "dbt": _validate_dbt,
    "notify": _validate_notify,
    "migrate": _validate_migrate,
    "union": _validate_union,
    "dataset_map": _validate_dataset_transform,
    "dataset_filter": _validate_dataset_transform,
}


# ── Public API ────────────────────────────────────────────────────────────────


def validate_pipeline(
    pipeline: Any,
    server_url: str = "",
    auth_header: str = "",
) -> ValidationResult:
    """Validate a pipeline definition before deploy.

    Checks:
    - Pipeline has a name and at least one node
    - All nodes have required config fields (dispatched per type)
    - All edges reference valid node IDs
    - No orphan nodes (disconnected from the DAG)
    - conn_id references exist on the server (if server_url provided)
    - No duplicate node names
    """
    result = ValidationResult()
    data: dict[str, Any] = pipeline.to_json()

    # Pipeline-level
    if not data.get("name"):
        result.add_error("", "name", "Pipeline name is required")

    if not data.get("nodes"):
        result.add_error("", "nodes", "Pipeline must have at least one node")
        return result

    # Node validation
    node_ids: set[str] = {n["id"] for n in data["nodes"]}
    seen_names: set[str] = set()
    has_source = False

    for node in data["nodes"]:
        name: str = node["name"]
        ntype: str = node["type"]
        config: dict[str, Any] = node.get("config", {})
        capabilities: list[str] = node.get("capabilities", [])

        if "source" in capabilities:
            has_source = True

        if name in seen_names:
            result.add_warning(name, "name", "Duplicate node name (also used by another node)")
        seen_names.add(name)

        # Config validation stays dispatched by exact node type -- each
        # type has its own required-field shape. Only the "does this
        # pipeline have a source" question below is answered generically
        # via the capability model, so that dbt/migrate and any
        # @source-decorated node are recognized as sources without
        # hardcoding their type strings here.
        validator = _NODE_VALIDATORS.get(ntype)
        if validator is not None:
            validator(name, config, result)
        # retry_backoff is accepted on several node types; the engine
        # only implements these strategies.
        _validate_enum(name, config, "retry_backoff", _RETRY_BACKOFFS, result)

    if not has_source:
        result.add_warning(
            "",
            "capabilities",
            "Pipeline has no source node — nothing produces data for downstream nodes to consume",
        )

    # Edge validation
    for edge in data.get("edges", []):
        if edge["from"] not in node_ids:
            result.add_error(
                "", "edge", f"Edge references unknown source node: {edge['from'][:12]}"
            )
        if edge["to"] not in node_ids:
            result.add_error("", "edge", f"Edge references unknown target node: {edge['to'][:12]}")

    # Arity and topology, mirroring engine/validate.go exactly: join takes
    # exactly 2 inputs, condition exactly 1, and in a multi-node pipeline
    # every node except migrate must touch an edge ("disconnected" is a
    # server-side ERROR since v0.10.10, not a style nit).
    indegree_by_node: dict[str, int] = {nid: 0 for nid in node_ids}
    touched: set[str] = set()
    for edge in data.get("edges", []):
        if edge["to"] in indegree_by_node:
            indegree_by_node[edge["to"]] += 1
        touched.add(edge["from"])
        touched.add(edge["to"])
    types_by_id = {n["id"]: n["type"] for n in data["nodes"]}
    names_by_id = {n["id"]: n["name"] for n in data["nodes"]}
    for nid, ntype in types_by_id.items():
        if ntype == "join" and indegree_by_node[nid] != 2:
            result.add_error(
                names_by_id[nid],
                "inputs",
                f"join requires exactly 2 inputs, got {indegree_by_node[nid]}",
            )
        if ntype == "condition" and indegree_by_node[nid] != 1:
            result.add_error(
                names_by_id[nid],
                "inputs",
                f"condition requires exactly 1 input, got {indegree_by_node[nid]}",
            )
    if len(node_ids) > 1:
        for nid in node_ids:
            if nid not in touched and types_by_id.get(nid) != "migrate":
                result.add_error(
                    names_by_id[nid],
                    "edges",
                    "node is disconnected -- the server rejects this at save",
                )

    # Cycle detection (Kahn's): the server rejects cycles at save time
    # since v0.10.10; catching them locally names the members instead of
    # a deploy-time 400.
    indegree = {nid: 0 for nid in node_ids}
    adjacency: dict[str, list[str]] = {nid: [] for nid in node_ids}
    for edge in data.get("edges", []):
        src, dst = edge["from"], edge["to"]
        if src in node_ids and dst in node_ids:
            adjacency[src].append(dst)
            indegree[dst] += 1
    queue = [nid for nid, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        current = queue.pop()
        visited += 1
        for nxt in adjacency[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if visited < len(node_ids):
        members = sorted(nid for nid, deg in indegree.items() if deg > 0)
        result.add_error(
            "",
            "edges",
            "Pipeline contains a cycle involving: " + ", ".join(members),
        )

    # Orphan detection
    connected: set[str] = set()
    for edge in data.get("edges", []):
        connected.add(edge["from"])
        connected.add(edge["to"])

    if len(data["nodes"]) > 1:
        for node in data["nodes"]:
            if node["id"] not in connected:
                result.add_warning(node["name"], "edges", "Node is disconnected from the DAG")

    # Server-side validation (conn_id existence)
    if server_url:
        _validate_connections(data, server_url, auth_header, result)

    return result


def _validate_connections(
    data: dict[str, Any],
    server_url: str,
    auth_header: str,
    result: ValidationResult,
) -> None:
    """Check that all conn_id references exist on the server."""
    conn_ids_used: dict[str, str] = {}
    for node in data["nodes"]:
        config = node.get("config", {})
        cid = config.get("conn_id", "")
        if cid:
            conn_ids_used[cid] = node["name"]

    if not conn_ids_used:
        return

    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header

    try:
        req = urllib.request.Request(f"{server_url}/api/connections", headers=headers)
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
        server_conns = json.loads(resp.read())
        server_conn_ids = {c.get("conn_id", c.get("id", "")) for c in server_conns}
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", str(exc))
        result.add_warning(
            "",
            "server",
            f"Could not connect to {server_url}: {reason} — skipping conn_id validation",
        )
        return
    except Exception as exc:
        result.add_warning(
            "",
            "server",
            f"Error fetching connections from {server_url}: {exc} — skipping conn_id validation",
        )
        return

    for cid, node_name in conn_ids_used.items():
        if cid not in server_conn_ids:
            result.add_error(
                node_name,
                "conn_id",
                f"Connection '{cid}' does not exist on the server. Create it in Connections page first.",
            )
