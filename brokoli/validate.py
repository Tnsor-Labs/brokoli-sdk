"""Pipeline validation — catch errors at deploy time, not runtime."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Callable

from brokoli.parsing import ParseError  # noqa: F401 — re-exported for consumers

REQUEST_TIMEOUT = 10

ALLOWED_DBT_COMMANDS = {"run", "test", "build", "seed", "snapshot", "compile", "debug", "clean"}


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


def _validate_source_file(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("path"):
        result.add_error(name, "path", "Source File requires a 'path'")


def _validate_transform(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("rules"):
        result.add_warning(name, "rules", "Transform has no rules — will pass data through unchanged")


def _validate_quality_check(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("rules"):
        result.add_error(name, "rules", "Quality Check requires at least one rule")


def _validate_code(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("script"):
        result.add_error(name, "script", "Code node requires a 'script'")


def _validate_condition(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("expression") and not config.get("script"):
        result.add_error(name, "expression", "Condition node requires an 'expression' or 'script'")


def _validate_sink_db(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("table"):
        result.add_error(name, "table", "Sink DB requires a 'table'")
    if not config.get("conn_id") and not config.get("uri"):
        result.add_error(name, "conn_id", "Sink DB requires 'conn_id' or 'uri'")


def _validate_sink_file(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("path"):
        result.add_error(name, "path", "Sink File requires a 'path'")


def _validate_sink_api(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("url"):
        result.add_error(name, "url", "Sink API requires a 'url'")


def _validate_join(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("left_key") and not config.get("right_key"):
        result.add_error(name, "on", "Join requires join keys (on='left_key=right_key')")


def _validate_dbt(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    command = config.get("command", "")
    if not command:
        result.add_error(name, "command", "dbt node requires a 'command'")
        return
    if command not in ALLOWED_DBT_COMMANDS:
        result.add_error(
            name, "command",
            f"dbt command '{command}' is not allowed. Must be one of: {', '.join(sorted(ALLOWED_DBT_COMMANDS))}",
        )


def _validate_notify(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    if not config.get("webhook_url"):
        result.add_error(name, "webhook_url", "Notify node requires a 'webhook_url'")


def _validate_migrate(name: str, config: dict[str, Any], result: ValidationResult) -> None:
    has_uris = config.get("source_uri") and config.get("target_uri")
    has_conns = config.get("source_conn_id") and config.get("target_conn_id")
    if not has_uris and not has_conns:
        result.add_error(
            name, "conn",
            "Migrate node requires 'source_uri' + 'target_uri' or 'source_conn_id' + 'target_conn_id'",
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

    for node in data["nodes"]:
        name: str = node["name"]
        ntype: str = node["type"]
        config: dict[str, Any] = node.get("config", {})

        if name in seen_names:
            result.add_warning(name, "name", "Duplicate node name (also used by another node)")
        seen_names.add(name)

        validator = _NODE_VALIDATORS.get(ntype)
        if validator is not None:
            validator(name, config, result)

    # Edge validation
    for edge in data.get("edges", []):
        if edge["from"] not in node_ids:
            result.add_error("", "edge", f"Edge references unknown source node: {edge['from'][:12]}")
        if edge["to"] not in node_ids:
            result.add_error("", "edge", f"Edge references unknown target node: {edge['to'][:12]}")

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
            "", "server",
            f"Could not connect to {server_url}: {reason} — skipping conn_id validation",
        )
        return
    except Exception as exc:
        result.add_warning(
            "", "server",
            f"Error fetching connections from {server_url}: {exc} — skipping conn_id validation",
        )
        return

    for cid, node_name in conn_ids_used.items():
        if cid not in server_conn_ids:
            result.add_error(
                node_name, "conn_id",
                f"Connection '{cid}' does not exist on the server. Create it in Connections page first.",
            )
