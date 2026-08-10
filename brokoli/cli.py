"""Brokoli CLI — deploy, run, pull pipelines."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import urllib.request
import urllib.error
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from brokoli.compatibility import preflight_server_compatibility
from brokoli.exceptions import CompatibilityError, DeployError, ValidationError
from brokoli.ir import canonical_json, diff_ir, normalize_ir

REQUEST_TIMEOUT = 10


def _collect_files(filepath: str) -> list[Path]:
    """Resolve a file path or directory to a list of .py files."""
    if os.path.isdir(filepath):
        return sorted(Path(filepath).glob("*.py"))
    return [Path(filepath)]


def _auth_header_from_args(args: argparse.Namespace) -> str:
    """Build an Authorization header value from CLI args / env."""
    token = os.getenv("BROKOLI_TOKEN", "")
    if token:
        return f"Bearer {token}"
    if getattr(args, "api_key", ""):
        return f"Bearer {args.api_key}"
    return ""


def _make_headers(auth_header: str, content_type: str | None = None) -> dict[str, str]:
    """Build a headers dict, omitting empty values."""
    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def _pipeline_module_name(filepath: str) -> str:
    """A unique, readable sys.modules name for a loaded pipeline file."""
    stem = re.sub(r"\W+", "_", Path(filepath).stem) or "brokoli_pipeline"
    name = f"_brokoli_{stem}"
    suffix = 1
    while name in sys.modules:
        suffix += 1
        name = f"_brokoli_{stem}_{suffix}"
    return name


def load_pipeline_from_file(filepath: str) -> list[Any]:
    """Import a Python file and extract all Pipeline objects.

    The module is registered in ``sys.modules`` (as the ``importlib`` docs
    require for ``module_from_spec``) and stays registered for the process
    lifetime: task packaging resolves each function's containing module via
    ``inspect.getmodule`` at serialization time, which walks
    ``sys.modules`` -- without the registration, import detection came back
    empty and any task referencing a top-level import failed to package
    under the CLI while working when the same file was imported normally.
    Each file gets a unique module name so a directory deploy doesn't make
    later files shadow earlier ones.

    Raises:
        DeployError: If the file cannot be loaded or contains no pipelines.
    """
    from brokoli.pipeline import Pipeline

    module_name = _pipeline_module_name(filepath)
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    if spec is None or spec.loader is None:
        raise DeployError(filepath, 0, f"Cannot load module from {filepath}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[module_name]
        raise

    pipelines = [obj for obj in vars(module).values() if isinstance(obj, Pipeline)]
    if not pipelines:
        raise DeployError(
            filepath, 0,
            f"No Pipeline found in {filepath}. Use: with Pipeline('name') as p: ...",
        )

    return pipelines


def _upsert_pipeline(
    server: str,
    auth_header: str,
    pipeline: Any,
    payload: dict[str, Any],
    match: dict[str, Any] | None,
) -> None:
    """Create or update a pipeline on the server.

    Raises:
        DeployError: On HTTP errors from the server.
    """
    if match:
        pid = match["id"]
        payload["id"] = pid
        url = f"{server}/api/pipelines/{pid}"
        method = "PUT"
        verb = "Updated"
    else:
        url = f"{server}/api/pipelines"
        method = "POST"
        verb = "Created"

    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method=method,
        headers=_make_headers(auth_header, "application/json"),
    )
    try:
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise DeployError(pipeline.name, e.code, body)

    result = json.loads(resp.read())
    print(f"  {verb}: {pipeline.name} ({result['id'][:8]})")
    print(f"    {len(payload['nodes'])} nodes, {len(payload['edges'])} edges")
    if not match and pipeline.schedule:
        print(f"    Schedule: {pipeline.schedule}")
    if not match and pipeline.sla_deadline:
        print(f"    SLA: {pipeline.sla_deadline} {pipeline.sla_timezone}")


def deploy(args: argparse.Namespace) -> None:
    """Deploy pipeline(s) to a Brokoli server."""
    from brokoli.validation import validate_pipeline

    server = args.server.rstrip("/")
    auth_header = _auth_header_from_args(args)
    skip_validation: bool = getattr(args, "skip_validation", False)
    allow_legacy_server: bool = getattr(args, "allow_legacy_server", False)
    pipelines: list[Any] = []

    for f in _collect_files(args.file):
        print(f"Loading {f}...")
        pipelines.extend(load_pipeline_from_file(str(f)))

    preflight_server_compatibility(
        pipelines,
        server,
        auth_header,
        allow_legacy_server=allow_legacy_server,
    )

    payloads: list[tuple[Any, dict[str, Any]]] = []
    local_ids: set[str] = set()
    for pipeline in pipelines:
        pipeline_id = getattr(pipeline, "pipeline_id", "")
        if pipeline_id and pipeline_id in local_ids:
            raise DeployError(
                pipeline.name, 0, f"Duplicate local pipeline_id {pipeline_id!r}"
            )
        if pipeline_id:
            local_ids.add(pipeline_id)

        if not skip_validation:
            print(f"  Validating {pipeline.name}...")
            vr = validate_pipeline(pipeline, server_url=server, auth_header=auth_header)
            vr.print_report()
            if not vr.valid:
                raise ValidationError(
                    [str(e) for e in vr.errors],
                )

        payloads.append((pipeline, pipeline.to_json()))

    remote_pipelines = _list_remote_pipelines(server, auth_header, operation="deploy")
    matches = _match_remote_pipelines(pipelines, remote_pipelines)

    for (pipeline, payload), match in zip(payloads, matches):
        _upsert_pipeline(server, auth_header, pipeline, payload, match)


def validate_cmd(args: argparse.Namespace) -> None:
    """Validate pipeline(s) without deploying."""
    from brokoli.validation import validate_pipeline

    server = args.server.rstrip("/")
    auth_header = _auth_header_from_args(args)
    allow_legacy_server: bool = getattr(args, "allow_legacy_server", False)
    pipelines: list[Any] = []

    for f in _collect_files(args.file):
        pipelines.extend(load_pipeline_from_file(str(f)))

    preflight_server_compatibility(
        pipelines,
        server,
        auth_header,
        allow_legacy_server=allow_legacy_server,
    )

    total_errors = 0
    for pipeline in pipelines:
        print(f"Validating: {pipeline.name}")
        vr = validate_pipeline(pipeline, server_url=server, auth_header=auth_header)
        vr.print_report()
        total_errors += len(vr.errors)
        print()

    if total_errors > 0:
        raise ValidationError([f"{total_errors} validation error(s) found"])

    print("All pipelines valid")


def _output_pipeline(pipeline: Any, fmt: str) -> str:
    """Serialize a pipeline to the requested format."""
    if fmt == "json":
        return json.dumps(pipeline.to_json(), indent=2)
    return pipeline.to_yaml()


def compile_cmd(args: argparse.Namespace) -> int:
    """Compile a pipeline file to YAML (default) or JSON."""
    fmt = getattr(args, "format", "yaml")
    pipelines: list[Any] = []
    for f in _collect_files(args.file):
        pipelines.extend(load_pipeline_from_file(str(f)))

    if getattr(args, "check", False):
        from brokoli.validation import validate_pipeline

        valid = True
        for pipeline in pipelines:
            print(f"Checking: {pipeline.name}")
            result = validate_pipeline(pipeline)
            result.print_report()
            serializable = True
            try:
                canonical_json(normalize_ir(pipeline.to_json()))
            except (TypeError, ValueError, OverflowError, RecursionError) as exc:
                print(f"  ✗ [ERROR] Normalized IR is not canonical JSON: {exc}")
                serializable = False
            valid = valid and result.valid and serializable
            print()
        if valid:
            print("All pipelines valid")
            return 0
        print("Pipeline check failed")
        return 1

    if getattr(args, "normalized", False):
        snapshots = [pipeline.to_normalized_json() for pipeline in pipelines]
        output: Any = snapshots[0] if len(snapshots) == 1 else snapshots
        print(canonical_json(output), end="")
        return 0

    for pipeline in pipelines:
        print(_output_pipeline(pipeline, fmt))
    return 0


def _get_json(url: str, auth_header: str, operation: str = "diff") -> Any:
    """GET and decode a server JSON response as an operational CLI request."""
    request = urllib.request.Request(url, headers=_make_headers(auth_header))
    try:
        response = urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT)
        return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise DeployError(operation, exc.code, body) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", str(exc))
        raise DeployError(operation, 0, f"Could not reach server: {reason}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
        raise DeployError(operation, 0, f"Malformed JSON response from {url}: {exc}") from exc


def _list_remote_pipelines(
    server: str,
    auth_header: str,
    operation: str = "diff",
) -> list[dict[str, Any]]:
    """List all remote pipelines across legacy and cursor response shapes."""
    pipelines: list[dict[str, Any]] = []
    after: str | None = None
    seen_cursors: set[str] = set()

    while True:
        query = {"limit": "100"}
        if after is not None:
            query["after"] = after
        payload = _get_json(
            f"{server}/api/pipelines?{urllib.parse.urlencode(query)}",
            auth_header,
            operation,
        )

        if isinstance(payload, list):
            items = payload
            has_next = False
            cursor = None
        elif isinstance(payload, dict):
            items = payload.get("items")
            has_next = payload.get("has_next")
            cursor = payload.get("cursor")
            if not isinstance(items, list) or not isinstance(has_next, bool):
                raise DeployError(
                    operation, 0, "Malformed pipeline list response: expected items and has_next",
                )
        else:
            raise DeployError(
                operation, 0, "Malformed pipeline list response: expected a list or object",
            )

        if not all(isinstance(item, dict) for item in items):
            raise DeployError(operation, 0, "Malformed pipeline list response: invalid item")
        pipelines.extend(items)

        if not has_next:
            return pipelines
        if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
            raise DeployError(operation, 0, "Malformed pipeline list response: invalid cursor")
        seen_cursors.add(cursor)
        after = cursor


def _match_remote_pipeline(
    pipeline: Any, remote: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Match by exact logical ID, falling back to exact name."""
    pipeline_id = getattr(pipeline, "pipeline_id", "")
    id_matches = [
        item for item in remote
        if pipeline_id and item.get("pipeline_id") == pipeline_id
    ]
    name_matches = [item for item in remote if item.get("name") == pipeline.name]
    conflicting_name_matches = [
        item for item in name_matches if item.get("pipeline_id") not in (None, "")
    ]
    if not id_matches and conflicting_name_matches:
        conflicting_ids = sorted(
            str(item["pipeline_id"]) for item in conflicting_name_matches
        )
        raise DeployError(
            pipeline.name,
            0,
            "Remote name matches a different pipeline_id: "
            + ", ".join(conflicting_ids),
        )
    matches = id_matches or [
        item for item in name_matches if item.get("pipeline_id") in (None, "")
    ]
    if len(matches) > 1:
        raise DeployError(
            pipeline.name,
            0,
            "Ambiguous remote match; pipeline_id or name matched multiple pipelines",
        )
    return matches[0] if matches else None


def _match_remote_pipelines(
    pipelines: list[Any],
    remote: list[dict[str, Any]],
) -> list[dict[str, Any] | None]:
    """Match a local batch while rejecting duplicate IDs and remote targets."""
    local_ids: set[str] = set()
    remote_targets: set[str] = set()
    matches: list[dict[str, Any] | None] = []
    for pipeline in pipelines:
        pipeline_id = getattr(pipeline, "pipeline_id", "")
        if pipeline_id and pipeline_id in local_ids:
            raise DeployError(
                pipeline.name, 0, f"Duplicate local pipeline_id {pipeline_id!r}"
            )
        if pipeline_id:
            local_ids.add(pipeline_id)

        match = _match_remote_pipeline(pipeline, remote)
        if match is not None:
            remote_id = match.get("id")
            if not isinstance(remote_id, str) or not remote_id:
                raise DeployError(
                    pipeline.name, 0, "Matched pipeline has no valid server id"
                )
            if remote_id in remote_targets:
                raise DeployError(
                    pipeline.name,
                    0,
                    f"Multiple local pipelines target remote pipeline {remote_id!r}",
                )
            remote_targets.add(remote_id)
        matches.append(match)
    return matches


def _validate_pipeline_detail(detail: Any, pipeline_name: str) -> dict[str, Any]:
    """Require the minimal full pipeline IR shape needed for semantic diff."""
    if not isinstance(detail, dict):
        raise DeployError(
            pipeline_name, 0, "Malformed pipeline detail response: expected an object",
        )
    if not isinstance(detail.get("name"), str):
        raise DeployError(
            pipeline_name, 0, "Malformed pipeline detail response: name must be a string",
        )
    for field in ("nodes", "edges"):
        if field not in detail or not (
            detail[field] is None or isinstance(detail[field], list)
        ):
            raise DeployError(
                pipeline_name,
                0,
                f"Malformed pipeline detail response: {field} must be a list or null",
            )

    validated = dict(detail)
    if isinstance(detail["nodes"], list):
        nodes: list[dict[str, Any]] = []
        for index, node in enumerate(detail["nodes"]):
            if not isinstance(node, Mapping):
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: nodes[{index}] must be an object",
                )
            node_copy = dict(node)
            if not isinstance(node_copy.get("id"), str) or not node_copy["id"]:
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: nodes[{index}].id must be a nonempty string",
                )
            for field in ("type", "name"):
                if not isinstance(node_copy.get(field), str):
                    raise DeployError(
                        pipeline_name,
                        0,
                        f"Malformed pipeline detail response: nodes[{index}].{field} must be a string",
                    )
            config = node_copy.get("config")
            if config is None:
                node_copy["config"] = {}
            elif not isinstance(config, Mapping):
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: nodes[{index}].config must be an object",
                )
            else:
                node_copy["config"] = dict(config)
            if (
                "capabilities" in node_copy
                and node_copy["capabilities"] is not None
                and (
                    not isinstance(node_copy["capabilities"], list)
                    or not all(
                        isinstance(capability, str)
                        for capability in node_copy["capabilities"]
                    )
                )
            ):
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: nodes[{index}].capabilities must be a list of strings",
                )
            nodes.append(node_copy)
        validated["nodes"] = nodes

    if isinstance(detail["edges"], list):
        edges: list[dict[str, Any]] = []
        for index, edge in enumerate(detail["edges"]):
            if not isinstance(edge, Mapping):
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: edges[{index}] must be an object",
                )
            edge_copy = dict(edge)
            for field in ("from", "to"):
                if not isinstance(edge_copy.get(field), str):
                    raise DeployError(
                        pipeline_name,
                        0,
                        f"Malformed pipeline detail response: edges[{index}].{field} must be a string",
                    )
            if "condition" in edge_copy and type(edge_copy["condition"]) is not bool:
                raise DeployError(
                    pipeline_name,
                    0,
                    f"Malformed pipeline detail response: edges[{index}].condition must be a boolean",
                )
            edges.append(edge_copy)
        validated["edges"] = edges

    return validated


def diff_cmd(args: argparse.Namespace) -> int:
    """Compare local normalized pipeline IR with full server definitions."""
    server = args.server.rstrip("/")
    auth_header = _auth_header_from_args(args)
    pipelines: list[Any] = []
    for f in _collect_files(args.file):
        pipelines.extend(load_pipeline_from_file(str(f)))

    remote = _list_remote_pipelines(server, auth_header)
    matches = _match_remote_pipelines(pipelines, remote)
    different = False
    for pipeline, match in zip(pipelines, matches):
        local_ir = pipeline.to_json()
        remote_ir = None
        if match is not None:
            remote_id = match.get("id")
            if not isinstance(remote_id, str) or not remote_id:
                raise DeployError(pipeline.name, 0, "Matched pipeline has no valid server id")
            detail_url = f"{server}/api/pipelines/{urllib.parse.quote(remote_id, safe='')}"
            detail = _get_json(detail_url, auth_header)
            remote_ir = _validate_pipeline_detail(detail, pipeline.name)

        difference = diff_ir(
            local_ir,
            remote_ir,
            local_label=f"local/{pipeline.name}",
            remote_label=f"server/{pipeline.name}",
        )
        if difference:
            print(difference, end="")
            different = True
        else:
            print(f"No semantic changes: {pipeline.name}")
    return 1 if different else 0


def export(args: argparse.Namespace) -> None:
    """Export pipeline definition as YAML (default) or JSON."""
    fmt = getattr(args, "format", "yaml")
    pipelines = load_pipeline_from_file(args.file)
    for pipeline in pipelines:
        output = _output_pipeline(pipeline, fmt)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Exported {pipeline.name} to {args.output}")
        else:
            print(output)


def main() -> None:
    """CLI entry point. This is the only place that catches exceptions and exits."""
    parser = argparse.ArgumentParser(prog="brokoli", description="Brokoli Python SDK CLI")
    sub = parser.add_subparsers(dest="command")

    # deploy
    dp = sub.add_parser("deploy", help="Deploy pipeline(s) to a Brokoli server")
    dp.add_argument("file", help="Python file or directory containing pipelines")
    dp.add_argument("--server", default="http://localhost:8080", help="Brokoli server URL")
    dp.add_argument("--api-key", default="", help="API key for authentication")
    dp.add_argument("--skip-validation", action="store_true", help="Skip pre-deploy validation")
    dp.add_argument(
        "--allow-legacy-server",
        action="store_true",
        help="Allow a trusted server that predates capability negotiation",
    )
    dp.set_defaults(func=deploy)

    # validate (without deploying)
    vp = sub.add_parser("validate", help="Validate pipeline(s) without deploying")
    vp.add_argument("file", help="Python file or directory")
    vp.add_argument("--server", default="http://localhost:8080", help="Brokoli server URL (for conn_id checks)")
    vp.add_argument("--api-key", default="", help="API key")
    vp.add_argument(
        "--allow-legacy-server",
        action="store_true",
        help="Allow a trusted server that predates capability negotiation",
    )
    vp.set_defaults(func=validate_cmd)

    # compile
    cp = sub.add_parser("compile", help="Compile pipeline to YAML (default) or JSON")
    cp.add_argument("file", help="Python file containing pipeline(s)")
    cp.add_argument("-f", "--format", choices=["yaml", "json"], default="yaml", help="Output format (default: yaml)")
    compile_mode = cp.add_mutually_exclusive_group()
    compile_mode.add_argument(
        "--normalized",
        action="store_true",
        help="Output canonical normalized JSON (overrides --format)",
    )
    compile_mode.add_argument(
        "--check",
        action="store_true",
        help="Validate and normalize locally without emitting IR",
    )
    cp.set_defaults(func=compile_cmd)

    # diff
    df = sub.add_parser("diff", help="Compare local IR with server pipeline definitions")
    df.add_argument("file", help="Python file or directory containing pipelines")
    df.add_argument("--server", required=True, help="Brokoli server URL")
    df.add_argument("--api-key", default="", help="API key for authentication")
    df.set_defaults(func=diff_cmd)

    # export
    ep = sub.add_parser("export", help="Export pipeline as YAML (default) or JSON")
    ep.add_argument("file", help="Python file containing pipeline")
    ep.add_argument("-o", "--output", help="Output file path")
    ep.add_argument("-f", "--format", choices=["yaml", "json"], default="yaml", help="Output format (default: yaml)")
    ep.set_defaults(func=export)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        status = args.func(args)
        if isinstance(status, int) and status != 0:
            sys.exit(status)
    except ValidationError as exc:
        print(f"\nValidation failed: {exc}")
        sys.exit(1)
    except CompatibilityError as exc:
        print(f"\nCompatibility error: {exc}")
        sys.exit(1)
    except DeployError as exc:
        print(f"\nDeploy error: {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
