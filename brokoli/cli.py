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
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

import brokoli
from brokoli.client import DEFAULT_SERVER
from brokoli import device
from brokoli.compatibility import preflight_server_compatibility
from brokoli.exceptions import CompatibilityError, DeployError, ValidationError
from brokoli.ir import canonical_json, diff_ir, ir_digest, normalize_ir

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


# Named deployment environments (brokoli-sdk#15 M3). A project config file
# maps environment names to a server URL and, optionally, the env var
# holding that environment's token:
#
#     environments:
#       dev:  { server: http://localhost:8080 }
#       prod: { server: https://prod.example, token_env: BROKOLI_PROD_TOKEN }
#
# so `brokoli deploy pipe.py --env prod` targets prod without repeating the
# URL or embedding a secret in the file. The file is `brokoli.yaml` in the
# working directory, or the path in BROKOLI_CONFIG.
def _config_path() -> "str | None":
    override = os.getenv("BROKOLI_CONFIG")
    if override:
        return override
    return "brokoli.yaml" if os.path.exists("brokoli.yaml") else None


def _load_environments() -> dict[str, dict]:
    """Read the ``environments`` mapping from the project config, or {}."""
    path = _config_path()
    if not path:
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise DeployError("config", 0, f"could not read {path}: {exc}") from exc
    envs = data.get("environments") or {}
    if not isinstance(envs, dict):
        raise DeployError("config", 0, f"{path}: 'environments' must be a mapping")
    return envs


def _resolve_target(
    args: argparse.Namespace, operation: str, default_server: "str | None" = DEFAULT_SERVER
) -> "tuple[str, str]":
    """Resolve (server, auth_header) from --server/--api-key and/or --env.

    Precedence: an explicit ``--server`` always wins; otherwise ``--env``'s
    configured server is used; otherwise ``default_server`` (the hosted
    platform, unless overridden). Auth likewise prefers an explicit token
    (BROKOLI_TOKEN / --api-key) and falls back to the environment's
    ``token_env``. The workspace is ``--workspace``, else
    ``BROKOLI_WORKSPACE``, else the environment's ``workspace``; it is
    sent as ``X-Workspace-ID`` on every request the command makes.
    """
    global _cli_workspace
    server = getattr(args, "server", None)
    env_cfg: dict = {}
    env_name = getattr(args, "env", None)
    if env_name:
        envs = _load_environments()
        if env_name not in envs:
            known = ", ".join(sorted(envs)) or "none configured"
            raise DeployError(operation, 0, f"unknown environment {env_name!r} (have: {known})")
        env_cfg = envs[env_name] or {}
        if server is None:
            env_server = env_cfg.get("server")
            server = env_server if isinstance(env_server, str) else None
    if server is None:
        server = default_server
    if not server:
        raise DeployError(operation, 0, "no server: pass --server or --env <name>")
    auth = _auth_header_from_args(args)
    if not auth and env_cfg.get("token_env"):
        token = os.getenv(str(env_cfg["token_env"]), "")
        if token:
            auth = f"Bearer {token}"
    if not auth:
        # Last in line: a token stored by ``brokoli auth`` (#75). Explicit
        # BROKOLI_TOKEN, --api-key, or a project token_env always wins.
        stored = device.load_token(server)
        if stored:
            auth = f"Bearer {stored}"
    workspace = str(getattr(args, "workspace", None) or os.getenv("BROKOLI_WORKSPACE") or "")
    env_workspace = env_cfg.get("workspace")
    if not workspace and isinstance(env_workspace, str):
        workspace = env_workspace
    _cli_workspace = workspace
    return server.rstrip("/"), auth


# The workspace the current command targets, set by _resolve_target. One
# command runs per process, and every request a command makes goes to the
# same workspace, so it travels here rather than through every helper that
# already takes the server and auth header.
_cli_workspace = ""


def _make_headers(auth_header: str, content_type: str | None = None) -> dict[str, str]:
    """Build a headers dict, omitting empty values."""
    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header
    if _cli_workspace:
        headers["X-Workspace-ID"] = _cli_workspace
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


# Set in the environment while a pipeline file is imported for discovery
# (compile/validate/deploy), and cleared afterward. Module-level code that
# only makes sense at run time -- opening a DB connection, building an API
# client, reading a large local file -- can guard on it to keep discovery
# free of those side effects:
#
#     import os
#     if not os.getenv("BROKOLI_DISCOVERY"):
#         client = connect_to_warehouse()   # skipped during compile/deploy
#
# It's an environment variable, not an import, so a file needs nothing from
# brokoli to check it. brokoli-sdk#15 M2.
DISCOVERY_ENV_VAR = "BROKOLI_DISCOVERY"


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
    # Mark this import as discovery so the file can skip run-time-only side
    # effects (see DISCOVERY_ENV_VAR). Restored afterward so discovery
    # leaves the process environment exactly as it found it.
    prior_discovery = os.environ.get(DISCOVERY_ENV_VAR)
    os.environ[DISCOVERY_ENV_VAR] = "1"
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[module_name]
        raise
    finally:
        if prior_discovery is None:
            os.environ.pop(DISCOVERY_ENV_VAR, None)
        else:
            os.environ[DISCOVERY_ENV_VAR] = prior_discovery

    pipelines = [obj for obj in vars(module).values() if isinstance(obj, Pipeline)]
    if not pipelines:
        raise DeployError(
            filepath,
            0,
            f"No Pipeline found in {filepath}. Use: with Pipeline('name') as p: ...",
        )

    return pipelines


def _upload_task_bundles(
    server: str,
    auth_header: str,
    pipelines: Sequence[Any],
) -> None:
    """Upload every attached task bundle content-addressed (ADR-031),
    once per digest across all pipelines, before the pipelines themselves
    are written. 201/200 are both success; the server refuses mismatched
    bytes (409), oversize archives (413), and malformed digests (400).
    """
    seen: set[str] = set()
    for pipeline in pipelines:
        for bundle in getattr(pipeline, "task_bundles", ()) or ():
            if bundle.digest in seen:
                continue
            seen.add(bundle.digest)
            url = f"{server}/api/task-bundles/{bundle.digest}"
            req = urllib.request.Request(
                url,
                data=bundle.archive,
                method="POST",
                headers=_make_headers(auth_header, "application/gzip"),
            )
            try:
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status not in (200, 201):
                        raise DeployError(
                            pipeline.name, resp.status, f"upload failed (HTTP {resp.status})"
                        )
            except urllib.error.HTTPError as e:
                raise DeployError(pipeline.name, e.code, e.read().decode()) from e
            print(f"  Uploaded task bundle {bundle.digest[:16]}… ({len(bundle.archive)} bytes)")


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
        url,
        data=data,
        method=method,
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
    # Auditable record of exactly what was deployed: a content digest of the
    # normalized IR, stable across create/update and cosmetic churn.
    print(f"    IR digest: {ir_digest(payload)}")
    if not match and pipeline.schedule:
        print(f"    Schedule: {pipeline.schedule}")
    if not match and pipeline.sla_deadline:
        print(f"    SLA: {pipeline.sla_deadline} {pipeline.sla_timezone}")


def deploy(args: argparse.Namespace) -> None:
    """Deploy pipeline(s) to a Brokoli server."""
    from brokoli.validation import validate_pipeline

    server, auth_header = _resolve_target(args, "deploy")
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
            raise DeployError(pipeline.name, 0, f"Duplicate local pipeline_id {pipeline_id!r}")
        if pipeline_id:
            local_ids.add(pipeline_id)

        if not skip_validation:
            print(f"  Validating {pipeline.name}...")
            vr = validate_pipeline(
                pipeline, server_url=server, auth_header=auth_header, workspace=_cli_workspace
            )
            vr.print_report()
            if not vr.valid:
                raise ValidationError(
                    [str(e) for e in vr.errors],
                )

        payloads.append((pipeline, pipeline.to_json()))

    remote_pipelines = _list_remote_pipelines(server, auth_header, operation="deploy")
    matches = _match_remote_pipelines(pipelines, remote_pipelines)

    # Task bundles ride sideband on the pipelines (the IR only carries
    # digest references): upload them first so the references resolve.
    _upload_task_bundles(server, auth_header, pipelines)

    for (pipeline, payload), match in zip(payloads, matches):
        _upsert_pipeline(server, auth_header, pipeline, payload, match)


def validate_cmd(args: argparse.Namespace) -> None:
    """Validate pipeline(s) without deploying."""
    from brokoli.validation import validate_pipeline

    server, auth_header = _resolve_target(args, "validate")
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
        vr = validate_pipeline(
            pipeline, server_url=server, auth_header=auth_header, workspace=_cli_workspace
        )
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

    if getattr(args, "digest", False):
        # One "<digest>  <name>" line per pipeline — a stable content hash
        # a CI job can capture and compare across builds to tell whether a
        # pipeline's compiled IR actually changed.
        for pipeline in pipelines:
            print(f"{ir_digest(pipeline.to_json())}  {pipeline.name}")
        return 0

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


def _post_json(
    url: str,
    auth_header: str,
    body: dict[str, Any] | None,
    operation: str,
) -> Any:
    """POST a JSON body and decode the response as an operational request.

    Mirrors :func:`_get_json`'s error handling. An empty response body
    decodes to ``{}`` (some endpoints return 202/204 with no content).
    """
    data = json.dumps(body or {}).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers=_make_headers(auth_header, "application/json"),
    )
    try:
        response = urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT)
        raw = response.read()
        return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise DeployError(operation, exc.code, detail) from exc
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

        items: list[Any] | None
        has_next: bool | None
        cursor: str | None
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
                    operation,
                    0,
                    "Malformed pipeline list response: expected items and has_next",
                )
        else:
            raise DeployError(
                operation,
                0,
                "Malformed pipeline list response: expected a list or object",
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
    pipeline: Any,
    remote: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Match by exact logical ID, falling back to exact name."""
    pipeline_id = getattr(pipeline, "pipeline_id", "")
    id_matches = [item for item in remote if pipeline_id and item.get("pipeline_id") == pipeline_id]
    name_matches = [item for item in remote if item.get("name") == pipeline.name]
    conflicting_name_matches = [
        item for item in name_matches if item.get("pipeline_id") not in (None, "")
    ]
    if not id_matches and conflicting_name_matches:
        conflicting_ids = sorted(str(item["pipeline_id"]) for item in conflicting_name_matches)
        raise DeployError(
            pipeline.name,
            0,
            "Remote name matches a different pipeline_id: " + ", ".join(conflicting_ids),
        )
    matches = id_matches or [item for item in name_matches if item.get("pipeline_id") in (None, "")]
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
            raise DeployError(pipeline.name, 0, f"Duplicate local pipeline_id {pipeline_id!r}")
        if pipeline_id:
            local_ids.add(pipeline_id)

        match = _match_remote_pipeline(pipeline, remote)
        if match is not None:
            remote_id = match.get("id")
            if not isinstance(remote_id, str) or not remote_id:
                raise DeployError(pipeline.name, 0, "Matched pipeline has no valid server id")
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
            pipeline_name,
            0,
            "Malformed pipeline detail response: expected an object",
        )
    if not isinstance(detail.get("name"), str):
        raise DeployError(
            pipeline_name,
            0,
            "Malformed pipeline detail response: name must be a string",
        )
    for field in ("nodes", "edges"):
        if field not in detail or not (detail[field] is None or isinstance(detail[field], list)):
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
                        isinstance(capability, str) for capability in node_copy["capabilities"]
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


def _compile_dispatch(args: argparse.Namespace) -> int:
    if getattr(args, "project", False):
        if args.file is not None:
            print("--project compiles the whole project; drop the file argument")
            return 2
        return compile_project_cmd(args)
    if args.file is None:
        print("compile needs a file, or --project for the whole project")
        return 2
    return compile_cmd(args)


def diff_cmd(args: argparse.Namespace) -> int:
    """Compare local normalized pipeline IR with full server definitions."""
    server, auth_header = _resolve_target(args, "diff")
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


def _parse_params(pairs: list[str] | None, operation: str) -> dict[str, str]:
    """Turn ``--param KEY=VALUE`` repeats into a params dict."""
    params: dict[str, str] = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise DeployError(operation, 0, f"--param must be KEY=VALUE, got {pair!r}")
        params[key] = value
    return params


def _resolve_pipeline_id(
    server: str,
    auth_header: str,
    identifier: str,
    operation: str,
) -> str:
    """Resolve a user-supplied pipeline identifier to the server's internal id.

    The run/backfill endpoints key on the server's internal id, but users
    know the ``pipeline_id`` they set in code (or the pipeline name). Accept
    any of the three -- internal id, logical ``pipeline_id``, or name -- and
    return the internal id.
    """
    remote = _list_remote_pipelines(server, auth_header, operation)
    for item in remote:  # an exact internal-id match is unambiguous
        if item.get("id") == identifier:
            return identifier
    matches = [i for i in remote if i.get("pipeline_id") == identifier] or [
        i for i in remote if i.get("name") == identifier
    ]
    if not matches:
        raise DeployError(
            operation,
            0,
            f"No pipeline matching {identifier!r} on {server}",
        )
    if len({i.get("id") for i in matches}) > 1:
        raise DeployError(
            operation,
            0,
            f"{identifier!r} matches multiple pipelines; use the pipeline id",
        )
    return matches[0]["id"]


def run_cmd(args: argparse.Namespace) -> int:
    """Trigger a run of a deployed pipeline.

    The server runs pipelines asynchronously: this returns as soon as the
    run is accepted, printing the run id to observe with ``brokoli status``.
    Accepts the pipeline's logical id, name, or internal id.
    """
    server, auth_header = _resolve_target(args, "run")
    params = _parse_params(getattr(args, "param", None), "run")
    pipeline_id = _resolve_pipeline_id(server, auth_header, args.pipeline, "run")
    result = _post_json(
        f"{server}/api/pipelines/{pipeline_id}/run",
        auth_header,
        {"params": params} if params else {},
        operation="run",
    )
    run_id = result.get("id", "")
    status = result.get("status", "accepted")
    print(f"Triggered run {run_id} ({status}) for pipeline {args.pipeline}")
    if run_id:
        print(f"  Observe: brokoli status {run_id} --server {server}")
    return 0


def _fmt_run(run: dict[str, Any]) -> str:
    """Render a run object as a compact, human-readable status block."""
    lines = [
        f"Run     {run.get('id', '?')}",
        f"Pipeline {run.get('pipeline_id', '?')}",
        f"Status  {run.get('status', '?')}",
    ]
    if run.get("started_at"):
        lines.append(f"Started  {run['started_at']}")
    if run.get("finished_at"):
        lines.append(f"Finished {run['finished_at']}")
    node_runs = run.get("node_runs") or []
    if node_runs:
        by_status: dict[str, int] = {}
        for nr in node_runs:
            by_status[nr.get("status", "?")] = by_status.get(nr.get("status", "?"), 0) + 1
        summary = ", ".join(f"{n} {s}" for s, n in sorted(by_status.items()))
        lines.append(f"Nodes   {len(node_runs)} ({summary})")
    if run.get("error"):
        lines.append(f"Error   {run['error']}")
    return "\n".join(lines)


def status_cmd(args: argparse.Namespace) -> int:
    """Show the status of a run by id."""
    server, auth_header = _resolve_target(args, "status")
    run = _get_json(
        f"{server}/api/runs/{args.run}",
        auth_header,
        operation="status",
    )
    if not isinstance(run, dict):
        raise DeployError("status", 0, "Malformed run response: expected an object")
    print(_fmt_run(run))
    return 0


def logs_cmd(args: argparse.Namespace) -> int:
    """Print a run's logs, optionally filtered by level or node."""
    server, auth_header = _resolve_target(args, "logs")
    query: dict[str, str] = {}
    if getattr(args, "level", None):
        query["level"] = args.level
    if getattr(args, "node", None):
        query["node_id"] = args.node
    url = f"{server}/api/runs/{args.run}/logs"
    if query:
        url += "?" + urllib.parse.urlencode(query)
    entries = _get_json(url, auth_header, operation="logs")
    if not isinstance(entries, list):
        raise DeployError("logs", 0, "Malformed logs response: expected a list")
    if not entries:
        print("(no logs)")
        return 0
    for entry in entries:
        ts = entry.get("timestamp", "")
        level = str(entry.get("level", "")).upper()
        node = entry.get("node_id", "")
        node_part = f" {node}" if node else ""
        print(f"{ts} {level:<7}{node_part}: {entry.get('message', '')}")
    return 0


def cancel_cmd(args: argparse.Namespace) -> int:
    """Cancel an in-progress run."""
    server, auth_header = _resolve_target(args, "cancel")
    result = _post_json(
        f"{server}/api/runs/{args.run}/cancel",
        auth_header,
        {},
        operation="cancel",
    )
    print(f"Run {args.run}: {result.get('status', 'cancelled')}")
    return 0


def retry_cmd(args: argparse.Namespace) -> int:
    """Retry a run by resuming it from where it stopped.

    Maps to the server's resume operation: successful nodes are preserved
    and execution continues from the failure, rather than starting over.
    """
    server, auth_header = _resolve_target(args, "retry")
    run = _post_json(
        f"{server}/api/runs/{args.run}/resume",
        auth_header,
        {},
        operation="retry",
    )
    if isinstance(run, dict) and run.get("id"):
        print(f"Resumed run {args.run}:")
        print(_fmt_run(run))
    else:
        print(f"Resumed run {args.run}")
    return 0


def _is_bare_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))


def backfill_cmd(args: argparse.Namespace) -> int:
    """Backfill a pipeline over a date range (a server-side operation).

    Interval-native servers (core v0.10.78+) take RFC3339 ``start``/``end``
    and answer with a plan; bare YYYY-MM-DD keeps its historical
    inclusive-day meaning through the legacy fields, which older servers
    also understand. Passing an RFC3339 timestamp into the date-only
    field was this verb's original bug -- the server refused it with
    "extra text", found live while walking the tutorial.
    """
    server, auth_header = _resolve_target(args, "backfill")
    pipeline_id = _resolve_pipeline_id(server, auth_header, args.pipeline, "backfill")
    body: dict = {}
    body["start_date" if _is_bare_date(args.start) else "start"] = args.start
    body["end_date" if _is_bare_date(args.end) else "end"] = args.end
    result = _post_json(
        f"{server}/api/pipelines/{pipeline_id}/backfill",
        auth_header,
        body,
        operation="backfill",
    )
    if "intervals" in result:
        # The interval-native plan response.
        print(
            f"Backfill {args.pipeline}: {result['intervals']} interval(s), "
            f"{result.get('first_interval_start')} .. {result.get('last_interval_end')}, "
            f"concurrency {result.get('concurrency')}"
        )
        if result.get("note"):
            print(f"  {result['note']}")
        return 0
    # Legacy servers answered with the run-id list.
    runs = result.get("runs") or []
    count = result.get("count", len(runs))
    if result.get("error"):
        print(f"Backfill incomplete: {result['error']}")
    print(f"Backfill {args.pipeline} {args.start}..{args.end}: {count} run(s) triggered")
    for run_id in runs:
        print(f"  {run_id}")
    return 0


def auth_cmd(args: argparse.Namespace) -> int:
    """Authorize this terminal via the browser (device grant, #75)."""
    server, _ = _resolve_target(args, "auth")
    try:
        device.device_login(server, open_browser=not args.no_browser)
    except device.DeviceAuthError as exc:
        print(f"auth failed: {exc}")
        return 1
    return 0


# ---------------------------------------------------------------- projects
#
# A project is a repository of pipelines described by a brokoli.yaml with
# `version: 1` (brokoli.project). These commands work on the whole tree.


def _print_problems(title: str, problems: Sequence[str]) -> None:
    print(title)
    for problem in problems:
        print(f"  - {problem}")


def propose_cmd(args: argparse.Namespace) -> int:
    """Write changes to a pipeline's definition into its pipeline.py."""
    from brokoli.projects import find_project_root
    from brokoli.propose import Change, ProposeError, propose

    root = Path(args.project) if args.project else find_project_root(".")
    if root is None:
        _print_problems("Nothing was proposed:", ["no project here (brokoli.yaml); pass --project"])
        return 1
    try:
        raw = sys.stdin.read() if args.changes == "-" else Path(args.changes).read_text()
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("the changes are a JSON list")
        changes = [Change.from_dict(item) for item in data]
    except (OSError, ValueError) as exc:
        _print_problems("Nothing was proposed:", [f"reading the changes: {exc}"])
        return 1
    try:
        proposal = propose(root, args.folder, changes)
    except ProposeError as exc:
        _print_problems("Nothing was proposed:", exc.problems)
        return 1
    if args.write and proposal.changed:
        (Path(root) / args.folder / "pipeline.py").write_text(proposal.source)
    path = f"{args.folder}/pipeline.py"
    if args.json:
        print(json.dumps(proposal.to_dict(path), indent=2))
        return 0
    if proposal.changed:
        print(proposal.diff(path), end="")
    else:
        print("No change written.")
    for change, reason in proposal.skipped:
        where = f"node {change.node_id} " if change.node_id else ""
        print(f"skipped: {where}{change.field}: {reason}")
    return 0


def init_cmd(args: argparse.Namespace) -> int:
    """Create a project."""
    from brokoli.projects import ProjectError
    from brokoli.scaffold import init_project

    try:
        written = init_project(
            args.directory,
            workspace=args.workspace,
            example=args.example,
            template=args.template,
            workflow=not args.no_workflow,
        )
    except ProjectError as exc:
        _print_problems("Nothing was created:", exc.problems)
        return 1
    root = Path(args.directory)
    print(f"Created a Brokoli project in {root.resolve()}:")
    for path in written:
        print(f"  {path.relative_to(root)}")
    print("\nNext: brokoli validate")
    return 0


def new_pipeline_cmd(args: argparse.Namespace) -> int:
    """Add a pipeline folder to the project."""
    from brokoli.projects import ProjectError
    from brokoli.scaffold import new_pipeline

    try:
        path = new_pipeline(args.pipeline_id, workspace=args.workspace, template=args.template)
    except ProjectError as exc:
        _print_problems("Nothing was created:", exc.problems)
        return 1
    print(f"Created {path}")
    return 0


def _load_project_or_report() -> Any:
    from brokoli.projects import ProjectError, load_project

    try:
        return load_project(".")
    except ProjectError as exc:
        _print_problems("The project has problems:", exc.problems)
        return None


def validate_project_cmd(args: argparse.Namespace) -> int:
    """Validate every pipeline in the project. Offline unless --server or
    --env is given, in which case each workspace with an `id` is also
    checked against that server (connections, capabilities)."""
    from brokoli.validation import validate_pipeline

    project = _load_project_or_report()
    if project is None:
        return 1
    online = bool(getattr(args, "server", None) or getattr(args, "env", None))
    server = auth_header = ""
    if online:
        server, auth_header = _resolve_target(args, "validate")

    global _cli_workspace
    total_errors = 0
    for name, spec in sorted(project.config.workspaces.items()):
        items = project.in_workspace(name)
        if not items:
            continue
        print(f"Workspace {name} ({spec.path}): {len(items)} pipeline(s)")
        ws_online = online and bool(spec.id)
        if online and not spec.id:
            print(f"  (no 'id' for {name} in brokoli.yaml: checked offline only)")
        if ws_online:
            _cli_workspace = spec.id
            preflight_server_compatibility(
                [i.pipeline for i in items],
                server,
                auth_header,
                allow_legacy_server=getattr(args, "allow_legacy_server", False),
            )
        for item in items:
            print(f"  {item.source.pipeline_id}")
            if ws_online:
                vr = validate_pipeline(
                    item.pipeline, server_url=server, auth_header=auth_header, workspace=spec.id
                )
            else:
                vr = validate_pipeline(item.pipeline)
            vr.print_report()
            total_errors += len(vr.errors)
    if not project.pipelines:
        print("No pipelines yet (add one with: brokoli new pipeline <id>)")
    if total_errors:
        raise ValidationError([f"{total_errors} validation error(s) found"])
    print(f"All {len(project.pipelines)} pipeline(s) valid")
    return 0


def compile_project_cmd(args: argparse.Namespace) -> int:
    """Compile the whole project: IR, task bundles, and a manifest."""
    from brokoli.projects import ProjectError, compile_project

    if getattr(args, "normalized", False) or getattr(args, "check", False):
        print("--project writes a build; use `brokoli validate` to check a project")
        return 2
    project = _load_project_or_report()
    if project is None:
        return 1
    if getattr(args, "digest", False):
        for item in sorted(
            project.pipelines, key=lambda i: (i.source.workspace, i.source.pipeline_id)
        ):
            print(f"{ir_digest(item.pipeline.to_json())}  {item.source.folder}")
        return 0
    try:
        manifest = compile_project(project, args.out)
    except ProjectError as exc:
        _print_problems("Nothing was compiled:", exc.problems)
        return 1
    for entry in manifest["pipelines"]:
        print(f"{entry['digest']}  {entry['folder']}")
    print(f"Wrote {len(manifest['pipelines'])} pipeline(s) to {args.out}")
    return 0


def plan_cmd(args: argparse.Namespace) -> int:
    """Compare the project with a server, workspace by workspace: which
    pipelines a deploy would add or change, which are unchanged, and which
    the server has that the folder does not."""
    global _cli_workspace
    project = _load_project_or_report()
    if project is None:
        return 1
    server, auth_header = _resolve_target(args, "plan")
    explicit_ws = _cli_workspace  # --workspace / BROKOLI_WORKSPACE / the env's workspace

    names = sorted(project.config.workspaces)
    if args.only:
        if args.only not in project.config.workspaces:
            raise DeployError("plan", 0, f"no workspace {args.only!r} (have: {', '.join(names)})")
        names = [args.only]
    if getattr(args, "workspace", None) and len(names) > 1:
        raise DeployError(
            "plan", 0, "--workspace names one server workspace; use it with --only <workspace>"
        )

    report: list[dict[str, Any]] = []
    changes = 0
    for name in names:
        spec = project.config.workspaces[name]
        ws_id = spec.id or (explicit_ws if len(names) == 1 else "")
        if getattr(args, "workspace", None):
            ws_id = args.workspace
        if not ws_id:
            raise DeployError(
                "plan",
                0,
                f"workspace {name!r} has no server workspace: add `id:` to it in brokoli.yaml "
                f"or pass --only {name} --workspace <id>",
            )
        _cli_workspace = ws_id
        items = project.in_workspace(name)
        remote = _list_remote_pipelines(server, auth_header, "plan")
        matches = _match_remote_pipelines([i.pipeline for i in items], remote)
        entry: dict[str, Any] = {
            "workspace": name,
            "server_workspace": ws_id,
            "added": [],
            "changed": [],
            "unchanged": [],
            "server_only": [],
        }
        matched: set[str] = set()
        for item, match in zip(items, matches):
            pid = item.source.pipeline_id
            if match is None:
                entry["added"].append({"pipeline_id": pid, "diff": ""})
                continue
            remote_id = str(match.get("id"))
            matched.add(remote_id)
            detail = _get_json(
                f"{server}/api/pipelines/{urllib.parse.quote(remote_id, safe='')}",
                auth_header,
                "plan",
            )
            remote_ir = _validate_pipeline_detail(detail, item.pipeline.name)
            difference = diff_ir(
                item.pipeline.to_json(),
                remote_ir,
                local_label=f"{item.source.folder}/pipeline.py",
                remote_label=f"server/{pid}",
            )
            key = "changed" if difference else "unchanged"
            entry[key].append({"pipeline_id": pid, "diff": difference})
        entry["server_only"] = sorted(
            str(r.get("pipeline_id") or r.get("name") or r.get("id"))
            for r in remote
            if str(r.get("id")) not in matched
        )
        changes += len(entry["added"]) + len(entry["changed"])
        report.append(entry)

    if args.json:
        print(json.dumps({"server": server, "workspaces": report}, indent=2, sort_keys=True))
    else:
        for entry in report:
            print(f"Workspace {entry['workspace']} -> {entry['server_workspace']}")
            for label, mark in (("added", "+"), ("changed", "~"), ("unchanged", "=")):
                for item in entry[label]:
                    print(f"  {mark} {item['pipeline_id']} ({label})")
                    if args.diff and item["diff"]:
                        print("".join("      " + line for line in item["diff"].splitlines(True)))
            if entry["server_only"]:
                print("  on the server, not in this folder: " + ", ".join(entry["server_only"]))
        print(f"\n{changes} pipeline(s) to add or change")
    if args.detailed_exitcode and changes:
        return 2
    return 0


def main() -> None:
    """CLI entry point. This is the only place that catches exceptions and exits."""
    parser = argparse.ArgumentParser(prog="brokoli", description="Brokoli Python SDK CLI")
    parser.add_argument(
        "--version", "-V", action="version", version=f"brokoli {brokoli.__version__}"
    )
    sub = parser.add_subparsers(dest="command")

    # auth
    ap = sub.add_parser(
        "auth", help="Authorize this terminal via the browser (no password in the shell)"
    )
    ap.add_argument("--server", default=None, help="Brokoli server URL")
    ap.add_argument("--env", default=None, help="Named environment from the project config")
    ap.add_argument(
        "--no-browser", action="store_true", help="Print the link instead of opening a browser"
    )
    ap.set_defaults(func=auth_cmd)

    # deploy
    dp = sub.add_parser("deploy", help="Deploy pipeline(s) to a Brokoli server")
    dp.add_argument("file", help="Python file or directory containing pipelines")
    dp.add_argument("--server", default=None, help="Brokoli server URL")
    dp.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    dp.add_argument("--api-key", default="", help="API key for authentication")
    dp.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    dp.add_argument("--skip-validation", action="store_true", help="Skip pre-deploy validation")
    dp.add_argument(
        "--allow-legacy-server",
        action="store_true",
        help="Allow a trusted server that predates capability negotiation",
    )
    dp.set_defaults(func=deploy)

    # validate (without deploying)
    vp = sub.add_parser(
        "validate",
        help="Validate pipeline(s) without deploying; without a file, the whole project",
    )
    vp.add_argument(
        "file", nargs="?", default=None, help="Python file or directory (default: the project)"
    )
    vp.add_argument("--server", default=None, help="Brokoli server URL (for conn_id checks)")
    vp.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    vp.add_argument("--api-key", default="", help="API key")
    vp.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    vp.add_argument(
        "--allow-legacy-server",
        action="store_true",
        help="Allow a trusted server that predates capability negotiation",
    )
    vp.set_defaults(
        func=lambda a: validate_cmd(a) if a.file is not None else validate_project_cmd(a)
    )

    # compile
    cp = sub.add_parser("compile", help="Compile pipeline to YAML (default) or JSON")
    cp.add_argument("file", nargs="?", default=None, help="Python file containing pipeline(s)")
    cp.add_argument(
        "--project",
        action="store_true",
        help="Compile the whole project: IR, task bundles and a manifest, under --out",
    )
    cp.add_argument("--out", default="build", help="Output folder for --project (default: build)")
    cp.add_argument(
        "-f",
        "--format",
        choices=["yaml", "json"],
        default="yaml",
        help="Output format (default: yaml)",
    )
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
    compile_mode.add_argument(
        "--digest",
        action="store_true",
        help="Print a stable sha256 IR digest per pipeline (for audit/CI)",
    )
    cp.set_defaults(func=_compile_dispatch)

    # init
    ip = sub.add_parser("init", help="Create a Brokoli project (brokoli.yaml and folders)")
    ip.add_argument("directory", nargs="?", default=".", help="Where (default: here)")
    ip.add_argument("--workspace", default="default", help="First workspace (default: default)")
    ip.add_argument("--example", default="example", help="Example pipeline id (default: example)")
    ip.add_argument("--template", choices=["basic", "python"], default="basic")
    ip.add_argument("--no-workflow", action="store_true", help="Skip the GitHub Actions workflow")
    ip.set_defaults(func=init_cmd)

    # new pipeline
    np_ = sub.add_parser("new", help="Add to the project")
    np_sub = np_.add_subparsers(dest="what", metavar="pipeline")
    np_sub.required = True
    npp = np_sub.add_parser("pipeline", help="Add a pipeline folder from a template")
    npp.add_argument("pipeline_id", help="The pipeline id, which is also its folder name")
    npp.add_argument("--workspace", default=None, help="Workspace (needed when there are several)")
    npp.add_argument("--template", choices=["basic", "python"], default="basic")
    npp.set_defaults(func=new_pipeline_cmd)

    # propose
    rp_ = sub.add_parser(
        "propose", help="Write changes to a pipeline's definition into its pipeline.py"
    )
    rp_.add_argument("folder", help="The pipeline's folder, relative to the project")
    rp_.add_argument(
        "--changes", required=True, help="JSON list of changes (a file, or - for stdin)"
    )
    rp_.add_argument("--project", default=None, help="Project root (default: found from here)")
    rp_.add_argument("--write", action="store_true", help="Write the result into pipeline.py")
    rp_.add_argument("--json", action="store_true", help="Machine-readable output")
    rp_.set_defaults(func=propose_cmd)

    # plan
    pp = sub.add_parser("plan", help="Compare the project with a server, workspace by workspace")
    pp.add_argument("--server", default=None, help="Brokoli server URL")
    pp.add_argument("--env", default=None, help="Named environment from brokoli.yaml")
    pp.add_argument("--api-key", default="", help="API key for authentication")
    pp.add_argument("--only", default=None, help="Plan one project workspace")
    pp.add_argument(
        "--workspace", default=None, help="Server workspace id for --only (overrides its id)"
    )
    pp.add_argument("--diff", action="store_true", help="Show each changed pipeline's IR diff")
    pp.add_argument("--json", action="store_true", help="Machine-readable output")
    pp.add_argument(
        "--detailed-exitcode",
        action="store_true",
        help="Exit 2 when something would be added or changed",
    )
    pp.set_defaults(func=plan_cmd)

    # diff
    df = sub.add_parser("diff", help="Compare local IR with server pipeline definitions")
    df.add_argument("file", help="Python file or directory containing pipelines")
    df.add_argument("--server", default=None, help="Brokoli server URL")
    df.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    df.add_argument("--api-key", default="", help="API key for authentication")
    df.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    df.set_defaults(func=diff_cmd)

    # run (trigger a deployed pipeline)
    rp = sub.add_parser("run", help="Trigger a run of a deployed pipeline")
    rp.add_argument("pipeline", help="Pipeline id to run")
    rp.add_argument("--server", default=None, help="Brokoli server URL")
    rp.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    rp.add_argument("--api-key", default="", help="API key for authentication")
    rp.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    rp.add_argument(
        "--param",
        action="append",
        metavar="KEY=VALUE",
        help="Runtime parameter override (repeatable)",
    )
    rp.set_defaults(func=run_cmd)

    # status (of a run)
    sp = sub.add_parser("status", help="Show the status of a run")
    sp.add_argument("run", help="Run id")
    sp.add_argument("--server", default=None, help="Brokoli server URL")
    sp.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    sp.add_argument("--api-key", default="", help="API key for authentication")
    sp.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    sp.set_defaults(func=status_cmd)

    # logs (of a run)
    lp = sub.add_parser("logs", help="Print a run's logs")
    lp.add_argument("run", help="Run id")
    lp.add_argument("--server", default=None, help="Brokoli server URL")
    lp.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    lp.add_argument("--api-key", default="", help="API key for authentication")
    lp.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    lp.add_argument(
        "--level",
        choices=["debug", "info", "warning", "error"],
        help="Only show logs at this level",
    )
    lp.add_argument("--node", metavar="NODE_ID", help="Only show logs from this node")
    lp.set_defaults(func=logs_cmd)

    # cancel (a run)
    cn = sub.add_parser("cancel", help="Cancel an in-progress run")
    cn.add_argument("run", help="Run id")
    cn.add_argument("--server", default=None, help="Brokoli server URL")
    cn.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    cn.add_argument("--api-key", default="", help="API key for authentication")
    cn.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    cn.set_defaults(func=cancel_cmd)

    # retry (resume a run)
    rt = sub.add_parser("retry", help="Retry a run, resuming from where it stopped")
    rt.add_argument("run", help="Run id")
    rt.add_argument("--server", default=None, help="Brokoli server URL")
    rt.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    rt.add_argument("--api-key", default="", help="API key for authentication")
    rt.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    rt.set_defaults(func=retry_cmd)

    # backfill (a pipeline over a date range)
    bf = sub.add_parser("backfill", help="Backfill a pipeline over a date range")
    bf.add_argument("pipeline", help="Pipeline id, name, or logical id")
    bf.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date")
    bf.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="End date")
    bf.add_argument("--server", default=None, help="Brokoli server URL")
    bf.add_argument(
        "--workspace",
        default=None,
        help="Workspace id to act in (X-Workspace-ID); default: your default workspace",
    )
    bf.add_argument("--api-key", default="", help="API key for authentication")
    bf.add_argument(
        "--env", default=None, help="Named environment from brokoli.yaml (server + token_env)"
    )
    bf.set_defaults(func=backfill_cmd)

    # export
    ep = sub.add_parser("export", help="Export pipeline as YAML (default) or JSON")
    ep.add_argument("file", help="Python file containing pipeline")
    ep.add_argument("-o", "--output", help="Output file path")
    ep.add_argument(
        "-f",
        "--format",
        choices=["yaml", "json"],
        default="yaml",
        help="Output format (default: yaml)",
    )
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
