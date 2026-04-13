"""Brokoli CLI — deploy, run, pull pipelines."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

from brokoli.exceptions import DeployError, ValidationError

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


def load_pipeline_from_file(filepath: str) -> list[Any]:
    """Import a Python file and extract all Pipeline objects.

    Raises:
        DeployError: If the file cannot be loaded or contains no pipelines.
    """
    from brokoli.pipeline import Pipeline

    spec = importlib.util.spec_from_file_location("_brokoli_module", filepath)
    if spec is None or spec.loader is None:
        raise DeployError(filepath, 0, f"Cannot load module from {filepath}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    pipelines = [obj for obj in vars(module).values() if isinstance(obj, Pipeline)]
    if not pipelines:
        raise DeployError(
            filepath, 0,
            f"No Pipeline found in {filepath}. Use: with Pipeline('name') as p: ...",
        )

    return pipelines


def _find_existing_pipeline(
    server: str, auth_header: str, name: str,
) -> dict[str, Any] | None:
    """Look up an existing pipeline by name on the server. Returns None on miss."""
    try:
        req = urllib.request.Request(
            f"{server}/api/pipelines",
            headers=_make_headers(auth_header),
        )
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
        existing = json.loads(resp.read())
        return next((p for p in existing if p.get("name") == name), None)
    except Exception:
        return None


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
    from brokoli.validate import validate_pipeline

    server = args.server.rstrip("/")
    auth_header = _auth_header_from_args(args)
    skip_validation: bool = args.skip_validation

    for f in _collect_files(args.file):
        print(f"Loading {f}...")
        pipelines = load_pipeline_from_file(str(f))

        for pipeline in pipelines:
            if not skip_validation:
                print(f"  Validating {pipeline.name}...")
                vr = validate_pipeline(pipeline, server_url=server, auth_header=auth_header)
                vr.print_report()
                if not vr.valid:
                    raise ValidationError(
                        [str(e) for e in vr.errors],
                    )

            payload = pipeline.to_json()
            match = _find_existing_pipeline(server, auth_header, pipeline.name)
            _upsert_pipeline(server, auth_header, pipeline, payload, match)


def validate_cmd(args: argparse.Namespace) -> None:
    """Validate pipeline(s) without deploying."""
    from brokoli.validate import validate_pipeline

    server = args.server.rstrip("/")
    auth_header = _auth_header_from_args(args)

    total_errors = 0
    for f in _collect_files(args.file):
        pipelines = load_pipeline_from_file(str(f))
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


def compile_cmd(args: argparse.Namespace) -> None:
    """Compile a pipeline file to YAML (default) or JSON."""
    fmt = getattr(args, "format", "yaml")
    for f in _collect_files(args.file):
        pipelines = load_pipeline_from_file(str(f))
        for pipeline in pipelines:
            print(_output_pipeline(pipeline, fmt))


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
    dp.set_defaults(func=deploy)

    # validate (without deploying)
    vp = sub.add_parser("validate", help="Validate pipeline(s) without deploying")
    vp.add_argument("file", help="Python file or directory")
    vp.add_argument("--server", default="http://localhost:8080", help="Brokoli server URL (for conn_id checks)")
    vp.add_argument("--api-key", default="", help="API key")
    vp.set_defaults(func=validate_cmd)

    # compile
    cp = sub.add_parser("compile", help="Compile pipeline to YAML (default) or JSON")
    cp.add_argument("file", help="Python file containing pipeline(s)")
    cp.add_argument("-f", "--format", choices=["yaml", "json"], default="yaml", help="Output format (default: yaml)")
    cp.set_defaults(func=compile_cmd)

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
        args.func(args)
    except ValidationError as exc:
        print(f"\nValidation failed: {exc}")
        sys.exit(1)
    except DeployError as exc:
        print(f"\nDeploy error: {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
