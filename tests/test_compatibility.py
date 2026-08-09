"""Tests for server capability negotiation and CLI preflight ordering."""

import argparse
import io
import json
import sys
import urllib.error
from unittest.mock import Mock

import pytest

from brokoli import Pipeline, condition_node, notify, source_db
from brokoli import cli
from brokoli.compatibility import (
    LegacyServerWarning,
    fetch_server_capabilities,
    preflight_server_compatibility,
)
from brokoli.exceptions import CompatibilityError


class FakeResponse:
    def __init__(self, payload):
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode()
        elif isinstance(payload, str):
            payload = payload.encode()
        self.payload = payload

    def read(self):
        return self.payload


class FakePipeline:
    def __init__(self, name, ir_version="2.0"):
        self.name = name
        self.ir_version = ir_version

    def to_json(self):
        return {
            "name": self.name,
            "ir_version": self.ir_version,
            "nodes": [],
            "edges": [],
        }


def http_error(code):
    return urllib.error.HTTPError(
        "http://server/api/capabilities",
        code,
        "error",
        {},
        io.BytesIO(b"{}"),
    )


def test_fetches_capabilities_with_auth_and_normalized_url(monkeypatch):
    urlopen = Mock(
        return_value=FakeResponse({"supported_ir_versions": ["1.0", "2.0"]})
    )
    monkeypatch.setattr("urllib.request.urlopen", urlopen)

    capabilities = fetch_server_capabilities(
        "http://server/", "Bearer secret"
    )

    assert capabilities.supported_ir_versions == ("1.0", "2.0")
    request = urlopen.call_args.args[0]
    assert request.full_url == "http://server/api/capabilities"
    assert request.get_header("Authorization") == "Bearer secret"
    assert urlopen.call_args.kwargs["timeout"] == 10


@pytest.mark.parametrize("code", [401, 403, 500])
def test_http_failures_are_not_legacy_bypassable(monkeypatch, code):
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=http_error(code)))

    with pytest.raises(CompatibilityError, match=f"HTTP {code}"):
        fetch_server_capabilities(
            "http://server", allow_legacy_server=True
        )


@pytest.mark.parametrize("code", [404, 405])
def test_missing_legacy_endpoint_requires_explicit_override(monkeypatch, code):
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=http_error(code)))

    with pytest.raises(CompatibilityError, match="--allow-legacy-server"):
        fetch_server_capabilities("http://server")

    with pytest.warns(LegacyServerWarning, match="could not be verified"):
        result = fetch_server_capabilities(
            "http://server", allow_legacy_server=True
        )
    assert result is None


def test_transport_failure_requires_explicit_override(monkeypatch):
    error = urllib.error.URLError("connection refused")
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=error))

    with pytest.raises(CompatibilityError, match="Could not reach"):
        fetch_server_capabilities("http://server")

    with pytest.warns(LegacyServerWarning):
        result = fetch_server_capabilities(
            "http://server", allow_legacy_server=True
        )
    assert result is None


@pytest.mark.parametrize(
    "payload, message",
    [
        ("not-json", "malformed JSON"),
        ([], "expected a JSON object"),
        ({}, "supported_ir_versions"),
        ({"supported_ir_versions": []}, "supported_ir_versions"),
        ({"supported_ir_versions": ["2.0", 2]}, "supported_ir_versions"),
    ],
)
def test_malformed_payloads_fail_closed(monkeypatch, payload, message):
    monkeypatch.setattr(
        "urllib.request.urlopen", Mock(return_value=FakeResponse(payload))
    )

    with pytest.raises(CompatibilityError, match=message):
        fetch_server_capabilities(
            "http://server", allow_legacy_server=True
        )


def test_reported_ir_mismatch_cannot_be_bypassed(monkeypatch):
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(return_value=FakeResponse({"supported_ir_versions": ["1.0"]})),
    )

    with pytest.raises(CompatibilityError, match="requires IR 2.0") as exc_info:
        preflight_server_compatibility(
            [FakePipeline("orders")],
            "http://server",
            allow_legacy_server=True,
        )

    assert "cannot override" in str(exc_info.value)


def test_conditional_pipeline_preflight_requires_ir_21(monkeypatch):
    with Pipeline("conditional") as pipeline:
        source = source_db("Source", query="SELECT 1")
        gate = condition_node("Gate", "always_true", source)
        gate.when(notify("Selected", webhook_url="https://example.test"))

    assert pipeline.to_json()["ir_version"] == "2.1"
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(return_value=FakeResponse({"supported_ir_versions": ["2.0"]})),
    )
    with pytest.raises(CompatibilityError, match="requires IR 2.1"):
        preflight_server_compatibility([pipeline], "http://server")


def test_conditional_pipeline_preflight_accepts_ir_21(monkeypatch):
    with Pipeline("conditional") as pipeline:
        source = source_db("Source", query="SELECT 1")
        gate = condition_node("Gate", "always_false", source)
        gate.otherwise(notify("Selected", webhook_url="https://example.test"))

    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(
            return_value=FakeResponse(
                {"supported_ir_versions": ["2.0", "2.1"]}
            )
        ),
    )
    preflight_server_compatibility([pipeline], "http://server")


def test_deploy_loads_and_preflights_all_pipelines_before_persistence(monkeypatch):
    pipelines_by_file = {
        "first.py": [FakePipeline("first")],
        "second.py": [FakePipeline("second")],
    }
    events = []
    args = argparse.Namespace(
        file="pipelines",
        server="http://server",
        api_key="",
        skip_validation=True,
        allow_legacy_server=False,
    )

    monkeypatch.setattr(
        cli, "_collect_files", lambda _: list(pipelines_by_file)
    )
    monkeypatch.setattr(
        cli,
        "load_pipeline_from_file",
        lambda path: pipelines_by_file[path],
    )
    monkeypatch.setattr(
        cli,
        "preflight_server_compatibility",
        lambda pipelines, *args, **kwargs: events.append(
            ("preflight", [pipeline.name for pipeline in pipelines])
        ),
    )
    monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        cli,
        "_upsert_pipeline",
        lambda *args: events.append(("upsert", args[2].name)),
    )

    cli.deploy(args)

    assert events == [
        ("preflight", ["first", "second"]),
        ("upsert", "first"),
        ("upsert", "second"),
    ]


def test_incompatible_second_pipeline_blocks_every_upsert(monkeypatch):
    args = argparse.Namespace(
        file="pipelines",
        server="http://server",
        api_key="",
        skip_validation=True,
        allow_legacy_server=False,
    )
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipelines.py"])
    monkeypatch.setattr(
        cli,
        "load_pipeline_from_file",
        lambda _: [FakePipeline("first"), FakePipeline("second", "3.0")],
    )
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(return_value=FakeResponse({"supported_ir_versions": ["2.0"]})),
    )
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(CompatibilityError, match="second"):
        cli.deploy(args)

    upsert.assert_not_called()


def test_validate_uses_the_same_preflight(monkeypatch):
    args = argparse.Namespace(
        file="pipeline.py",
        server="http://server",
        api_key="",
        allow_legacy_server=True,
    )
    pipeline = FakePipeline("orders")
    preflight = Mock(side_effect=CompatibilityError("blocked"))
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipeline.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", lambda _: [pipeline])
    monkeypatch.setattr(cli, "preflight_server_compatibility", preflight)

    with pytest.raises(CompatibilityError, match="blocked"):
        cli.validate_cmd(args)

    preflight.assert_called_once_with(
        [pipeline],
        "http://server",
        "",
        allow_legacy_server=True,
    )


@pytest.mark.parametrize(
    "command, handler_name",
    [("deploy", "deploy"), ("validate", "validate_cmd")],
)
def test_cli_accepts_legacy_override_for_preflight(
    monkeypatch, command, handler_name
):
    handler = Mock()
    monkeypatch.setattr(cli, handler_name, handler)
    monkeypatch.setattr(
        sys,
        "argv",
        ["brokoli", command, "pipeline.py", "--allow-legacy-server"],
    )

    cli.main()

    assert handler.call_args.args[0].allow_legacy_server is True
