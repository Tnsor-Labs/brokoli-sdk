"""Deployment lookup and persistence-order regression tests."""

import argparse
import io
import urllib.error
from unittest.mock import Mock

import pytest

from brokoli import cli
from brokoli.exceptions import DeployError, ValidationError


class FakePipeline:
    def __init__(self, name, pipeline_id):
        self.name = name
        self.pipeline_id = pipeline_id
        self.schedule = ""
        self.sla_deadline = ""

    def to_json(self):
        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "nodes": [],
            "edges": [],
        }


def _args():
    return argparse.Namespace(
        file="pipelines.py",
        server="http://server/",
        api_key="secret",
        skip_validation=True,
        allow_legacy_server=False,
    )


def _install_pipelines(monkeypatch, pipelines):
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipelines.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", lambda _: pipelines)
    monkeypatch.setattr(cli, "preflight_server_compatibility", Mock())


def test_deploy_lists_once_and_matches_pipeline_id_before_name(monkeypatch):
    pipeline = FakePipeline("orders", "orders-v2")
    _install_pipelines(monkeypatch, [pipeline])
    remote = [
        {"id": "wrong", "pipeline_id": "legacy", "name": "orders"},
        {"id": "right", "pipeline_id": "orders-v2", "name": "old name"},
    ]
    list_remote = Mock(return_value=remote)
    upsert = Mock()
    monkeypatch.setattr(cli, "_list_remote_pipelines", list_remote)
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    cli.deploy(_args())

    list_remote.assert_called_once_with("http://server", "Bearer secret", operation="deploy")
    upsert.assert_called_once_with(
        "http://server",
        "Bearer secret",
        pipeline,
        pipeline.to_json(),
        remote[1],
    )


def test_deploy_falls_back_to_name_for_legacy_remote(monkeypatch):
    pipeline = FakePipeline("orders", "orders-v2")
    _install_pipelines(monkeypatch, [pipeline])
    legacy = {"id": "legacy", "name": "orders"}
    monkeypatch.setattr(cli, "_list_remote_pipelines", Mock(return_value=[legacy]))
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    cli.deploy(_args())

    assert upsert.call_args.args[-1] == legacy


def test_deploy_lookup_failure_never_becomes_create(monkeypatch):
    _install_pipelines(monkeypatch, [FakePipeline("orders", "orders")])
    monkeypatch.setattr(
        cli,
        "_list_remote_pipelines",
        Mock(side_effect=DeployError("deploy", 0, "Could not reach server")),
    )
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError, match="Could not reach server"):
        cli.deploy(_args())

    upsert.assert_not_called()


def test_deploy_http_lookup_error_is_labeled_as_deploy(monkeypatch):
    _install_pipelines(monkeypatch, [FakePipeline("orders", "orders")])
    error = urllib.error.HTTPError(
        "http://server/api/pipelines",
        401,
        "unauthorized",
        {},
        io.BytesIO(b"denied"),
    )
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=error))
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError) as exc_info:
        cli.deploy(_args())

    assert exc_info.value.pipeline_name == "deploy"
    assert exc_info.value.status_code == 401
    upsert.assert_not_called()


def test_ambiguous_later_match_blocks_every_upsert(monkeypatch):
    pipelines = [
        FakePipeline("first", "first"),
        FakePipeline("second", "second"),
    ]
    _install_pipelines(monkeypatch, pipelines)
    monkeypatch.setattr(
        cli,
        "_list_remote_pipelines",
        Mock(
            return_value=[
                {"id": "first", "pipeline_id": "first", "name": "first"},
                {"id": "second-a", "pipeline_id": "second", "name": "second"},
                {"id": "second-b", "pipeline_id": "second", "name": "second"},
            ]
        ),
    )
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError, match="Ambiguous remote match"):
        cli.deploy(_args())

    upsert.assert_not_called()


def test_name_match_with_different_logical_id_is_a_conflict(monkeypatch):
    pipeline = FakePipeline("orders", "orders-v2")
    _install_pipelines(monkeypatch, [pipeline])
    monkeypatch.setattr(
        cli,
        "_list_remote_pipelines",
        Mock(return_value=[{"id": "orders-v1", "pipeline_id": "orders-v1", "name": "orders"}]),
    )
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError, match="different pipeline_id"):
        cli.deploy(_args())

    upsert.assert_not_called()


def test_duplicate_local_identity_fails_before_remote_lookup(monkeypatch):
    pipelines = [
        FakePipeline("first", "shared"),
        FakePipeline("second", "shared"),
    ]
    _install_pipelines(monkeypatch, pipelines)
    list_remote = Mock()
    monkeypatch.setattr(cli, "_list_remote_pipelines", list_remote)
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError, match="Duplicate local pipeline_id"):
        cli.deploy(_args())

    list_remote.assert_not_called()
    upsert.assert_not_called()


def test_multiple_locals_cannot_target_the_same_remote(monkeypatch):
    pipelines = [
        FakePipeline("first", "first"),
        FakePipeline("second", "second"),
    ]
    _install_pipelines(monkeypatch, pipelines)
    monkeypatch.setattr(
        cli,
        "_list_remote_pipelines",
        Mock(
            return_value=[
                {"id": "shared", "pipeline_id": "first", "name": "first"},
                {"id": "shared", "pipeline_id": "second", "name": "second"},
            ]
        ),
    )
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    with pytest.raises(DeployError, match="target remote pipeline"):
        cli.deploy(_args())

    upsert.assert_not_called()


def test_distinct_logical_ids_may_share_a_display_name(monkeypatch):
    pipelines = [
        FakePipeline("orders", "orders-us"),
        FakePipeline("orders", "orders-eu"),
    ]
    _install_pipelines(monkeypatch, pipelines)
    remote = [
        {"id": "us", "pipeline_id": "orders-us", "name": "orders"},
        {"id": "eu", "pipeline_id": "orders-eu", "name": "orders"},
    ]
    monkeypatch.setattr(cli, "_list_remote_pipelines", Mock(return_value=remote))
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)

    cli.deploy(_args())

    assert [call.args[-1]["id"] for call in upsert.call_args_list] == ["us", "eu"]


def test_diff_rejects_multiple_locals_targeting_one_remote(monkeypatch):
    pipelines = [
        FakePipeline("first", "first"),
        FakePipeline("second", "second"),
    ]
    _install_pipelines(monkeypatch, pipelines)
    monkeypatch.setattr(
        cli,
        "_list_remote_pipelines",
        Mock(
            return_value=[
                {"id": "shared", "pipeline_id": "first", "name": "first"},
                {"id": "shared", "pipeline_id": "second", "name": "second"},
            ]
        ),
    )

    with pytest.raises(DeployError, match="target remote pipeline"):
        cli.diff_cmd(argparse.Namespace(file="pipelines.py", server="http://server", api_key=""))


def test_invalid_pipeline_is_reported_before_remote_lookup(monkeypatch):
    pipeline = FakePipeline("orders", "orders")
    _install_pipelines(monkeypatch, [pipeline])
    result = Mock(valid=False, errors=["invalid node"])
    monkeypatch.setattr("brokoli.validation.validate_pipeline", Mock(return_value=result))
    list_remote = Mock()
    monkeypatch.setattr(cli, "_list_remote_pipelines", list_remote)
    upsert = Mock()
    monkeypatch.setattr(cli, "_upsert_pipeline", upsert)
    args = _args()
    args.skip_validation = False

    with pytest.raises(ValidationError):
        cli.deploy(args)

    result.print_report.assert_called_once_with()
    list_remote.assert_not_called()
    upsert.assert_not_called()
