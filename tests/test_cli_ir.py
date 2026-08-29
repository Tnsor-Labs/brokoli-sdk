"""CLI tests for normalized compile, check, and server diff."""

import argparse
import io
import json
import sys
import urllib.error
from unittest.mock import Mock

import pytest

from brokoli import cli
from brokoli.exceptions import DeployError
from brokoli.ir import normalize_ir


class FakeResponse:
    def __init__(self, payload):
        if not isinstance(payload, bytes):
            payload = json.dumps(payload).encode()
        self.payload = payload

    def read(self):
        return self.payload


class FakePipeline:
    def __init__(self, name="orders", pipeline_id="orders", value=1):
        self.name = name
        self.pipeline_id = pipeline_id
        self.value = value

    def to_json(self):
        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "nodes": [
                {
                    "id": "node",
                    "type": "code",
                    "name": "Node",
                    "config": {"value": self.value},
                }
            ],
            "edges": [],
        }

    def to_normalized_json(self):
        return normalize_ir(self.to_json())


def test_normalized_compile_uses_canonical_json_and_valid_array(monkeypatch, capsys):
    pipelines = [FakePipeline("one"), FakePipeline("two")]
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipelines.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", lambda _: pipelines)
    args = argparse.Namespace(file="pipelines.py", format="yaml", normalized=True, check=False)

    assert cli.compile_cmd(args) == 0
    output = capsys.readouterr().out
    assert [item["name"] for item in json.loads(output)] == ["one", "two"]
    assert output.endswith("\n") and not output.endswith("\n\n")


def test_normalized_compile_loads_temporary_pipeline_file(tmp_path, capsys):
    pipeline_file = tmp_path / "pipeline.py"
    pipeline_file.write_text(
        "from brokoli import Pipeline, source_db\n"
        "with Pipeline('temporary') as pipeline:\n"
        "    source_db('Source', query='SELECT 1', conn_id='warehouse')\n"
    )
    args = argparse.Namespace(file=str(pipeline_file), format="yaml", normalized=True, check=False)

    assert cli.compile_cmd(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["name"] == "temporary"
    assert "position" not in output["nodes"][0]


@pytest.mark.parametrize("valid, expected", [(True, 0), (False, 1)])
def test_compile_check_imports_once_emits_no_ir_and_returns_status(
    monkeypatch, capsys, valid, expected
):
    pipeline = FakePipeline()
    loader = Mock(return_value=[pipeline])
    result = Mock(valid=valid)
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipeline.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", loader)
    monkeypatch.setattr("brokoli.validation.validate_pipeline", Mock(return_value=result))
    args = argparse.Namespace(file="pipeline.py", format="yaml", normalized=False, check=True)

    assert cli.compile_cmd(args) == expected
    loader.assert_called_once_with("pipeline.py")
    result.print_report.assert_called_once_with()
    assert '"pipeline_id"' not in capsys.readouterr().out


@pytest.mark.parametrize("value", [{"not-json"}, float("nan")])
def test_compile_check_reports_normalized_serialization_failure(monkeypatch, capsys, value):
    pipeline = FakePipeline(value=value)
    loader = Mock(return_value=[pipeline])
    result = Mock(valid=True)
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipeline.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", loader)
    monkeypatch.setattr("brokoli.validation.validate_pipeline", Mock(return_value=result))
    args = argparse.Namespace(file="pipeline.py", format="yaml", normalized=False, check=True)

    assert cli.compile_cmd(args) == 1
    loader.assert_called_once_with("pipeline.py")
    captured = capsys.readouterr()
    assert "Normalized IR is not canonical JSON" in captured.out
    assert '"pipeline_id"' not in captured.out
    assert captured.err == ""


def _diff_args(api_key=""):
    return argparse.Namespace(file="pipeline.py", server="http://server/", api_key=api_key)


def _install_pipeline(monkeypatch, pipeline):
    monkeypatch.setattr(cli, "_collect_files", lambda _: ["pipeline.py"])
    monkeypatch.setattr(cli, "load_pipeline_from_file", lambda _: [pipeline])


def test_diff_equal_legacy_list_fetches_detail_with_auth(monkeypatch, capsys):
    pipeline = FakePipeline()
    _install_pipeline(monkeypatch, pipeline)
    urlopen = Mock(
        side_effect=[
            FakeResponse([{"id": "server/id", "pipeline_id": "orders", "name": "old"}]),
            FakeResponse({**pipeline.to_json(), "id": "owned", "created_at": "now"}),
        ]
    )
    monkeypatch.setattr("urllib.request.urlopen", urlopen)

    assert cli.diff_cmd(_diff_args("secret")) == 0
    assert capsys.readouterr().out == "No semantic changes: orders\n"
    requests = [call.args[0] for call in urlopen.call_args_list]
    assert requests[0].full_url == "http://server/api/pipelines?limit=100"
    assert requests[1].full_url == "http://server/api/pipelines/server%2Fid"
    assert all(req.get_header("Authorization") == "Bearer secret" for req in requests)
    assert all(call.kwargs["timeout"] == cli.REQUEST_TIMEOUT for call in urlopen.call_args_list)


def test_diff_cursor_pagination_name_fallback_and_difference(monkeypatch, capsys):
    pipeline = FakePipeline(value=2)
    _install_pipeline(monkeypatch, pipeline)
    urlopen = Mock(
        side_effect=[
            FakeResponse({"items": [], "has_next": True, "cursor": "a/b +"}),
            FakeResponse(
                {"items": [{"id": "remote", "name": "orders"}], "has_next": False, "cursor": None}
            ),
            FakeResponse(FakePipeline(value=1).to_json()),
        ]
    )
    monkeypatch.setattr("urllib.request.urlopen", urlopen)

    assert cli.diff_cmd(_diff_args()) == 1
    output = capsys.readouterr().out
    assert '"value": 1' in output and '"value": 2' in output
    assert (
        urlopen.call_args_list[1]
        .args[0]
        .full_url.endswith("/api/pipelines?limit=100&after=a%2Fb+%2B")
    )


def test_diff_missing_remote_shows_local_as_added(monkeypatch, capsys):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr("urllib.request.urlopen", Mock(return_value=FakeResponse([])))

    assert cli.diff_cmd(_diff_args()) == 1
    output = capsys.readouterr().out
    assert "--- server/orders" in output
    assert "+++ local/orders" in output
    assert '+  "name": "orders"' in output


def test_diff_rejects_ambiguous_matches_without_detail_get(monkeypatch):
    _install_pipeline(monkeypatch, FakePipeline())
    urlopen = Mock(
        return_value=FakeResponse(
            [
                {"id": "one", "pipeline_id": "orders", "name": "orders"},
                {"id": "two", "pipeline_id": "orders", "name": "orders"},
            ]
        )
    )
    monkeypatch.setattr("urllib.request.urlopen", urlopen)

    with pytest.raises(Exception, match="Ambiguous remote match"):
        cli.diff_cmd(_diff_args())
    assert urlopen.call_count == 1


@pytest.mark.parametrize(
    "response, message",
    [
        (FakeResponse(b"not-json"), "Malformed JSON"),
        (FakeResponse({"items": "bad", "has_next": False}), "Malformed pipeline list"),
    ],
)
def test_diff_rejects_malformed_responses(monkeypatch, response, message):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr("urllib.request.urlopen", Mock(return_value=response))
    with pytest.raises(Exception, match=message):
        cli.diff_cmd(_diff_args())


@pytest.mark.parametrize(
    "detail, message",
    [
        ({}, "name must be a string"),
        ({"error": "unauthorized"}, "name must be a string"),
        ({"name": "orders", "nodes": {}, "edges": []}, "nodes must be a list or null"),
        ({"name": "orders", "nodes": [], "edges": {}}, "edges must be a list or null"),
    ],
)
def test_diff_rejects_malformed_pipeline_detail(monkeypatch, detail, message):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(
            side_effect=[
                FakeResponse([{"id": "remote", "pipeline_id": "orders"}]),
                FakeResponse(detail),
            ]
        ),
    )

    with pytest.raises(DeployError, match=message):
        cli.diff_cmd(_diff_args())


@pytest.mark.parametrize(
    "nodes, edges, message",
    [
        ([None], [], r"nodes\[0\] must be an object"),
        ([7], [], r"nodes\[0\] must be an object"),
        ([{"id": "", "type": "code", "name": "Node"}], [], r"nodes\[0\].id"),
        ([{"id": "n", "type": 1, "name": "Node"}], [], r"nodes\[0\].type"),
        ([{"id": "n", "type": "code", "name": None}], [], r"nodes\[0\].name"),
        (
            [{"id": "n", "type": "code", "name": "Node", "config": []}],
            [],
            r"nodes\[0\].config",
        ),
        (
            [{"id": "n", "type": "code", "name": "Node", "capabilities": [1]}],
            [],
            r"nodes\[0\].capabilities",
        ),
        ([], [None], r"edges\[0\] must be an object"),
        ([], ["bad"], r"edges\[0\] must be an object"),
        ([], [{"from": 1, "to": "n"}], r"edges\[0\].from"),
        ([], [{"from": "n", "to": None}], r"edges\[0\].to"),
        ([], [{"from": "a", "to": "b", "condition": 1}], r"edges\[0\].condition"),
    ],
)
def test_diff_rejects_malformed_pipeline_detail_elements(monkeypatch, nodes, edges, message):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(
            side_effect=[
                FakeResponse([{"id": "remote", "pipeline_id": "orders"}]),
                FakeResponse({"name": "orders", "nodes": nodes, "edges": edges}),
            ]
        ),
    )

    with pytest.raises(DeployError, match=message):
        cli.diff_cmd(_diff_args())


def test_pipeline_detail_normalizes_omitted_legacy_config():
    detail = cli._validate_pipeline_detail(
        {
            "name": "orders",
            "nodes": [{"id": "n", "type": "code", "name": "Node"}],
            "edges": [],
        },
        "orders",
    )

    assert detail["nodes"][0]["config"] == {}
    assert "capabilities" not in detail["nodes"][0]


def test_pipeline_detail_accepts_null_legacy_capabilities():
    detail = cli._validate_pipeline_detail(
        {
            "name": "orders",
            "nodes": [
                {
                    "id": "n",
                    "type": "source_db",
                    "name": "Source",
                    "config": {},
                    "capabilities": None,
                }
            ],
            "edges": [],
        },
        "orders",
    )

    assert detail["nodes"][0]["capabilities"] is None
    assert normalize_ir(detail)["nodes"][0]["capabilities"] == [
        "dataset-output",
        "source",
    ]


def test_diff_accepts_null_legacy_nodes_and_edges(monkeypatch, capsys):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(
            side_effect=[
                FakeResponse([{"id": "remote", "pipeline_id": "orders"}]),
                FakeResponse({"name": "orders", "nodes": None, "edges": None}),
            ]
        ),
    )

    assert cli.diff_cmd(_diff_args()) == 1
    assert "+++ local/orders" in capsys.readouterr().out


def test_diff_converts_http_errors_to_operational_error(monkeypatch):
    _install_pipeline(monkeypatch, FakePipeline())
    error = urllib.error.HTTPError("http://server", 401, "unauthorized", {}, io.BytesIO(b"denied"))
    monkeypatch.setattr("urllib.request.urlopen", Mock(side_effect=error))
    with pytest.raises(Exception, match="HTTP 401"):
        cli.diff_cmd(_diff_args())


def test_diff_converts_network_errors_to_operational_error(monkeypatch):
    _install_pipeline(monkeypatch, FakePipeline())
    monkeypatch.setattr(
        "urllib.request.urlopen",
        Mock(side_effect=urllib.error.URLError("connection refused")),
    )
    with pytest.raises(Exception, match="Could not reach server"):
        cli.diff_cmd(_diff_args())


def test_main_honors_integer_handler_status(monkeypatch):
    monkeypatch.setattr(cli, "compile_cmd", Mock(return_value=1))
    monkeypatch.setattr(sys, "argv", ["brokoli", "compile", "pipeline.py", "--check"])
    with pytest.raises(SystemExit) as exc_info:
        cli.main()
    assert exc_info.value.code == 1
