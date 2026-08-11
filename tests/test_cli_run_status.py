"""brokoli-sdk#15 M3: the `run` and `status` operational commands.

These are thin, well-tested wrappers over the backend run API:
    run    -> POST /api/pipelines/{id}/run   (async; returns a run id)
    status -> GET  /api/runs/{id}
"""

import argparse

import pytest

import brokoli.cli as cli
from brokoli.exceptions import DeployError


def _ns(**kw):
    return argparse.Namespace(**kw)


class TestParseParams:
    def test_key_value_pairs(self):
        assert cli._parse_params(["a=1", "b=2"], "run") == {"a": "1", "b": "2"}

    def test_value_may_contain_equals(self):
        assert cli._parse_params(["q=a=b"], "run") == {"q": "a=b"}

    def test_empty(self):
        assert cli._parse_params(None, "run") == {}

    def test_rejects_missing_equals(self):
        with pytest.raises(DeployError, match="KEY=VALUE"):
            cli._parse_params(["nope"], "run")

    def test_rejects_empty_key(self):
        with pytest.raises(DeployError, match="KEY=VALUE"):
            cli._parse_params(["=x"], "run")


class TestResolvePipelineId:
    def _remote(self):
        return [
            {"id": "uuid-1", "pipeline_id": "orders", "name": "Orders"},
            {"id": "uuid-2", "pipeline_id": "events", "name": "Events"},
        ]

    def test_exact_internal_id_wins(self, monkeypatch):
        monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *a: self._remote())
        assert cli._resolve_pipeline_id("s", "", "uuid-2", "run") == "uuid-2"

    def test_logical_pipeline_id(self, monkeypatch):
        monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *a: self._remote())
        assert cli._resolve_pipeline_id("s", "", "orders", "run") == "uuid-1"

    def test_name_fallback(self, monkeypatch):
        monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *a: self._remote())
        assert cli._resolve_pipeline_id("s", "", "Events", "run") == "uuid-2"

    def test_no_match_raises(self, monkeypatch):
        monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *a: self._remote())
        with pytest.raises(DeployError, match="No pipeline matching"):
            cli._resolve_pipeline_id("s", "", "ghost", "run")

    def test_ambiguous_raises(self, monkeypatch):
        dupes = [
            {"id": "a", "pipeline_id": "", "name": "Dup"},
            {"id": "b", "pipeline_id": "", "name": "Dup"},
        ]
        monkeypatch.setattr(cli, "_list_remote_pipelines", lambda *a: dupes)
        with pytest.raises(DeployError, match="matches multiple"):
            cli._resolve_pipeline_id("s", "", "Dup", "run")


class TestRun:
    def test_triggers_and_prints_run_id(self, monkeypatch, capsys):
        seen = {}
        monkeypatch.setattr(cli, "_resolve_pipeline_id", lambda *a: "orders-uuid")

        def fake_post(url, auth, body, operation):
            seen.update(url=url, body=body, operation=operation)
            return {"id": "run-123", "status": "pending"}

        monkeypatch.setattr(cli, "_post_json", fake_post)
        rc = cli.run_cmd(
            _ns(pipeline="orders", server="https://s", api_key="k", param=None)
        )
        assert rc == 0
        # the resolved internal id, not the user-supplied identifier
        assert seen["url"] == "https://s/api/pipelines/orders-uuid/run"
        assert seen["operation"] == "run"
        assert seen["body"] == {}
        out = capsys.readouterr().out
        assert "run-123" in out
        assert "brokoli status run-123" in out

    def test_sends_params(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(cli, "_resolve_pipeline_id", lambda *a: "p-uuid")
        monkeypatch.setattr(
            cli, "_post_json",
            lambda url, auth, body, operation: seen.update(body=body) or {"id": "r"},
        )
        cli.run_cmd(
            _ns(pipeline="p", server="https://s", api_key="", param=["x=1", "y=2"])
        )
        assert seen["body"] == {"params": {"x": "1", "y": "2"}}

    def test_bad_param_raises_before_any_request(self, monkeypatch):
        called = {"n": 0}
        monkeypatch.setattr(
            cli, "_resolve_pipeline_id",
            lambda *a: called.__setitem__("n", called["n"] + 1),
        )
        monkeypatch.setattr(
            cli, "_post_json",
            lambda *a, **k: called.__setitem__("n", called["n"] + 1),
        )
        with pytest.raises(DeployError, match="KEY=VALUE"):
            cli.run_cmd(_ns(pipeline="p", server="s", api_key="", param=["bad"]))
        assert called["n"] == 0


class TestStatus:
    def test_fetches_and_formats(self, monkeypatch, capsys):
        seen = {}

        def fake_get(url, auth, operation):
            seen.update(url=url, operation=operation)
            return {
                "id": "run-9",
                "pipeline_id": "orders",
                "status": "failed",
                "started_at": "2026-08-11T00:00:00Z",
                "finished_at": "2026-08-11T00:01:00Z",
                "error": "node X blew up",
                "node_runs": [
                    {"status": "success"},
                    {"status": "success"},
                    {"status": "failed"},
                ],
            }

        monkeypatch.setattr(cli, "_get_json", fake_get)
        rc = cli.status_cmd(_ns(run="run-9", server="https://s", api_key="k"))
        assert rc == 0
        assert seen["url"] == "https://s/api/runs/run-9"
        assert seen["operation"] == "status"
        out = capsys.readouterr().out
        assert "run-9" in out
        assert "failed" in out
        assert "node X blew up" in out
        # node summary counts by status
        assert "3 (" in out and "2 success" in out and "1 failed" in out

    def test_minimal_run_object(self, monkeypatch, capsys):
        monkeypatch.setattr(
            cli, "_get_json",
            lambda url, auth, operation: {"id": "r", "pipeline_id": "p", "status": "pending"},
        )
        cli.status_cmd(_ns(run="r", server="s", api_key=""))
        out = capsys.readouterr().out
        assert "pending" in out
        assert "Error" not in out  # no error line when there's no error

    def test_rejects_non_object_response(self, monkeypatch):
        monkeypatch.setattr(cli, "_get_json", lambda url, auth, operation: ["nope"])
        with pytest.raises(DeployError, match="expected an object"):
            cli.status_cmd(_ns(run="r", server="s", api_key=""))


class TestMainDispatch:
    def test_run_is_wired_and_parses_args(self, monkeypatch):
        import sys

        captured = {}
        # set_defaults(func=run_cmd) in main() resolves the name at call
        # time, so patching the module global here reaches the subparser.
        monkeypatch.setattr(
            cli, "run_cmd",
            lambda args: captured.setdefault("args", args) or 0,
        )
        monkeypatch.setattr(
            sys, "argv",
            ["brokoli", "run", "orders", "--server", "https://s", "--param", "a=1"],
        )
        cli.main()
        assert captured["args"].pipeline == "orders"
        assert captured["args"].server == "https://s"
        assert captured["args"].param == ["a=1"]

    def test_status_is_wired_and_parses_args(self, monkeypatch):
        import sys

        captured = {}
        monkeypatch.setattr(
            cli, "status_cmd",
            lambda args: captured.setdefault("args", args) or 0,
        )
        monkeypatch.setattr(
            sys, "argv", ["brokoli", "status", "run-7", "--server", "https://s"]
        )
        cli.main()
        assert captured["args"].run == "run-7"
        assert captured["args"].server == "https://s"

    def test_server_is_required_for_run(self, monkeypatch):
        import sys

        monkeypatch.setattr(sys, "argv", ["brokoli", "run", "orders"])
        with pytest.raises(SystemExit):
            cli.main()
