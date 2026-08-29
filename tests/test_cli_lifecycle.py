"""brokoli-sdk#15 M3: the run-lifecycle commands.

logs     -> GET  /api/runs/{id}/logs
cancel   -> POST /api/runs/{id}/cancel
retry    -> POST /api/runs/{id}/resume   (resume-from-failure)
backfill -> POST /api/pipelines/{id}/backfill
"""

import argparse

import pytest

import brokoli.cli as cli
from brokoli.exceptions import DeployError


def _ns(**kw):
    return argparse.Namespace(**kw)


class TestLogs:
    def test_prints_entries(self, monkeypatch, capsys):
        seen = {}

        def fake_get(url, auth, operation):
            seen.update(url=url, operation=operation)
            return [
                {"timestamp": "T1", "level": "info", "node_id": "n1", "message": "started"},
                {"timestamp": "T2", "level": "error", "node_id": "n2", "message": "boom"},
            ]

        monkeypatch.setattr(cli, "_get_json", fake_get)
        rc = cli.logs_cmd(_ns(run="r1", server="https://s", api_key="k", level=None, node=None))
        assert rc == 0
        assert seen["url"] == "https://s/api/runs/r1/logs"
        assert seen["operation"] == "logs"
        out = capsys.readouterr().out
        assert "started" in out and "boom" in out
        assert "INFO" in out and "ERROR" in out and "n2" in out

    def test_level_and_node_become_query_params(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(
            cli,
            "_get_json",
            lambda url, auth, operation: seen.update(url=url) or [],
        )
        cli.logs_cmd(_ns(run="r1", server="https://s", api_key="", level="error", node="n9"))
        assert "level=error" in seen["url"]
        assert "node_id=n9" in seen["url"]
        assert seen["url"].startswith("https://s/api/runs/r1/logs?")

    def test_empty_logs(self, monkeypatch, capsys):
        monkeypatch.setattr(cli, "_get_json", lambda *a, **k: [])
        cli.logs_cmd(_ns(run="r", server="s", api_key="", level=None, node=None))
        assert "(no logs)" in capsys.readouterr().out

    def test_rejects_non_list(self, monkeypatch):
        monkeypatch.setattr(cli, "_get_json", lambda *a, **k: {"not": "a list"})
        with pytest.raises(DeployError, match="expected a list"):
            cli.logs_cmd(_ns(run="r", server="s", api_key="", level=None, node=None))


class TestCancel:
    def test_cancels_and_reports(self, monkeypatch, capsys):
        seen = {}

        def fake_post(url, auth, body, operation):
            seen.update(url=url, body=body, operation=operation)
            return {"status": "cancelled"}

        monkeypatch.setattr(cli, "_post_json", fake_post)
        rc = cli.cancel_cmd(_ns(run="r1", server="https://s", api_key="k"))
        assert rc == 0
        assert seen["url"] == "https://s/api/runs/r1/cancel"
        assert seen["operation"] == "cancel"
        assert seen["body"] == {}
        assert "cancelled" in capsys.readouterr().out


class TestRetry:
    def test_resumes_and_shows_run(self, monkeypatch, capsys):
        seen = {}

        def fake_post(url, auth, body, operation):
            seen.update(url=url, operation=operation)
            return {"id": "r1", "pipeline_id": "p", "status": "running"}

        monkeypatch.setattr(cli, "_post_json", fake_post)
        rc = cli.retry_cmd(_ns(run="r1", server="https://s", api_key="k"))
        assert rc == 0
        assert seen["url"] == "https://s/api/runs/r1/resume"
        assert seen["operation"] == "retry"
        out = capsys.readouterr().out
        assert "Resumed run r1" in out
        assert "running" in out

    def test_handles_empty_body(self, monkeypatch, capsys):
        monkeypatch.setattr(cli, "_post_json", lambda *a, **k: {})
        cli.retry_cmd(_ns(run="r1", server="s", api_key=""))
        assert "Resumed run r1" in capsys.readouterr().out


class TestBackfill:
    def test_resolves_and_triggers(self, monkeypatch, capsys):
        seen = {}
        monkeypatch.setattr(cli, "_resolve_pipeline_id", lambda *a: "p-uuid")

        def fake_post(url, auth, body, operation):
            seen.update(url=url, body=body, operation=operation)
            return {"runs": ["r1", "r2", "r3"], "count": 3}

        monkeypatch.setattr(cli, "_post_json", fake_post)
        rc = cli.backfill_cmd(
            _ns(
                pipeline="orders",
                start="2026-01-01",
                end="2026-01-03",
                server="https://s",
                api_key="k",
            )
        )
        assert rc == 0
        assert seen["url"] == "https://s/api/pipelines/p-uuid/backfill"
        assert seen["operation"] == "backfill"
        assert seen["body"] == {"start_date": "2026-01-01", "end_date": "2026-01-03"}
        out = capsys.readouterr().out
        assert "3 run(s)" in out
        assert "r1" in out and "r3" in out

    def test_reports_partial_error(self, monkeypatch, capsys):
        monkeypatch.setattr(cli, "_resolve_pipeline_id", lambda *a: "p-uuid")
        monkeypatch.setattr(
            cli,
            "_post_json",
            lambda *a, **k: {"runs": ["r1"], "error": "one date failed"},
        )
        cli.backfill_cmd(
            _ns(pipeline="p", start="2026-01-01", end="2026-01-02", server="s", api_key="")
        )
        out = capsys.readouterr().out
        assert "incomplete" in out and "one date failed" in out
        assert "1 run(s)" in out


class TestMainDispatch:
    @pytest.mark.parametrize(
        "argv,attr,expected",
        [
            (["logs", "r1", "--server", "s"], "run", "r1"),
            (["cancel", "r1", "--server", "s"], "run", "r1"),
            (["retry", "r1", "--server", "s"], "run", "r1"),
            (["backfill", "p", "--start", "a", "--end", "b", "--server", "s"], "pipeline", "p"),
        ],
    )
    def test_commands_are_wired(self, monkeypatch, argv, attr, expected):
        import sys

        captured = {}
        cmd = {
            "logs": "logs_cmd",
            "cancel": "cancel_cmd",
            "retry": "retry_cmd",
            "backfill": "backfill_cmd",
        }[argv[0]]
        monkeypatch.setattr(cli, cmd, lambda args: captured.setdefault("args", args) or 0)
        monkeypatch.setattr(sys, "argv", ["brokoli", *argv])
        cli.main()
        assert getattr(captured["args"], attr) == expected

    def test_backfill_requires_start_and_end(self, monkeypatch):
        import sys

        monkeypatch.setattr(sys, "argv", ["brokoli", "backfill", "p", "--server", "s"])
        with pytest.raises(SystemExit):
            cli.main()
