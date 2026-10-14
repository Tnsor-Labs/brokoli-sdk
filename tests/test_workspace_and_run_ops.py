"""Workspace targeting (X-Workspace-ID) and the dry-run, backfill and
resume run operations, against the in-process fake server."""

from __future__ import annotations

import argparse
import datetime

import pytest

import brokoli.cli as cli
from brokoli import Pipeline, source_db
from brokoli.client import Client
from brokoli.validation import validate_pipeline

from conftest import PIPES, FakeBrokoli


def _client(server, **kwargs):
    FakeBrokoli.tokens.add("k")
    FakeBrokoli.use_cursor_shape = False
    FakeBrokoli.pipelines_flat = PIPES
    return Client(server, api_key="k", **kwargs)


class TestWorkspace:
    def test_every_client_request_carries_the_workspace(self, server):
        client = _client(server, workspace="team-a")
        client.run(PIPES[0]["id"]).detail()
        assert FakeBrokoli.seen, "no requests recorded"
        assert {ws for _m, _p, ws in FakeBrokoli.seen} == {"team-a"}

    def test_no_workspace_sends_no_header(self, server):
        client = _client(server)
        client.pipelines()
        assert {ws for _m, _p, ws in FakeBrokoli.seen} == {None}

    def test_from_env_reads_brokoli_workspace(self, server, monkeypatch):
        monkeypatch.setenv("BROKOLI_WORKSPACE", "team-b")
        monkeypatch.setenv("BROKOLI_TOKEN", "k")
        assert Client.from_env(server).workspace == "team-b"
        assert Client.from_env(server, workspace="explicit").workspace == "explicit"

    def test_validation_lists_connections_in_the_workspace(self, server):
        FakeBrokoli.tokens.add("k")
        with Pipeline("p") as p:
            source_db("Read", query="SELECT 1", conn_id="warehouse")
        validate_pipeline(p, server_url=server, auth_header="Bearer k", workspace="team-a")
        assert ("GET", "/api/connections", "team-a") in FakeBrokoli.seen

    def test_cli_flag_env_and_named_environment(self, server, monkeypatch, tmp_path):
        FakeBrokoli.tokens.add("k")
        FakeBrokoli.run_statuses = {"r1": "success"}
        monkeypatch.setenv("BROKOLI_TOKEN", "k")

        def status(**kw):
            FakeBrokoli.seen = []
            ns = argparse.Namespace(run="r1", server=server, api_key="", env=None, **kw)
            cli.status_cmd(ns)
            return {ws for _m, _p, ws in FakeBrokoli.seen}

        assert status(workspace="flag") == {"flag"}
        monkeypatch.setenv("BROKOLI_WORKSPACE", "from-env")
        assert status(workspace=None) == {"from-env"}
        assert status(workspace="flag") == {"flag"}
        monkeypatch.delenv("BROKOLI_WORKSPACE")

        config = tmp_path / "brokoli.yaml"
        config.write_text(f"environments:\n  prod: {{server: {server}, workspace: from-config}}\n")
        monkeypatch.setenv("BROKOLI_CONFIG", str(config))
        FakeBrokoli.seen = []
        cli.status_cmd(argparse.Namespace(run="r1", server=None, api_key="", env="prod", workspace=None))
        assert {ws for _m, _p, ws in FakeBrokoli.seen} == {"from-config"}

        # A later command without one does not inherit the last command's.
        assert status(workspace=None) == {None}


class TestRunOperations:
    def test_dry_run(self, server):
        client = _client(server)
        result = client.dry_run(PIPES[0]["pipeline_id"])
        assert result["results"][0]["node_id"] == "n1"
        assert FakeBrokoli.run_ops == [("dry-run", PIPES[0]["id"], {})]

    @pytest.mark.parametrize(
        ("start", "end", "body"),
        [
            ("2026-10-01", "2026-10-03", {"start_date": "2026-10-01", "end_date": "2026-10-03"}),
            (
                datetime.date(2026, 10, 1),
                datetime.date(2026, 10, 3),
                {"start_date": "2026-10-01", "end_date": "2026-10-03"},
            ),
            (
                datetime.datetime(2026, 10, 1, tzinfo=datetime.timezone.utc),
                "2026-10-03T00:00:00Z",
                {"start": "2026-10-01T00:00:00+00:00", "end": "2026-10-03T00:00:00Z"},
            ),
        ],
    )
    def test_backfill_bounds(self, server, start, end, body):
        client = _client(server)
        plan = client.backfill(PIPES[0]["id"], start, end)
        assert plan["intervals"] == 3
        assert FakeBrokoli.run_ops == [("backfill", PIPES[0]["id"], body)]

    def test_backfill_force_and_naive_datetime(self, server):
        client = _client(server)
        client.backfill(PIPES[0]["id"], "2026-10-01", "2026-10-02", force=True)
        assert FakeBrokoli.run_ops[0][2]["force"] is True
        with pytest.raises(ValueError, match="timezone"):
            client.backfill(PIPES[0]["id"], datetime.datetime(2026, 10, 1), "2026-10-02")

    def test_resume_returns_the_new_run(self, server):
        client = _client(server)
        resumed = client.run_handle("old-run").resume()
        assert resumed.id == "resumed-1"
        assert FakeBrokoli.run_ops == [("resume", "old-run", {})]

    def test_resume_from_node(self, server):
        client = _client(server)
        resumed = client.resume("old-run", from_node="load")
        assert resumed.id == "resumed-1"
        assert FakeBrokoli.run_ops == [("resume", "old-run", {"from_node": "load"})]
