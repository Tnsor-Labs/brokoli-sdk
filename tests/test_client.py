"""brokoli-sdk#57 items 1-6: the programmatic run-ops client.

These tests run against a real in-process HTTP server rather than
monkeypatched internals: the behaviors under test — lazy login, exactly-
one 401 re-negotiation, cursor pagination, response-shape absorption,
concurrent firing — live in the transport layer, and a fake at the
urllib seam would test the fake.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from brokoli.client import (
    APIError,
    AuthError,
    Client,
    RunFailed,
)

from conftest import FakeBrokoli, PIPES


def _static_client(server):
    FakeBrokoli.tokens.add("static-key")
    return Client(server, api_key="static-key")


class TestAuth:
    def test_lazy_login_then_requests(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        client = Client(server, username="e2e", password="right")
        assert len(client.pipelines()) == 2
        assert FakeBrokoli.login_calls == 1
        # A second request reuses the session — no re-login.
        client.pipelines()
        assert FakeBrokoli.login_calls == 1

    def test_bad_credentials_raise_auth_error(self, server):
        client = Client(server, username="e2e", password="wrong")
        with pytest.raises(AuthError):
            client.pipelines()

    def test_expired_session_renegotiates_exactly_once(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        client = Client(server, username="e2e", password="right")
        client.pipelines()
        # Kill the session server-side: next request 401s, client must
        # re-login once and succeed.
        FakeBrokoli.tokens = set()
        assert len(client.pipelines()) == 2
        assert FakeBrokoli.login_calls == 2

    def test_static_token_401_is_final(self, server):
        client = Client(server, api_key="never-valid")
        with pytest.raises(AuthError) as exc_info:
            client.pipelines()
        assert exc_info.value.status == 401

    def test_key_and_credentials_are_mutually_exclusive(self, server):
        with pytest.raises(ValueError):
            Client(server, api_key="k", username="u", password="p")


class TestPipelines:
    def test_cursor_pagination_is_flattened(self, server):
        FakeBrokoli.pipelines_pages = [[PIPES[0]], [PIPES[1]]]
        client = _static_client(server)
        assert [p["id"] for p in client.pipelines()] == ["uuid-1", "uuid-2"]

    def test_bare_list_shape(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        client = _static_client(server)
        assert len(client.pipelines()) == 2

    def test_resolution_precedence(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        client = _static_client(server)
        assert client.pipeline("uuid-2")["name"] == "Events"
        assert client.pipeline("orders")["id"] == "uuid-1"
        assert client.pipeline("Events")["id"] == "uuid-2"

    def test_no_match_is_404(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        client = _static_client(server)
        with pytest.raises(APIError) as exc_info:
            client.pipeline("ghost")
        assert exc_info.value.status == 404

    def test_ambiguity_refuses_to_guess(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = [
            {"id": "a", "pipeline_id": "", "name": "Dup"},
            {"id": "b", "pipeline_id": "", "name": "Dup"},
        ]
        client = _static_client(server)
        with pytest.raises(APIError, match="multiple"):
            client.pipeline("Dup")


class TestRuns:
    def _client(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        return _static_client(server)

    @pytest.mark.parametrize("shape", ["run_id", "id", "nested"])
    def test_trigger_absorbs_response_shapes(self, server, shape):
        FakeBrokoli.trigger_shape = shape
        run = self._client(server).run("orders")
        assert run.id == "run-1"
        assert FakeBrokoli.triggered[0]["pipeline"] == "uuid-1"

    def test_params_are_sent(self, server):
        self._client(server).run("orders", params={"day": "2026-08-15"})
        assert FakeBrokoli.triggered[0]["body"] == {"params": {"day": "2026-08-15"}}

    def test_wait_reaches_terminal(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_statuses[run.id] = "running"
        # Flip to terminal from another thread mid-wait.
        flipper = threading.Timer(
            0.3, lambda: FakeBrokoli.run_statuses.__setitem__(run.id, "success")
        )
        flipper.start()
        detail = run.wait(timeout=10, poll_interval=0.05)
        assert detail["status"] == "success"

    def test_wait_absorbs_the_visibility_race(self, server):
        # sdk#72: the trigger response can carry the run id a beat before
        # the run row is readable; the fast first polls must not treat
        # that 404 as fatal.
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_statuses[run.id] = "success"
        FakeBrokoli.run_invisible_polls[run.id] = 3
        detail = run.wait(timeout=10, poll_interval=0.05)
        assert detail["status"] == "success"
        assert FakeBrokoli.run_invisible_polls[run.id] == 0

    def test_wait_404_after_grace_is_fatal(self, server):
        # The grace window is a window, not a blanket: a run that stays
        # invisible past it is genuinely gone and must raise 404.
        client = self._client(server)
        run = client.run("orders")
        del FakeBrokoli.run_statuses[run.id]  # never becomes visible
        with pytest.raises(APIError) as exc_info:
            run.wait(timeout=10, poll_interval=0.05, visibility_grace=0.2)
        assert exc_info.value.status == 404

    def test_wait_timeout_names_last_status(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_statuses[run.id] = "running"
        with pytest.raises(TimeoutError, match="running"):
            run.wait(timeout=0.2, poll_interval=0.05)

    def test_wait_raise_on_failure_carries_detail(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_statuses[run.id] = "failed"
        FakeBrokoli.runs[run.id] = {"error": "node exploded"}
        with pytest.raises(RunFailed, match="node exploded") as exc_info:
            run.wait(timeout=5, poll_interval=0.05, raise_on_failure=True)
        assert exc_info.value.detail["error"] == "node exploded"

    def test_cancel(self, server):
        client = self._client(server)
        run = client.run("orders")
        assert run.cancel()["status"] == "cancelled"
        assert run.status() == "cancelled"

    @pytest.mark.parametrize("shape", ["list", "wrapped"])
    def test_logs_absorbs_shapes(self, server, shape):
        FakeBrokoli.logs_shape = shape
        FakeBrokoli.log_entries = [{"message": "Streamed 4 rule(s) by reference"}]
        client = self._client(server)
        run = client.run("orders")
        assert run.logs()[0]["message"].startswith("Streamed")

    def test_concurrent_firing_is_safe(self, server):
        client = self._client(server)
        with ThreadPoolExecutor(max_workers=8) as pool:
            runs = list(pool.map(lambda _: client.run("orders"), range(16)))
        assert len({r.id for r in runs}) == 16

    def test_node_preview(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.previews[f"{run.id}/aggregate"] = {
            "columns": ["region", "total"],
            "rows": [{"region": "us", "total": 42}],
        }
        preview = run.node_preview("aggregate")
        assert preview["columns"] == ["region", "total"]
        assert preview["rows"][0]["total"] == 42

    def test_node_preview_unavailable_is_404(self, server):
        client = self._client(server)
        run = client.run("orders")
        with pytest.raises(APIError) as exc_info:
            run.node_preview("never-ran")
        assert exc_info.value.status == 404


class TestObservability:
    def _client(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES
        return _static_client(server)

    def test_dlq_resolves_pipeline_and_returns_entries(self, server):
        FakeBrokoli.dlq_entries["uuid-1"] = [
            {"id": "dlq-1", "node_id": "sink", "error": "constraint violation"}
        ]
        client = self._client(server)
        entries = client.dlq("orders")
        assert entries[0]["error"] == "constraint violation"

    def test_dlq_empty_by_default(self, server):
        client = self._client(server)
        assert client.dlq("orders") == []

    def test_dlq_query_params(self, server):
        client = self._client(server)
        client.dlq("orders", include_resolved=True, limit=10)
        assert FakeBrokoli.last_dlq_query == {"limit": "10", "include_resolved": "true"}


class TestDeploy:
    def _pipeline(self, name="Orders", pipeline_id="orders"):
        from brokoli import Pipeline, source_file

        with Pipeline(name, pipeline_id=pipeline_id) as p:
            source_file("Src", path="/tmp/x.csv", format="csv")
        return p

    def test_creates_when_absent(self, server, monkeypatch):
        self._quiet_preflight(monkeypatch)
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)
        result = client.deploy(self._pipeline(), validate=False)
        method, payload = FakeBrokoli.deployed[0]
        assert method == "POST"
        assert result["id"].startswith("created-")
        assert payload["pipeline_id"] == "orders"

    def test_updates_pipeline_id_match_before_name(self, server, monkeypatch):
        self._quiet_preflight(monkeypatch)
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = [
            {"id": "by-name", "pipeline_id": "other", "name": "Orders"},
            {"id": "by-pid", "pipeline_id": "orders", "name": "Old Name"},
        ]
        client = _static_client(server)
        client.deploy(self._pipeline(), validate=False)
        method, payload = FakeBrokoli.deployed[0]
        assert method == "PUT"
        assert payload["id"] == "by-pid"

    def test_ambiguous_target_fails_closed(self, server, monkeypatch):
        self._quiet_preflight(monkeypatch)
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = [
            {"id": "a", "pipeline_id": "orders", "name": "One"},
            {"id": "b", "pipeline_id": "orders", "name": "Two"},
        ]
        client = _static_client(server)
        with pytest.raises(APIError, match="ambiguous"):
            client.deploy(self._pipeline(), validate=False)
        assert FakeBrokoli.deployed == []

    def test_first_call_deploy_on_credentialed_client_logs_in_first(self, server):
        # Deliberately does NOT monkeypatch preflight_server_compatibility:
        # it and validate_pipeline read _auth_header() directly rather than
        # going through _request, so a credentialed client whose very first
        # call is deploy() must still be logged in before those two calls
        # fire — otherwise they hit /api/capabilities unauthenticated and
        # fail with a "verify your token" error despite valid credentials.
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = Client(server, username="e2e", password="right")
        result = client.deploy(self._pipeline(), validate=False)
        assert FakeBrokoli.login_calls == 1
        assert result["id"].startswith("created-")

    @staticmethod
    def _quiet_preflight(monkeypatch):
        import brokoli.compatibility as compatibility

        monkeypatch.setattr(compatibility, "preflight_server_compatibility", lambda *a, **k: None)
