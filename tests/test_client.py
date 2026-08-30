"""brokoli-sdk#57 items 1-6: the programmatic run-ops client.

These tests run against a real in-process HTTP server rather than
monkeypatched internals: the behaviors under test — lazy login, exactly-
one 401 re-negotiation, cursor pagination, response-shape absorption,
concurrent firing — live in the transport layer, and a fake at the
urllib seam would test the fake.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from brokoli.client import (
    APIError,
    AuthError,
    Client,
    RunFailed,
)


class FakeBrokoli(BaseHTTPRequestHandler):
    """A scriptable stand-in for the server's run-ops API surface.

    Class-level state is reset per test by the fixture. Handlers mirror
    the REAL response-shape quirks (run_id vs id, bare list vs cursor
    page) because absorbing those is exactly what the client promises.
    """

    tokens: set[str] = set()
    require_auth = True
    login_calls = 0
    expire_after_logins = 0  # tokens minted before this many logins are dead
    pipelines_pages: list = []
    pipelines_flat: list = []
    use_cursor_shape = True
    runs: dict = {}
    run_statuses: dict = {}
    run_visibility_404s = 0
    trigger_shape = "run_id"  # or "id" or "nested"
    triggered: list = []
    deployed: list = []
    logs_shape = "list"  # or "wrapped"
    log_entries: list = []

    def log_message(self, *args):  # noqa: D102 - silence test output
        pass

    def _json(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        if not type(self).require_auth:
            return True
        header = self.headers.get("Authorization", "")
        return header.removeprefix("Bearer ") in type(self).tokens

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else {}

    def do_POST(self):
        cls = type(self)
        if self.path == "/api/auth/login":
            body = self._read_body()
            cls.login_calls += 1
            if body.get("password") != "right":
                return self._json(401, {"error": "bad credentials"})
            token = f"tok-{cls.login_calls}"
            cls.tokens.add(token)
            return self._json(200, {"token": token})
        if not self._authed():
            return self._json(401, {"error": "unauthenticated"})
        if self.path.endswith("/run") and self.path.startswith("/api/pipelines/"):
            pid = self.path.split("/")[3]
            body = self._read_body()
            run_id = f"run-{len(cls.triggered) + 1}"
            cls.triggered.append({"pipeline": pid, "body": body, "run_id": run_id})
            cls.run_statuses.setdefault(run_id, "success")
            if cls.trigger_shape == "id":
                return self._json(201, {"id": run_id})
            if cls.trigger_shape == "nested":
                return self._json(201, {"run": {"id": run_id}})
            return self._json(201, {"run_id": run_id})
        if self.path.endswith("/cancel"):
            run_id = self.path.split("/")[3]
            cls.run_statuses[run_id] = "cancelled"
            return self._json(200, {"status": "cancelled"})
        if self.path == "/api/pipelines":
            payload = self._read_body()
            payload["id"] = f"created-{len(cls.deployed) + 1}"
            cls.deployed.append(("POST", payload))
            return self._json(201, payload)
        return self._json(404, {"error": "nope"})

    def do_PUT(self):
        cls = type(self)
        if not self._authed():
            return self._json(401, {"error": "unauthenticated"})
        if self.path.startswith("/api/pipelines/"):
            payload = self._read_body()
            cls.deployed.append(("PUT", payload))
            return self._json(200, payload)
        return self._json(404, {"error": "nope"})

    def do_GET(self):
        cls = type(self)
        if not self._authed():
            return self._json(401, {"error": "unauthenticated"})
        if self.path.startswith("/api/pipelines"):
            if not cls.use_cursor_shape:
                return self._json(200, cls.pipelines_flat)
            # Cursor shape: serve successive pages per `after` param.
            after = ""
            if "after=" in self.path:
                after = self.path.split("after=")[1].split("&")[0]
            index = int(after) if after else 0
            page = cls.pipelines_pages[index] if index < len(cls.pipelines_pages) else []
            has_next = index + 1 < len(cls.pipelines_pages)
            return self._json(
                200,
                {"items": page, "has_next": has_next, "cursor": str(index + 1)},
            )
        if self.path.startswith("/api/runs/") and self.path.endswith("/logs"):
            entries = cls.log_entries
            if cls.logs_shape == "wrapped":
                return self._json(200, {"logs": entries})
            return self._json(200, entries)
        if self.path.startswith("/api/runs/"):
            run_id = self.path.split("/")[3]
            if cls.run_visibility_404s:
                cls.run_visibility_404s -= 1
                return self._json(404, {"error": "run not found"})
            status = cls.run_statuses.get(run_id)
            if status is None:
                return self._json(404, {"error": "run not found"})
            detail = {"id": run_id, "status": status}
            detail.update(cls.runs.get(run_id, {}))
            return self._json(200, detail)
        return self._json(404, {"error": "nope"})


@pytest.fixture()
def server():
    # Reset scriptable state so tests can't bleed into each other.
    FakeBrokoli.tokens = set()
    FakeBrokoli.require_auth = True
    FakeBrokoli.login_calls = 0
    FakeBrokoli.pipelines_pages = []
    FakeBrokoli.pipelines_flat = []
    FakeBrokoli.use_cursor_shape = True
    FakeBrokoli.runs = {}
    FakeBrokoli.run_statuses = {}
    FakeBrokoli.run_visibility_404s = 0
    FakeBrokoli.trigger_shape = "run_id"
    FakeBrokoli.triggered = []
    FakeBrokoli.deployed = []
    FakeBrokoli.logs_shape = "list"
    FakeBrokoli.log_entries = []

    httpd = HTTPServer(("127.0.0.1", 0), FakeBrokoli)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


PIPES = [
    {"id": "uuid-1", "pipeline_id": "orders", "name": "Orders"},
    {"id": "uuid-2", "pipeline_id": "events", "name": "Events"},
]


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

    def test_wait_retries_run_visibility_race(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_visibility_404s = 3
        detail = run.wait(timeout=5, poll_interval=0.05)
        assert detail["status"] == "success"

    def test_wait_does_not_hide_real_404(self, server):
        client = self._client(server)
        run = client.run("orders")
        FakeBrokoli.run_statuses.pop(run.id)
        FakeBrokoli.run_visibility_404s = 100
        with pytest.raises(APIError, match="run not found"):
            run.wait(timeout=0.2, poll_interval=0.05, visibility_grace=0.01)

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

    @staticmethod
    def _quiet_preflight(monkeypatch):
        import brokoli.compatibility as compatibility

        monkeypatch.setattr(compatibility, "preflight_server_compatibility", lambda *a, **k: None)
