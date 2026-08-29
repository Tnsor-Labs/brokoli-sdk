"""Shared fixtures for the run-ops client tests (test_client.py,
test_async_client.py) -- a real in-process HTTP server rather than
monkeypatched internals, since the behaviors under test (lazy login,
exactly-one 401 re-negotiation, cursor pagination, response-shape
absorption) live in the transport layer, and a fake at the urllib seam
would test the fake.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


class FakeBrokoli(BaseHTTPRequestHandler):
    """A scriptable stand-in for the server's run-ops API surface.

    Class-level state is reset per test by the `server` fixture. Handlers
    mirror the REAL response-shape quirks (run_id vs id, bare list vs
    cursor page) because absorbing those is exactly what the client
    promises.
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
    # sdk#72: run_id -> how many detail GETs still answer 404 before the
    # row "becomes visible" -- the async-dispatch race, made deterministic.
    run_invisible_polls: dict = {}
    trigger_shape = "run_id"  # or "id" or "nested"
    triggered: list = []
    deployed: list = []
    logs_shape = "list"  # or "wrapped"
    log_entries: list = []
    dlq_entries: dict = {}  # pipeline id -> list of entries
    last_dlq_query: dict = {}
    previews: dict = {}  # "run_id/node_id" -> {"columns": ..., "rows": ...}
    deleted_pipeline_ids: list = []

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
            # A created pipeline is discoverable by subsequent list/lookup
            # calls, same as a real server -- needed by anything that
            # deploys then immediately runs or deletes by the returned id.
            cls.pipelines_flat.append(payload)
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

    def do_DELETE(self):
        cls = type(self)
        if not self._authed():
            return self._json(401, {"error": "unauthenticated"})
        if self.path.startswith("/api/pipelines/"):
            pipeline_id = self.path.split("/")[3]
            all_ids = {p["id"] for p in cls.pipelines_flat} | {
                p["id"] for page in cls.pipelines_pages for p in page
            }
            if pipeline_id not in all_ids:
                return self._json(404, {"error": "not found"})
            cls.deleted_pipeline_ids.append(pipeline_id)
            cls.pipelines_flat = [p for p in cls.pipelines_flat if p["id"] != pipeline_id]
            cls.pipelines_pages = [
                [p for p in page if p["id"] != pipeline_id] for page in cls.pipelines_pages
            ]
            return self._json(204, {})
        return self._json(404, {"error": "nope"})

    def do_GET(self):
        cls = type(self)
        if not self._authed():
            return self._json(401, {"error": "unauthenticated"})
        if self.path == "/api/capabilities":
            return self._json(200, {"supported_ir_versions": ["2.0"]})
        if self.path.startswith("/api/pipelines/") and "/dlq" in self.path:
            pipeline_id = self.path.split("/")[3]
            parsed = urllib.parse.urlsplit(self.path)
            cls.last_dlq_query = dict(urllib.parse.parse_qsl(parsed.query))
            return self._json(200, cls.dlq_entries.get(pipeline_id, []))
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
        if (
            self.path.startswith("/api/runs/")
            and "/nodes/" in self.path
            and self.path.endswith("/preview")
        ):
            parts = self.path.split("/")
            run_id, node_id = parts[3], parts[5]
            preview = cls.previews.get(f"{run_id}/{node_id}")
            if preview is None:
                return self._json(404, {"error": "no preview available"})
            return self._json(200, preview)
        if self.path.startswith("/api/runs/"):
            run_id = self.path.split("/")[3]
            if cls.run_invisible_polls.get(run_id, 0) > 0:
                cls.run_invisible_polls[run_id] -= 1
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
    FakeBrokoli.run_invisible_polls = {}
    FakeBrokoli.trigger_shape = "run_id"
    FakeBrokoli.triggered = []
    FakeBrokoli.deployed = []
    FakeBrokoli.logs_shape = "list"
    FakeBrokoli.log_entries = []
    FakeBrokoli.dlq_entries = {}
    FakeBrokoli.last_dlq_query = {}
    FakeBrokoli.previews = {}
    FakeBrokoli.deleted_pipeline_ids = []

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
