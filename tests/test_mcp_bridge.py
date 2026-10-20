"""brokoli mcp: the server's MCP endpoint relayed on stdio."""

from __future__ import annotations

import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from brokoli import mcp_bridge


def run(
    lines: list[str], answers: dict[str, tuple[int, str, bytes]]
) -> tuple[list[dict[str, Any]], list[bytes]]:
    """Bridge lines through a poster answering by method; return what was written and posted."""
    posted: list[bytes] = []

    def post(body: bytes) -> tuple[int, str, bytes]:
        posted.append(body)
        method = json.loads(body).get("method", "")
        if method == "boom":
            raise OSError("connection refused")
        return answers.get(method, (202, "", b""))

    out = io.StringIO()
    mcp_bridge.bridge(
        post, "https://brokoli.example.com", io.StringIO("\n".join(lines) + "\n"), out
    )
    return [json.loads(line) for line in out.getvalue().splitlines()], posted


def test_messages_are_relayed_both_ways() -> None:
    answer = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}).encode()
    stream = b'event: message\ndata: {"jsonrpc":"2.0","id":2,"result":{}}\n\n'
    out, posted = run(
        [
            '{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
            "",
            '{"jsonrpc":"2.0","method":"notifications/initialized"}',
            '{"jsonrpc":"2.0","id":2,"method":"ping"}',
        ],
        {
            "tools/list": (200, "application/json", answer),
            "ping": (200, "text/event-stream", stream),
        },
    )
    assert out == [
        {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}},
        {"jsonrpc": "2.0", "id": 2, "result": {}},
    ]
    assert (
        len(posted) == 3
    )  # the blank line is not a message; the notification is posted, answered with nothing


def test_failures_answer_the_request_that_failed() -> None:
    out, _ = run(
        [
            "not json",
            '{"jsonrpc":"2.0","id":3,"method":"tools/list"}',
            '{"jsonrpc":"2.0","id":4,"method":"denied"}',
            '{"jsonrpc":"2.0","id":5,"method":"boom"}',
            '{"jsonrpc":"2.0","method":"denied"}',
        ],
        {
            "tools/list": (401, "application/json", b'{"error":"invalid token"}'),
            "denied": (403, "application/json", b'{"error":"insufficient permissions"}'),
        },
    )
    assert out[0]["error"]["code"] == -32700 and out[0]["id"] is None
    assert (
        out[1]["id"] == 3
        and "brokoli auth --server https://brokoli.example.com" in out[1]["error"]["message"]
    )
    assert (
        out[2]["id"] == 4
        and out[2]["error"]["message"] == "not permitted: insufficient permissions"
    )
    assert out[3]["id"] == 5 and "could not reach" in out[3]["error"]["message"]
    assert len(out) == 4  # a failed notification has no one to answer


class _Server(BaseHTTPRequestHandler):
    seen: dict[str, str] = {}

    def do_POST(self) -> None:  # noqa: N802
        _Server.seen = {k.lower(): v for k, v in self.headers.items()}
        body = self.rfile.read(int(self.headers["Content-Length"]))
        msg = json.loads(body)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": {"path": self.path}}).encode()
        )

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def server() -> Any:
    httpd = HTTPServer(("127.0.0.1", 0), _Server)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_the_http_poster_sends_the_credential_and_workspace(server: str) -> None:
    post = mcp_bridge.http_poster(server + "/", "Bearer bka_x", "ws-1")
    status, content_type, body = post(b'{"jsonrpc":"2.0","id":7,"method":"ping"}')
    assert status == 200 and json.loads(body)["result"]["path"] == "/api/mcp"
    assert (
        _Server.seen["authorization"] == "Bearer bka_x" and _Server.seen["x-workspace-id"] == "ws-1"
    )
    assert "text/event-stream" in _Server.seen["accept"]
