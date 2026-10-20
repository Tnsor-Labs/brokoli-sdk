"""``brokoli mcp``: a Brokoli server's MCP endpoint, on stdio.

Brokoli serves the Model Context Protocol at ``POST /api/mcp`` (Streamable
HTTP, stateless). Agent clients that only start local processes speak MCP
over stdio instead; this bridges the two. Each JSON-RPC message read from
standard input is posted to the server with the caller's token, and the
server's answer is written to standard output. The tools, and every check
on what the caller may do, are the server's: the bridge adds nothing and
decides nothing.

Credentials are those of every other command: ``BROKOLI_TOKEN`` or
``--api-key``, a project environment's ``token_env``, else the token
``brokoli auth`` stored for the server.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any, Callable, Optional, TextIO

# post(body) -> (status, content type, body)
Poster = Callable[[bytes], "tuple[int, str, bytes]"]

TIMEOUT = 120.0


def http_poster(server: str, auth: str, workspace: str = "", timeout: float = TIMEOUT) -> Poster:
    """A poster for ``server``'s MCP endpoint."""
    url = server.rstrip("/") + "/api/mcp"

    def post(body: bytes) -> "tuple[int, str, bytes]":
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        if auth:
            req.add_header("Authorization", auth)
        if workspace:
            req.add_header("X-Workspace-ID", workspace)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- the configured server
                return resp.status, resp.headers.get("Content-Type", ""), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers.get("Content-Type", "") if exc.headers else "", exc.read()

    return post


def _messages(content_type: str, body: bytes) -> list[str]:
    """The JSON-RPC messages in an answer: a JSON body, or the data lines
    of an event stream."""
    text = body.decode("utf-8", "replace").strip()
    if not text:
        return []
    if "text/event-stream" not in content_type:
        return [text]
    out = []
    for event in text.split("\n\n"):
        data = "\n".join(
            line[5:].lstrip() for line in event.splitlines() if line.startswith("data:")
        )
        if data:
            out.append(data)
    return out


def _error(msg_id: Any, code: int, message: str) -> str:
    return json.dumps({"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}})


def _why(status: int, body: bytes, server: str) -> str:
    try:
        detail = json.loads(body or b"{}").get("error", "")
    except ValueError:
        detail = ""
    if status == 401:
        return (
            f"not signed in to {server}: run 'brokoli auth --server {server}', or set BROKOLI_TOKEN"
        )
    if status == 403:
        return f"not permitted: {detail or 'forbidden'}"
    return f"{server} answered {status}" + (f": {detail}" if detail else "")


def bridge(
    post: Poster,
    server: str,
    stdin: Optional[TextIO] = None,
    stdout: Optional[TextIO] = None,
) -> None:
    """Relay newline-delimited JSON-RPC messages from stdin to the server
    and its answers to stdout, until stdin closes."""
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout

    def write(line: str) -> None:
        stdout.write(line + "\n")
        stdout.flush()

    for raw in stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            write(_error(None, -32700, "not JSON"))
            continue
        msg_id = message.get("id") if isinstance(message, dict) else None
        try:
            status, content_type, body = post(line.encode("utf-8"))
        except OSError as exc:
            if msg_id is not None:
                write(_error(msg_id, -32000, f"could not reach {server}: {exc}"))
            continue
        if status in (200, 202, 204) or 200 <= status < 300:
            for out in _messages(content_type, body):
                write(out)
            continue
        if msg_id is not None:
            write(_error(msg_id, -32000, _why(status, body, server)))
