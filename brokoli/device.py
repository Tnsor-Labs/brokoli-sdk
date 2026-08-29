"""Device authorization (#75): ``brokoli auth`` and its library face.

The terminal asks the server for a code pair, the person confirms the
short code in a browser that is already logged in, and the terminal
receives an ordinary session token -- no password ever touches the shell,
its history, or a CI log. The token lands in a per-user credentials file
keyed by server URL, where the CLI's auth resolution picks it up as the
LAST fallback: an explicit ``BROKOLI_TOKEN``, ``--api-key``, or a project
environment's ``token_env`` always wins over stored credentials.

Servers without the grant (any OSS server, or an enterprise server from
before the feature) are refused up front with a message naming the
alternatives -- the same capability-honesty rule as deploy preflight.
"""

from __future__ import annotations

import json
import os
import stat
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any, Callable

from brokoli.exceptions import BrokoliError

REQUEST_TIMEOUT = 10


class DeviceAuthError(BrokoliError):
    """The device authorization flow could not complete."""


def _post_json(server: str, path: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        server.rstrip("/") + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read() or b"{}")
        except Exception:
            payload = {}
        return exc.code, payload


def request_device_authorization(server: str) -> dict[str, Any]:
    """Ask the server for a device/user code pair, refusing clearly when
    the server does not speak the grant."""
    status, payload = _post_json(server, "/api/auth/oauth/device", {})
    if status in (404, 405):
        raise DeviceAuthError(
            f"{server} does not support device authorization; "
            "use username/password or an API key instead"
        )
    if status != 200 or "device_code" not in payload:
        raise DeviceAuthError(f"device authorization request failed (HTTP {status}): {payload}")
    return payload


def poll_for_token(
    server: str,
    device_code: str,
    interval: int,
    expires_in: int,
    on_wait: "Callable[[], None] | None" = None,
) -> tuple[str, str]:
    """Poll until approved; return (token, username).

    Honors the protocol's pacing answers: ``authorization_pending`` keeps
    polling, ``slow_down`` widens the interval, ``access_denied`` and
    ``expired`` are terminal.
    """
    deadline = time.monotonic() + max(1, expires_in)
    wait = max(1, interval)
    while True:
        if time.monotonic() > deadline:
            raise DeviceAuthError("the code expired before it was approved; run brokoli auth again")
        time.sleep(wait)
        status, payload = _post_json(
            server, "/api/auth/oauth/device/poll", {"device_code": device_code}
        )
        if status != 200:
            raise DeviceAuthError(f"polling failed (HTTP {status}): {payload}")
        answer = str(payload.get("status", ""))
        if answer == "authorization_pending":
            if on_wait is not None:
                on_wait()
            continue
        if answer == "slow_down":
            wait += 2
            continue
        if answer == "access_denied":
            raise DeviceAuthError("the request was denied in the browser")
        if answer == "expired":
            raise DeviceAuthError("the code expired before it was approved; run brokoli auth again")
        if answer == "approved":
            token = str(payload.get("token", ""))
            if not token:
                raise DeviceAuthError(f"approval carried no token: {payload}")
            return token, str(payload.get("username", ""))
        raise DeviceAuthError(f"unexpected poll answer {answer!r}")


# ── stored credentials ─────────────────────────────────────────────────


def _credentials_path() -> Path:
    override = os.getenv("BROKOLI_CREDENTIALS")
    if override:
        return Path(override)
    base = os.getenv("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
    return Path(base) / "brokoli" / "credentials.json"


def _load_all() -> dict[str, str]:
    path = _credentials_path()
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    servers = data.get("servers")
    return servers if isinstance(servers, dict) else {}


def load_token(server: str) -> str:
    """The stored token for ``server``, or empty."""
    return str(_load_all().get(server.rstrip("/"), "") or "")


def store_token(server: str, token: str) -> Path:
    """Persist ``token`` for ``server``; the file is user-only (0600)."""
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    servers = _load_all()
    servers[server.rstrip("/")] = token
    path.write_text(json.dumps({"servers": servers}, indent=2) + "\n")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def forget_token(server: str) -> None:
    """Drop the stored token for ``server``, if any."""
    path = _credentials_path()
    servers = _load_all()
    if servers.pop(server.rstrip("/"), None) is not None:
        path.write_text(json.dumps({"servers": servers}, indent=2) + "\n")


# ── the whole flow ─────────────────────────────────────────────────────


def _echo_flush(line: str) -> None:
    # The code and link print, then the process waits minutes for the
    # browser -- without an explicit flush, piped or redirected stdout
    # shows NOTHING until exit, which reads as a hang. Found live.
    print(line, flush=True)


def device_login(
    server: str,
    *,
    open_browser: bool = True,
    echo: Callable[[str], None] = _echo_flush,
) -> str:
    """Run the grant end to end; store and return the token."""
    server = server.rstrip("/")
    grant = request_device_authorization(server)
    uri = str(grant.get("verification_uri", ""))
    echo(f"Confirm this code in your browser: {grant.get('user_code')}")
    echo(f"  {uri}")
    if open_browser:
        try:  # best-effort; headless terminals just use the printed link
            webbrowser.open(uri)
        except Exception:
            pass
    echo("Waiting for approval...")
    token, username = poll_for_token(
        server,
        str(grant.get("device_code", "")),
        int(grant.get("interval", 3)),
        int(grant.get("expires_in", 600)),
    )
    path = store_token(server, token)
    who = f" as {username}" if username else ""
    echo(f"Authorized{who}. Token stored in {path}.")
    return token
