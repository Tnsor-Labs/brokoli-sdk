"""Programmatic client for the Brokoli API (brokoli-sdk#57, items 1-6).

Everything here existed operationally in the CLI (``brokoli run``,
``status``, ``logs``, ``cancel``, ``deploy``) but with the HTTP layer
inlined per command — nothing a test or script could import. This module
is that layer as a library: authenticate once, fire runs, wait on them,
read their logs, and deploy pipelines, all in-process.

Design notes, in the order they bit people:

- **Response shapes are absorbed here, once.** The pipeline list endpoint
  answers with a bare list (legacy) or a cursor page (``items``/
  ``has_next``/``cursor``); the run-trigger endpoint has answered with
  ``run_id``, ``id``, and ``{"run": {"id": ...}}`` across versions; the
  logs endpoint with a bare list or a wrapped one. Every verification
  script that predated this module re-derived those variants by hand, and
  one of them shipped a bug doing it. Callers of this module see exactly
  one shape each.

- **Auth is either a static token or a login session.** A static
  ``api_key`` is used as-is and a 401 is final. With ``username``/
  ``password`` the client logs in lazily and re-logs-in exactly once per
  request on 401 — long-lived processes (a soak harness, a scheduler)
  survive token expiry without hand-rolled retry loops.

- **Thread-safe by construction.** State is one token string behind a
  lock; each request builds its own ``urllib`` request. Burst tests fire
  from many threads against one client.

- **Stdlib only**, like the rest of the SDK's runtime surface.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from brokoli.exceptions import BrokoliError, ValidationError

#: Run statuses the server treats as terminal. ``Run.wait`` returns when
#: the run reaches any of these.
TERMINAL_RUN_STATUSES = frozenset({"success", "failed", "cancelled", "blocked"})

_DEFAULT_TIMEOUT = 10.0


class APIError(BrokoliError):
    """An HTTP-level failure talking to the server.

    ``status`` is the HTTP status code (0 for transport errors — refused
    connection, timeout), ``body`` the raw response body when there was
    one, and ``url`` the request that failed.
    """

    def __init__(self, message: str, *, status: int = 0, url: str = "", body: str = "") -> None:
        self.status = status
        self.url = url
        self.body = body
        super().__init__(message)


class AuthError(APIError):
    """Authentication failed and cannot be recovered by retrying.

    Raised when a login attempt is rejected, or when a request gets a 401
    and the client has only a static token (nothing to re-negotiate).
    """


class RunFailed(BrokoliError):
    """Raised by ``Run.wait(raise_on_failure=True)`` for a non-success end.

    ``detail`` is the run's final API object — status, error, node_runs —
    so an assertion failure message can say what actually happened.
    """

    def __init__(self, detail: dict[str, Any]) -> None:
        self.detail = detail
        status = detail.get("status", "?")
        error = detail.get("error") or ""
        run_id = detail.get("id", "?")
        suffix = f": {error}" if error else ""
        super().__init__(f"run {run_id} finished {status}{suffix}")


def _absorb_list(payload: Any, *keys: str) -> list[Any] | None:
    """Return the list inside ``payload`` whichever documented shape it uses."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return None


def _extract_run_id(payload: Any) -> str:
    """Pull the run id out of a trigger response, across its known shapes."""
    if isinstance(payload, dict):
        for key in ("run_id", "id"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        nested = payload.get("run")
        if isinstance(nested, dict):
            value = nested.get("id")
            if isinstance(value, str) and value:
                return value
    raise APIError(f"run trigger response carried no run id: {payload!r}")


class Client:
    """A connection to one Brokoli server.

    Exactly one of the auth styles applies:

    - ``api_key``: sent as a Bearer token, never refreshed.
    - ``username`` + ``password``: the client logs in lazily on first use
      and re-logs-in once per request when a token expires mid-flight.
    - neither: unauthenticated (servers in open mode).
    """

    def __init__(
        self,
        server: str,
        *,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        if not server:
            raise ValueError("server is required, e.g. Client('http://localhost:8080')")
        if api_key and (username or password):
            raise ValueError("pass api_key OR username/password, not both")
        if (username is None) != (password is None):
            raise ValueError("username and password go together")
        self.server = server.rstrip("/")
        self.timeout = timeout
        self._username = username
        self._password = password
        self._static = bool(api_key)
        self._lock = threading.Lock()
        self._token = api_key or ""

    @classmethod
    def from_env(cls, server: str | None = None, **kwargs: Any) -> "Client":
        """Build a client from ``BROKOLI_SERVER`` / ``BROKOLI_TOKEN``.

        Explicit arguments win over the environment; the same variables
        the CLI honors, so one shell setup serves both.
        """
        server = server or os.getenv("BROKOLI_SERVER", "")
        token = os.getenv("BROKOLI_TOKEN", "")
        if token and "api_key" not in kwargs and "username" not in kwargs:
            kwargs["api_key"] = token
        return cls(server, **kwargs)

    # ------------------------------------------------------------------ auth

    def login(self) -> None:
        """Authenticate with username/password and store the session token.

        Called automatically; call it eagerly only to fail fast on bad
        credentials at startup.
        """
        if not self._username:
            raise AuthError("login requires the client to have username/password")
        try:
            payload = self._raw_request(
                "POST",
                "/api/auth/login",
                body={"username": self._username, "password": self._password},
                auth=False,
            )
        except APIError as exc:
            if exc.status in (401, 403):
                raise AuthError(
                    f"login rejected for {self._username!r}",
                    status=exc.status,
                    url=exc.url,
                    body=exc.body,
                ) from exc
            raise
        token = ""
        if isinstance(payload, dict):
            token = payload.get("token") or payload.get("access_token") or ""
        if not token:
            raise AuthError(f"login response carried no token: {payload!r}")
        with self._lock:
            self._token = token

    def _auth_header(self) -> str:
        with self._lock:
            token = self._token
        return f"Bearer {token}" if token else ""

    # -------------------------------------------------------------- requests

    def _raw_request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, str] | None = None,
        auth: bool = True,
    ) -> Any:
        url = self.server + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        headers: dict[str, str] = {}
        if auth:
            header = self._auth_header()
            if header:
                headers["Authorization"] = header
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode(errors="replace")
            raise APIError(
                f"{method} {url} -> HTTP {exc.code}: {error_body}",
                status=exc.code,
                url=url,
                body=error_body,
            ) from exc
        except urllib.error.URLError as exc:
            raise APIError(f"{method} {url} failed: {exc.reason}", url=url) from exc
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise APIError(
                f"{method} {url} returned non-JSON body",
                status=200,
                url=url,
                body=raw[:500].decode(errors="replace"),
            ) from exc

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        # Lazy first login for credentialed clients.
        if self._username and not self._auth_header():
            self.login()
        try:
            return self._raw_request(method, path, body=body, query=query)
        except APIError as exc:
            if exc.status != 401:
                raise
            if not self._username:
                raise AuthError(
                    f"{method} {path} -> 401 with a static token; nothing to re-negotiate",
                    status=401,
                    url=exc.url,
                    body=exc.body,
                ) from exc
            # A credentialed session can expire mid-flight; renegotiate
            # exactly once — a second 401 means the credentials are bad.
            self.login()
            return self._raw_request(method, path, body=body, query=query)

    # ------------------------------------------------------------- pipelines

    def pipelines(self) -> list[dict[str, Any]]:
        """All pipelines visible to this token, across both list shapes.

        Handles the cursor pagination newer servers use and the bare list
        older ones answer with; either way the caller gets one flat list.
        """
        results: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        while True:
            query = {"limit": "100"}
            if after is not None:
                query["after"] = after
            payload = self._request("GET", "/api/pipelines", query=query)
            if isinstance(payload, list):
                results.extend(p for p in payload if isinstance(p, dict))
                return results
            items = _absorb_list(payload, "items", "pipelines")
            if items is None:
                raise APIError(f"malformed pipeline list response: {payload!r}")
            results.extend(p for p in items if isinstance(p, dict))
            has_next = bool(isinstance(payload, dict) and payload.get("has_next"))
            cursor = payload.get("cursor") if isinstance(payload, dict) else None
            if not has_next or not isinstance(cursor, str) or cursor in seen_cursors:
                return results
            seen_cursors.add(cursor)
            after = cursor

    def pipeline(self, identifier: str) -> dict[str, Any]:
        """Resolve a pipeline by internal id, logical pipeline_id, or name.

        The same precedence the CLI uses: an exact internal-id match is
        unambiguous; otherwise pipeline_id beats name; multiple matches
        are an error rather than a guess.
        """
        remote = self.pipelines()
        for item in remote:
            if item.get("id") == identifier:
                return item
        matches = [i for i in remote if i.get("pipeline_id") == identifier] or [
            i for i in remote if i.get("name") == identifier
        ]
        if not matches:
            raise APIError(f"no pipeline matching {identifier!r} on {self.server}", status=404)
        if len({i.get("id") for i in matches}) > 1:
            raise APIError(
                f"{identifier!r} matches multiple pipelines; use the internal id"
            )
        return matches[0]

    def deploy(
        self,
        pipeline: Any,
        *,
        validate: bool = True,
        allow_legacy_server: bool = False,
    ) -> dict[str, Any]:
        """Create or update ``pipeline`` on the server, and return its
        remote object.

        The same fail-closed matching discipline as ``brokoli deploy``
        (pipeline_id first, then name, never a guess across duplicates),
        without the CLI's printing. ``validate=False`` skips client-side
        validation, exactly like ``--skip-validation``.
        """
        from brokoli.compatibility import preflight_server_compatibility
        from brokoli.validation import validate_pipeline

        preflight_server_compatibility(
            [pipeline],
            self.server,
            self._auth_header(),
            allow_legacy_server=allow_legacy_server,
        )
        if validate:
            result = validate_pipeline(
                pipeline, server_url=self.server, auth_header=self._auth_header()
            )
            if not result.valid:
                raise ValidationError([str(e) for e in result.errors])

        payload = pipeline.to_json()
        remote = self.pipelines()
        logical_id = payload.get("pipeline_id") or ""
        if logical_id:
            matches = [r for r in remote if r.get("pipeline_id") == logical_id]
        else:
            matches = [r for r in remote if r.get("name") == payload.get("name")]
        if len({m.get("id") for m in matches}) > 1:
            raise APIError(
                f"deploy target for {payload.get('name')!r} is ambiguous on the server"
            )

        if matches:
            existing_id = matches[0]["id"]
            payload["id"] = existing_id
            response = self._request("PUT", f"/api/pipelines/{existing_id}", body=payload)
        else:
            response = self._request("POST", "/api/pipelines", body=payload)
        if not isinstance(response, dict):
            raise APIError(f"malformed deploy response: {response!r}")
        return response

    # ------------------------------------------------------------------ runs

    def run(self, pipeline: Any, params: dict[str, str] | None = None) -> "Run":
        """Trigger a run and return its handle.

        ``pipeline`` is an internal id, logical pipeline_id, name, or an
        authored ``Pipeline`` object (its pipeline_id/name resolves).
        """
        identifier = pipeline
        if not isinstance(pipeline, str):
            identifier = getattr(pipeline, "pipeline_id", "") or getattr(pipeline, "name", "")
        remote = self.pipeline(str(identifier))
        body = {"params": params} if params else {}
        response = self._request("POST", f"/api/pipelines/{remote['id']}/run", body=body)
        return Run(self, _extract_run_id(response))

    def run_handle(self, run_id: str) -> "Run":
        """Wrap an existing run id (from a log, another process, the UI)."""
        return Run(self, run_id)


class Run:
    """Handle to one run: poll it, wait on it, cancel it, read its logs.

    Deliberately stateless beyond the id — every accessor asks the server,
    so two handles to the same run can never disagree with each other,
    only with time.
    """

    def __init__(self, client: Client, run_id: str) -> None:
        self.client = client
        self.id = run_id

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"Run({self.id!r})"

    def detail(self) -> dict[str, Any]:
        """The run's full API object: status, error, node_runs, timings."""
        payload = self.client._request("GET", f"/api/runs/{self.id}")
        if not isinstance(payload, dict):
            raise APIError(f"malformed run response for {self.id}: {payload!r}")
        return payload

    def status(self) -> str:
        return str(self.detail().get("status", ""))

    def node_runs(self) -> list[dict[str, Any]]:
        return [nr for nr in (self.detail().get("node_runs") or []) if isinstance(nr, dict)]

    def wait(
        self,
        timeout: float = 600.0,
        poll_interval: float = 2.0,
        *,
        raise_on_failure: bool = False,
    ) -> dict[str, Any]:
        """Poll until the run is terminal; return its final API object.

        Raises ``TimeoutError`` (stdlib) if the run is still live when the
        deadline passes, and ``RunFailed`` for a non-success terminal
        status when ``raise_on_failure`` is set — with the full final
        object attached either way there is something to assert on.
        """
        deadline = time.monotonic() + timeout
        last_status = ""
        while True:
            detail = self.detail()
            last_status = str(detail.get("status", ""))
            if last_status in TERMINAL_RUN_STATUSES:
                if raise_on_failure and last_status != "success":
                    raise RunFailed(detail)
                return detail
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"run {self.id} still {last_status!r} after {timeout:.0f}s"
                )
            time.sleep(poll_interval)

    def cancel(self) -> dict[str, Any]:
        """Request cancellation. Terminal statuses arrive asynchronously —
        follow with ``wait()`` to observe the run actually settle."""
        payload = self.client._request("POST", f"/api/runs/{self.id}/cancel", body={})
        return payload if isinstance(payload, dict) else {}

    def logs(self, *, level: str | None = None, node: str | None = None) -> list[dict[str, Any]]:
        """The run's log entries, absorbing both response shapes."""
        query: dict[str, str] = {}
        if level:
            query["level"] = level
        if node:
            query["node_id"] = node
        payload = self.client._request(
            "GET", f"/api/runs/{self.id}/logs", query=query or None
        )
        entries = _absorb_list(payload, "logs", "items")
        if entries is None:
            raise APIError(f"malformed logs response for {self.id}: {payload!r}")
        return [e for e in entries if isinstance(e, dict)]
