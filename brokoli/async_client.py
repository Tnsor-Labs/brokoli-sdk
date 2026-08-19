"""Async programmatic client for the Brokoli API (brokoli-sdk#57, item 8).

A parallel async counterpart to :mod:`brokoli.client`'s ``Client``/``Run``
-- not a background-thread wrapper around them. Every REST operation
delegates to a plain synchronous ``Client`` via ``asyncio.to_thread``:
there is exactly one HTTP implementation (urllib-based, already tested)
to keep correct, not two. What is genuinely async-native here is
``AsyncRun.watch()``/``wait()``: a real WebSocket subscription against
the server's SODP (State-Oriented Data Protocol) endpoint, not a polling
loop wearing an async face.

Design notes:

- **The SODP push path is a tripwire to refetch, not the source of
  truth.** ``runs.{run_id}`` state (status/pipeline_id/started_at/...)
  is a narrower rollup than the REST run object (no ``node_runs``, no
  ``id``), and the Go backend's own bridge documents this exact pattern:
  a state write signals "something changed, go refetch" rather than
  being rendered directly. Every value this module yields is a real
  ``detail()`` call; the subscription only decides *when* to make one.

- **A poll fallback runs alongside the subscription, not instead of
  it.** A run that finishes ``blocked`` (a cross-pipeline dependency
  check failing before the run ever starts) is written straight to the
  store and never emits a ``run.*`` event -- its SODP state key is never
  created, so a subscription alone would wait forever. Every other
  terminal status resolves near-instantly via the push notification;
  the fallback poll exists for this one gap, not as the common path.

- **``sodp-client`` is an optional dependency** (the ``watch`` extra:
  ``pip install 'brokoli[watch]'``). Without it, ``watch()``/``wait()``
  still work correctly -- they just poll at ``poll_interval`` the whole
  time, exactly like the sync ``Run.wait()``. Every other ``AsyncClient``
  method works with zero extra dependencies either way.

- **Auth reuses the client's own bearer token, over SODP's AUTH frame**,
  not cookies -- the browser UI authenticates the WebSocket upgrade
  itself via an httpOnly session cookie the browser sends automatically,
  which a non-browser SDK client has no equivalent of. The server
  accepts the same JWT either way; only the delivery mechanism differs.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Callable

from brokoli.client import (
    Client,
    DEFAULT_SERVER,
    Run,
    RunFailed,
    TERMINAL_RUN_STATUSES,
)

try:
    from sodp import SodpClient  # type: ignore[import-not-found]
except ImportError:
    SodpClient = None  # type: ignore[assignment,misc]

#: Default interval, in seconds, between fallback REST polls in
#: ``AsyncRun.watch()``/``wait()`` -- both when the ``watch`` extra isn't
#: installed at all, and as the safety net alongside a live subscription
#: (see the module docstring's "blocked runs" note).
_DEFAULT_POLL_INTERVAL = 20.0

#: How long to wait for the SODP WebSocket handshake to complete before
#: giving up on it for this call and falling back to polling instead.
_SODP_CONNECT_TIMEOUT = 5.0


def _ws_url(server: str) -> str:
    """Translate the client's http(s):// server URL into its ws(s):// SODP endpoint."""
    if server.startswith("https://"):
        return "wss://" + server[len("https://") :] + "/api/ws"
    if server.startswith("http://"):
        return "ws://" + server[len("http://") :] + "/api/ws"
    raise ValueError(f"server must start with http:// or https://, got {server!r}")


class AsyncClient:
    """Async counterpart to :class:`brokoli.client.Client`.

    Construction mirrors ``Client`` exactly (same auth styles, same
    ``from_env``); every method is the ``await``-able version of the same
    name.
    """

    def __init__(
        self,
        server: str = DEFAULT_SERVER,
        *,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._init_from_sync(
            Client(server, api_key=api_key, username=username, password=password, timeout=timeout)
        )

    def _init_from_sync(self, sync_client: Client) -> None:
        self._sync = sync_client
        self._sodp: Any = None
        self._sodp_lock = asyncio.Lock()

    @classmethod
    def from_env(cls, server: str | None = None, **kwargs: Any) -> "AsyncClient":
        """Build a client from ``BROKOLI_SERVER`` / ``BROKOLI_TOKEN`` -- see
        :meth:`brokoli.client.Client.from_env`."""
        self = cls.__new__(cls)
        self._init_from_sync(Client.from_env(server, **kwargs))
        return self

    @property
    def server(self) -> str:
        return self._sync.server

    # ------------------------------------------------------------- pipelines

    async def pipelines(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._sync.pipelines)

    async def pipeline(self, identifier: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._sync.pipeline, identifier)

    async def deploy(
        self,
        pipeline: Any,
        *,
        validate: bool = True,
        allow_legacy_server: bool = False,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._sync.deploy, pipeline, validate=validate, allow_legacy_server=allow_legacy_server
        )

    async def delete_pipeline(self, pipeline: Any) -> None:
        await asyncio.to_thread(self._sync.delete_pipeline, pipeline)

    # ------------------------------------------------------------------ runs

    async def run(self, pipeline: Any, params: dict[str, str] | None = None) -> "AsyncRun":
        sync_run = await asyncio.to_thread(self._sync.run, pipeline, params)
        return AsyncRun(self, sync_run.id)

    def run_handle(self, run_id: str) -> "AsyncRun":
        """Wrap an existing run id. No I/O, so this isn't a coroutine."""
        return AsyncRun(self, run_id)

    # ---------------------------------------------------------------- sodp

    async def _sodp_client(self) -> Any:
        """Lazily create and connect the shared SODP client for this
        ``AsyncClient``.

        One WebSocket per ``AsyncClient``, reused by every ``AsyncRun``
        it hands out -- mirroring the browser UI's own connection
        lifecycle (``ui/src/lib/sodp.ts``'s ``getSodpClient()`` singleton),
        not one connection per watched run.

        Returns ``None``, gracefully, if the ``watch`` extra isn't
        installed, or if connecting doesn't succeed within
        ``_SODP_CONNECT_TIMEOUT`` -- callers (``AsyncRun.watch()``) fall
        back to polling rather than treating either as an error.
        ``SodpClient(reconnect=True)`` retries forever in the background
        and never itself raises on a connection failure, so without this
        timeout a genuinely unreachable server would hang ``await
        client.ready`` (and therefore this whole call) indefinitely
        instead of degrading to the poll fallback that exists for exactly
        this situation.
        """
        async with self._sodp_lock:
            if self._sodp is not None:
                return self._sodp
            if SodpClient is None:
                return None
            self._sync._ensure_login()
            header = self._sync._auth_header()
            token = header[len("Bearer ") :] if header.startswith("Bearer ") else None
            client = SodpClient(_ws_url(self._sync.server), token=token, reconnect=True)
            try:
                await asyncio.wait_for(client.ready, timeout=_SODP_CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                client.close()
                return None
            self._sodp = client
            return self._sodp

    async def aclose(self) -> None:
        """Close the shared SODP connection, if one was ever opened."""
        async with self._sodp_lock:
            if self._sodp is not None:
                self._sodp.close()
                self._sodp = None

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.aclose()


class AsyncRun:
    """Async counterpart to :class:`brokoli.client.Run`.

    Deliberately stateless beyond the id, same as ``Run`` -- every
    accessor asks the server.
    """

    def __init__(self, client: AsyncClient, run_id: str) -> None:
        self.client = client
        self.id = run_id

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"AsyncRun({self.id!r})"

    @property
    def _sync_run(self) -> Run:
        return Run(self.client._sync, self.id)

    async def detail(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sync_run.detail)

    async def status(self) -> str:
        return await asyncio.to_thread(self._sync_run.status)

    async def node_runs(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._sync_run.node_runs)

    async def cancel(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sync_run.cancel)

    async def logs(
        self, *, level: str | None = None, node: str | None = None
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._sync_run.logs, level=level, node=node)

    # ------------------------------------------------------------- watching

    async def watch(
        self, *, poll_interval: float = _DEFAULT_POLL_INTERVAL
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield this run's detail every time its status changes, stopping
        after the terminal one.

        Backed by a real SODP subscription on ``runs.{id}`` when the
        ``watch`` extra is installed, for near-instant notice; either way,
        a REST poll every ``poll_interval`` seconds runs alongside it as a
        safety net (see the module docstring's notes on why neither the
        subscription's payload nor its presence can be trusted alone).

        Breaking out of ``async for`` early leaves the subscription (and
        the poll loop) running until this generator is garbage-collected;
        call ``await gen.aclose()`` first if that matters for your process
        lifetime -- ``wait()`` below always consumes to completion, so it
        doesn't have this concern.
        """
        sodp = await self.client._sodp_client()
        wake: asyncio.Queue[None] = asyncio.Queue()
        unsub: Callable[[], None] | None = None
        if sodp is not None:

            def _on_change(_value: Any, _meta: Any) -> None:
                wake.put_nowait(None)

            unsub = sodp.watch(f"runs.{self.id}", _on_change)

        try:
            last_status: str | None = None
            while True:
                detail = await self.detail()
                status = str(detail.get("status", ""))
                if status != last_status:
                    last_status = status
                    yield detail
                if status in TERMINAL_RUN_STATUSES:
                    return
                try:
                    await asyncio.wait_for(wake.get(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
                # A burst of deltas during one interval means "go refetch",
                # once -- drain the rest so next loop doesn't do it again
                # per queued wake-up.
                while not wake.empty():
                    wake.get_nowait()
        finally:
            if unsub is not None:
                unsub()

    async def wait(
        self,
        timeout: float = 600.0,
        *,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        raise_on_failure: bool = False,
    ) -> dict[str, Any]:
        """Wait until the run is terminal; return its final API object.

        Push-based via ``watch()`` above, not a polling loop by default --
        see its docstring for what that does and doesn't guarantee.
        Raises stdlib ``TimeoutError`` if still live when the deadline
        passes, and ``RunFailed`` for a non-success terminal status when
        ``raise_on_failure`` is set, exactly like the sync ``Run.wait()``.
        """

        async def _consume() -> dict[str, Any]:
            last: dict[str, Any] | None = None
            async for last in self.watch(poll_interval=poll_interval):
                pass
            assert last is not None
            return last

        try:
            detail = await asyncio.wait_for(_consume(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"run {self.id} did not reach a terminal status within {timeout:.0f}s"
            ) from None
        if raise_on_failure and detail.get("status") != "success":
            raise RunFailed(detail)
        return detail
