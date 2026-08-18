"""brokoli-sdk#57 item 8: the async run-ops client.

Reuses test_client.py's FakeBrokoli/server fixture for the REST surface
(AsyncClient delegates every REST call to a plain Client via
asyncio.to_thread -- there's only one HTTP implementation to test) and
adds a small fake SODP client for the push-notification path, so these
tests don't need a real WebSocket server. Every test that touches
asyncio wraps a plain (non-async) test function in ``asyncio.run`` --
matching this repo's existing pattern (see test_authoring_context.py)
rather than adding pytest-asyncio as a new dependency.
"""

from __future__ import annotations

import asyncio

import pytest

from brokoli.async_client import AsyncClient, _ws_url
from brokoli.client import RunFailed

from conftest import FakeBrokoli, PIPES


class FakeSodpClient:
    """Enough of sodp.SodpClient's watch() surface to drive AsyncRun.watch()
    tests without a real WebSocket connection."""

    def __init__(self) -> None:
        self._watchers: dict[str, list] = {}
        self.closed = False

    def watch(self, key, callback):
        self._watchers.setdefault(key, []).append(callback)

        def unsub() -> None:
            self._watchers.get(key, []).remove(callback)

        return unsub

    def fire(self, key: str) -> None:
        for cb in list(self._watchers.get(key, [])):
            cb(None, None)

    def close(self) -> None:
        self.closed = True


def _static_client(server_url: str) -> AsyncClient:
    FakeBrokoli.tokens.add("static-key")
    return AsyncClient(server_url, api_key="static-key")


def test_ws_url_translation():
    assert _ws_url("http://localhost:8080") == "ws://localhost:8080/api/ws"
    assert _ws_url("https://brokoli.example.com") == "wss://brokoli.example.com/api/ws"
    with pytest.raises(ValueError):
        _ws_url("ftp://nope")


class TestRestDelegation:
    """AsyncClient's REST methods delegate to a plain Client -- these just
    confirm the delegation is wired, not re-testing every response-shape
    absorption test_client.py already covers."""

    def test_pipelines_and_run_and_cancel(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = PIPES

        async def main():
            client = _static_client(server)
            pipelines = await client.pipelines()
            assert {p["pipeline_id"] for p in pipelines} == {"orders", "events"}

            run = await client.run("orders")
            assert FakeBrokoli.triggered[0]["pipeline"] == "uuid-1"

            detail = await run.detail()
            assert detail["id"] == run.id

            cancelled = await run.cancel()
            assert cancelled["status"] == "cancelled"

        asyncio.run(main())

    def test_run_handle_and_logs(self, server):
        FakeBrokoli.log_entries = [{"message": "hello"}]

        async def main():
            client = _static_client(server)
            run = client.run_handle("some-run")  # not a coroutine -- no I/O
            FakeBrokoli.run_statuses["some-run"] = "success"
            entries = await run.logs()
            assert entries == [{"message": "hello"}]

        asyncio.run(main())


class TestWatchAndWait:
    def test_wait_resolves_via_push_notification(self, server):
        """A tiny poll_interval would also make this pass -- the point of
        this test is that it resolves fast with a LARGE poll_interval,
        which only push notifications can explain."""
        FakeBrokoli.run_statuses["run-push"] = "running"

        async def main():
            client = _static_client(server)
            client._sodp = FakeSodpClient()
            run = client.run_handle("run-push")

            async def flip_after_delay():
                await asyncio.sleep(0.1)
                FakeBrokoli.run_statuses["run-push"] = "success"
                client._sodp.fire("runs.run-push")

            asyncio.create_task(flip_after_delay())
            detail = await run.wait(timeout=5.0, poll_interval=999.0)
            assert detail["status"] == "success"

        asyncio.run(main())

    def test_wait_falls_back_to_polling_without_watch_extra(self, server, monkeypatch):
        import brokoli.async_client as async_client_module

        monkeypatch.setattr(async_client_module, "SodpClient", None)
        FakeBrokoli.run_statuses["run-poll"] = "running"

        async def main():
            client = _static_client(server)
            run = client.run_handle("run-poll")

            async def flip_after_delay():
                await asyncio.sleep(0.1)
                FakeBrokoli.run_statuses["run-poll"] = "success"

            asyncio.create_task(flip_after_delay())
            detail = await run.wait(timeout=5.0, poll_interval=0.05)
            assert detail["status"] == "success"

        asyncio.run(main())

    def test_sodp_connect_timeout_falls_back_to_polling(self, server, monkeypatch):
        """An unreachable SODP server must not hang wait() forever -- see
        _sodp_client()'s docstring on why a bounded connect timeout exists."""
        import brokoli.async_client as async_client_module

        class _NeverReadySodpClient:
            def __init__(self, *args, **kwargs):
                self.closed = False

            @property
            def ready(self):
                return asyncio.Event().wait()  # never set -- never resolves

            def close(self):
                self.closed = True

        monkeypatch.setattr(async_client_module, "SodpClient", _NeverReadySodpClient)
        monkeypatch.setattr(async_client_module, "_SODP_CONNECT_TIMEOUT", 0.05)
        FakeBrokoli.run_statuses["run-unreachable"] = "running"

        async def main():
            client = _static_client(server)
            run = client.run_handle("run-unreachable")

            async def flip_after_delay():
                await asyncio.sleep(0.15)
                FakeBrokoli.run_statuses["run-unreachable"] = "success"

            asyncio.create_task(flip_after_delay())
            detail = await run.wait(timeout=5.0, poll_interval=0.05)
            assert detail["status"] == "success"

        asyncio.run(main())

    def test_watch_yields_once_per_distinct_status(self, server):
        FakeBrokoli.run_statuses["run-multi"] = "running"

        async def main():
            client = _static_client(server)
            client._sodp = FakeSodpClient()
            run = client.run_handle("run-multi")

            async def transitions():
                await asyncio.sleep(0.05)
                # Same status again -- must not produce an extra yield.
                client._sodp.fire("runs.run-multi")
                await asyncio.sleep(0.05)
                FakeBrokoli.run_statuses["run-multi"] = "success"
                client._sodp.fire("runs.run-multi")

            asyncio.create_task(transitions())

            statuses = []
            async for detail in run.watch(poll_interval=999.0):
                statuses.append(detail["status"])

            assert statuses == ["running", "success"]

        asyncio.run(main())

    def test_wait_raises_on_failure(self, server):
        FakeBrokoli.run_statuses["run-fail"] = "failed"
        FakeBrokoli.runs["run-fail"] = {"error": "node exploded"}

        async def main():
            client = _static_client(server)
            client._sodp = FakeSodpClient()
            run = client.run_handle("run-fail")
            with pytest.raises(RunFailed):
                await run.wait(timeout=5.0, raise_on_failure=True)

        asyncio.run(main())

    def test_wait_times_out(self, server):
        FakeBrokoli.run_statuses["run-stuck"] = "running"

        async def main():
            client = _static_client(server)
            client._sodp = FakeSodpClient()
            run = client.run_handle("run-stuck")
            with pytest.raises(TimeoutError):
                await run.wait(timeout=0.2, poll_interval=0.05)

        asyncio.run(main())

    def test_watch_early_break_then_aclose_unsubscribes(self, server):
        FakeBrokoli.run_statuses["run-break"] = "running"

        async def main():
            client = _static_client(server)
            fake = FakeSodpClient()
            client._sodp = fake
            run = client.run_handle("run-break")

            gen = run.watch(poll_interval=999.0)
            async for _detail in gen:
                break
            await gen.aclose()

            assert fake._watchers.get("runs.run-break", []) == []

        asyncio.run(main())


class TestLifecycle:
    def test_context_manager_closes_sodp(self, server):
        async def main():
            fake = FakeSodpClient()
            async with _static_client(server) as client:
                client._sodp = fake
            assert fake.closed is True

        asyncio.run(main())

    def test_from_env(self, server, monkeypatch):
        FakeBrokoli.tokens.add("env-token")
        monkeypatch.setenv("BROKOLI_SERVER", server)
        monkeypatch.setenv("BROKOLI_TOKEN", "env-token")

        async def main():
            client = AsyncClient.from_env()
            assert client.server == server

        asyncio.run(main())
