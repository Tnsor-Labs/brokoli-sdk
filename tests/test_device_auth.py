"""Device authorization (#75): the grant end to end against the fake
server, the pacing answers, the refusals, stored credentials, and the CLI
resolver picking stored tokens up as the last fallback."""

import json
import os
import stat

import pytest

from brokoli import device
from brokoli.client import Client
from conftest import FakeBrokoli


@pytest.fixture()
def credfile(tmp_path, monkeypatch):
    path = tmp_path / "credentials.json"
    monkeypatch.setenv("BROKOLI_CREDENTIALS", str(path))
    return path


class TestDeviceLogin:
    def test_happy_path_stores_a_user_only_token(self, server, credfile):
        FakeBrokoli.device_poll_answers = [
            {"status": "authorization_pending"},
            {"status": "approved", "token": "tok-123", "username": "approver"},
        ]
        token = device.device_login(server, open_browser=False, echo=lambda _line: None)
        assert token == "tok-123"
        assert device.load_token(server) == "tok-123"
        mode = stat.S_IMODE(os.stat(credfile).st_mode)
        assert mode == 0o600, f"credentials file mode {oct(mode)}"
        # Keyed by server, so two servers coexist.
        assert json.loads(credfile.read_text())["servers"][server.rstrip("/")] == "tok-123"

    def test_slow_down_widens_the_interval(self, server, credfile):
        FakeBrokoli.device_poll_answers = [
            {"status": "slow_down"},
            {"status": "approved", "token": "tok-9", "username": ""},
        ]
        assert device.device_login(server, open_browser=False, echo=lambda _line: None) == "tok-9"

    def test_denied_raises(self, server, credfile):
        FakeBrokoli.device_poll_answers = [{"status": "access_denied"}]
        with pytest.raises(device.DeviceAuthError, match="denied"):
            device.device_login(server, open_browser=False, echo=lambda _line: None)

    def test_expired_raises_naming_the_retry(self, server, credfile):
        FakeBrokoli.device_poll_answers = [{"status": "expired"}]
        with pytest.raises(device.DeviceAuthError, match="brokoli auth"):
            device.device_login(server, open_browser=False, echo=lambda _line: None)

    def test_unsupported_server_refuses_naming_alternatives(self, server, credfile):
        FakeBrokoli.device_supported = False
        with pytest.raises(device.DeviceAuthError, match="username/password or an API key"):
            device.device_login(server, open_browser=False, echo=lambda _line: None)


class TestStoredCredentialsIntegration:
    def test_cli_resolver_falls_back_to_stored_token(self, server, credfile, monkeypatch):
        import argparse

        from brokoli.cli import _resolve_target

        device.store_token(server, "stored-tok")
        monkeypatch.delenv("BROKOLI_TOKEN", raising=False)
        args = argparse.Namespace(server=server, env=None, api_key="")
        resolved_server, auth = _resolve_target(args, "test")
        assert auth == "Bearer stored-tok"
        # Explicit env token beats stored credentials.
        monkeypatch.setenv("BROKOLI_TOKEN", "explicit-tok")
        _, auth = _resolve_target(args, "test")
        assert auth == "Bearer explicit-tok"

    def test_client_device_auth_uses_stored_token(self, server, credfile):
        device.store_token(server, "stored-tok")
        c = Client(server, device_auth=True)
        assert c._token == "stored-tok"

    def test_client_device_auth_excludes_other_styles(self, server, credfile):
        with pytest.raises(ValueError, match="excludes"):
            Client(server, device_auth=True, api_key="k")

    def test_forget_token(self, server, credfile):
        device.store_token(server, "t")
        device.forget_token(server)
        assert device.load_token(server) == ""
