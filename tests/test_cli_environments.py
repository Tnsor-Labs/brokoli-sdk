"""brokoli-sdk#15 M3: named deployment environments.

`brokoli <cmd> --env prod` resolves the server (and optionally the token)
from a `brokoli.yaml`, so operators don't repeat --server/--api-key or
embed secrets in the file.
"""

import argparse

import pytest

import brokoli.cli as cli
from brokoli.exceptions import DeployError


def _write_config(tmp_path, monkeypatch, body):
    cfg = tmp_path / "brokoli.yaml"
    cfg.write_text(body)
    monkeypatch.setenv("BROKOLI_CONFIG", str(cfg))
    return cfg


def _ns(**kw):
    kw.setdefault("server", None)
    kw.setdefault("env", None)
    kw.setdefault("api_key", "")
    return argparse.Namespace(**kw)


CONFIG = """
environments:
  dev:
    server: http://localhost:8080
  prod:
    server: https://prod.example.com/
    token_env: BROKOLI_PROD_TOKEN
"""


class TestLoadEnvironments:
    def test_missing_config_is_empty(self, monkeypatch, tmp_path):
        monkeypatch.delenv("BROKOLI_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)  # no brokoli.yaml here
        assert cli._load_environments() == {}

    def test_reads_environments(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        envs = cli._load_environments()
        assert set(envs) == {"dev", "prod"}
        assert envs["prod"]["server"] == "https://prod.example.com/"

    def test_non_mapping_environments_errors(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, "environments: [1, 2, 3]\n")
        with pytest.raises(DeployError, match="must be a mapping"):
            cli._load_environments()


class TestResolveTarget:
    def test_env_supplies_server(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        server, _ = cli._resolve_target(_ns(env="dev"), "deploy")
        assert server == "http://localhost:8080"

    def test_explicit_server_wins_over_env(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        server, _ = cli._resolve_target(
            _ns(env="prod", server="https://override.example"), "deploy"
        )
        assert server == "https://override.example"

    def test_env_trailing_slash_trimmed(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        server, _ = cli._resolve_target(_ns(env="prod"), "deploy")
        assert server == "https://prod.example.com"

    def test_env_token_env_supplies_auth(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        monkeypatch.delenv("BROKOLI_TOKEN", raising=False)
        monkeypatch.setenv("BROKOLI_PROD_TOKEN", "prod-secret")
        server, auth = cli._resolve_target(_ns(env="prod"), "deploy")
        assert auth == "Bearer prod-secret"

    def test_explicit_token_beats_env_token(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        monkeypatch.setenv("BROKOLI_TOKEN", "explicit")
        monkeypatch.setenv("BROKOLI_PROD_TOKEN", "prod-secret")
        _, auth = cli._resolve_target(_ns(env="prod"), "deploy")
        assert auth == "Bearer explicit"

    def test_unknown_env_errors_with_known_list(self, monkeypatch, tmp_path):
        _write_config(tmp_path, monkeypatch, CONFIG)
        with pytest.raises(DeployError, match="unknown environment 'staging'.*dev, prod"):
            cli._resolve_target(_ns(env="staging"), "deploy")

    def test_no_server_no_env_uses_default(self, monkeypatch, tmp_path):
        monkeypatch.delenv("BROKOLI_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        server, _ = cli._resolve_target(_ns(), "deploy", default_server="http://d")
        assert server == "http://d"

    def test_no_server_no_env_no_default_errors(self, monkeypatch, tmp_path):
        monkeypatch.delenv("BROKOLI_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        with pytest.raises(DeployError, match="no server"):
            cli._resolve_target(_ns(), "run")
