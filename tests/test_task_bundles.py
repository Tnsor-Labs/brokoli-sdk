"""Task-bundle packaging, IR, feature gate, validation, and deploy upload
exercise for ``@task(package="bundle")`` (ADR-031).

The packaging half builds tiny projects on disk (tmp_path/.git keeps
``_find_project_root`` deterministic) and asserts on the archive's manifest
and tar contents; the deploy half runs against the in-process FakeBrokoli
server from conftest, asserting that archives are uploaded content-address
idempotently *before* the referencing pipeline is created.
"""

import gzip
import getpass
import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile

import pytest

from brokoli.compatibility import required_execution_features
from brokoli.taskbundle import FORMAT, BundleError, digest_bytes, package_task_project
from brokoli.validation import validate_pipeline


# Every tmp_path fixture project lives under this prefix; cached modules
# pointing there are stale the moment their owning test ends.
_PYTEST_ROOT = os.path.join(tempfile.gettempdir(), f"pytest-of-{getpass.getuser()}")


def _load_module(name, root):
    """Import *name* from *root*, scoped to this call so the tmp_path
    layout of one test never shadows another project the suite created:
    evict any still-cached module whose source sits in a previous test's
    tmp dir (a stale ``import pkg``/``import mod`` would otherwise resolve
    from sys.modules against a deleted project)."""
    import importlib

    root = str(root)
    base = _PYTEST_ROOT + os.sep
    for key in list(sys.modules):
        source = getattr(sys.modules[key], "__file__", None)
        if source and os.path.realpath(source).startswith(base):
            del sys.modules[key]
    sys.path.insert(0, root)
    try:
        return importlib.import_module(name)
    finally:
        sys.path.remove(root)


def _build(module_name, *, project_root):
    mod = _load_module(module_name, project_root)
    return package_task_project(getattr(mod, "enrich"), "Enrich", project_root=project_root)


def _archive_tar(bundle):
    return tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle.archive)))


# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------


class TestPackageTaskProject:
    def test_bundles_relative_import_and_init_chain(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mypkg").mkdir()
        (tmp_path / "mypkg" / "__init__.py").write_text("")
        (tmp_path / "mypkg" / "helpers.py").write_text("def double(x): return x * 2\n")
        (tmp_path / "mypkg" / "tasks.py").write_text(
            "from .helpers import double\n"
            "def enrich(rows):\n"
            "    return [{**r, 'v': r['v'] * double(2)} for r in rows]\n"
        )
        bundle = _build("mypkg.tasks", project_root=str(tmp_path))

        assert "entry.py" in bundle.files
        assert "mypkg/__init__.py" in bundle.files
        assert "mypkg/helpers.py" in bundle.files
        assert "mypkg/tasks.py" in bundle.files
        # stdlib is imported by the source but never shipped: the worker
        # already has it.
        assert not any(f.startswith(("json", "os")) for f in bundle.files)

    def test_absolute_submodule_import_pulled(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "pkg").mkdir()
        (tmp_path / "pkg" / "__init__.py").write_text("")
        (tmp_path / "pkg" / "sub.py").write_text("VALUE = 7\n")
        (tmp_path / "mod.py").write_text(
            "from pkg import sub\n"
            "def enrich(rows):\n"
            "    return [{**r, 'v': r['v'] * sub.VALUE} for r in rows]\n"
        )
        bundle = _build("mod", project_root=str(tmp_path))
        assert "pkg/sub.py" in bundle.files

    def test_symbol_from_init_does_not_fail(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "pkg").mkdir()
        (tmp_path / "pkg" / "__init__.py").write_text("DOUBLE = 2\n")
        (tmp_path / "mod.py").write_text(
            "from pkg import DOUBLE\n"
            "def enrich(rows):\n"
            "    return [{**r, 'v': r['v'] * DOUBLE} for r in rows]\n"
        )
        bundle = _build("mod", project_root=str(tmp_path))
        assert "mod.py" in bundle.files
        assert "pkg/sub.py" not in bundle.files

    def test_third_party_import_refused_by_name(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text(
            "import brokoli\n"
            "def enrich(rows):\n"
            "    return rows\n"
        )
        with pytest.raises(BundleError) as exc:
            _build("mod", project_root=str(tmp_path))
        assert "brokoli" in str(exc.value)
        assert "third-party" in str(exc.value)

    def test_entry_imports_task_module_under_wrapper_contract(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mypkg").mkdir()
        (tmp_path / "mypkg" / "__init__.py").write_text("")
        (tmp_path / "mypkg" / "tasks.py").write_text(
            "def enrich(rows):\n"
            "    return [{**r, 'v': r['v'] * 2} for r in rows]\n"
        )
        bundle = _build("mypkg.tasks", project_root=str(tmp_path))

        with _archive_tar(bundle) as tf:
            entry = tf.extractfile("entry.py").read().decode()
        assert "from mypkg.tasks import enrich" in entry
        assert bundle.digest == digest_bytes(bundle.archive)

    def test_digest_is_archive_sha256(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text("def enrich(rows):\n    return rows\n")
        bundle = _build("mod", project_root=str(tmp_path))
        assert bundle.digest == "sha256:" + hashlib.sha256(bundle.archive).hexdigest()
        assert bundle.digest.startswith("sha256:") and len(bundle.digest) == 71

    def test_deterministic(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text("def enrich(rows):\n    return rows\n")
        first = _build("mod", project_root=str(tmp_path))
        second = _build("mod", project_root=str(tmp_path))
        assert first.digest == second.digest
        assert first.archive == second.archive

    def test_manifest_contract_shape(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text("def enrich(rows):\n    return rows\n")
        bundle = _build("mod", project_root=str(tmp_path))

        with _archive_tar(bundle) as tf:
            manifest = json.loads(tf.extractfile("manifest.json").read())
        assert manifest["format"] == FORMAT
        assert manifest["language"] == "python"
        assert manifest["language_runtime"].startswith(">=")
        assert manifest["entry"] == "entry.py"
        assert manifest["task_name"] == "Enrich"
        assert manifest["files"] == list(bundle.files)
        assert "archive_sha256" not in manifest  # self-referential; core refuses it

    def test_size_cap(self, tmp_path, monkeypatch):
        monkeypatch.setattr("brokoli.taskbundle.MAX_BYTES", 64)
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text(
            "def enrich(rows):\n    return rows\n# " + "#" * 8000
        )
        with pytest.raises(BundleError) as exc:
            _build("mod", project_root=str(tmp_path))
        assert "over the" in str(exc.value) and "-byte cap" in str(exc.value)


# ---------------------------------------------------------------------------
# Pipeline wiring and IR
# ---------------------------------------------------------------------------


class TestPipelineWiring:
    def _pipeline(self, tmp_path):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text(
            "def enrich(rows):\n"
            "    return [{**r, 'v': r.get('v', 1) * 2} for r in rows]\n"
        )
        mod = _load_module("mod", tmp_path)
        from brokoli import Pipeline, task

        with Pipeline("b", pipeline_id="b1") as pl:
            task("Enrich", package="bundle")(mod.enrich)()
        return pl

    def test_bundle_via_task_decorator(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pl = self._pipeline(tmp_path)
        node = pl.to_json()["nodes"][0]
        assert list(node["config"]) == ["language", "task_bundle"]
        tb = node["config"]["task_bundle"]
        assert tb["format"] == FORMAT
        assert tb["digest"].startswith("sha256:")
        assert [b.digest for b in pl.task_bundles] == [tb["digest"]]

    def test_ir_digest_accounts_for_bundle_digest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from brokoli import ir_digest

        pl = self._pipeline(tmp_path)
        original = pl.to_json()
        tampered = {**original, "nodes": [dict(n) for n in original["nodes"]]}
        tampered["nodes"][0]["config"] = dict(tampered["nodes"][0]["config"])
        tampered["nodes"][0]["config"]["task_bundle"] = dict(
            tampered["nodes"][0]["config"]["task_bundle"]
        )
        tampered["nodes"][0]["config"]["task_bundle"]["digest"] = "sha256:" + "0" * 64
        assert ir_digest(tampered) != ir_digest(original)

    def test_validate_accepts_bundle_node(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = validate_pipeline(self._pipeline(tmp_path))
        assert result.valid, [str(e) for e in result.errors]

    def test_feature_gate_reports_task_bundles(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from brokoli import Pipeline, task

        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text(
            "def enrich(rows):\n"
            "    return [{**r, 'v': r.get('v', 1) * 2} for r in rows]\n"
        )
        mod = _load_module("mod", tmp_path)

        with Pipeline("a", pipeline_id="a1") as pl:
            task("Plain")(mod.enrich)()
        assert "task-bundles" not in required_execution_features(pl.to_json())

        bundle_pl = self._pipeline(tmp_path)
        assert "task-bundles" in required_execution_features(bundle_pl.to_json())

    def test_schema_parity_bundle_payload(self, tmp_path, monkeypatch):
        jsonschema = pytest.importorskip("jsonschema")
        import json as _json
        from pathlib import Path

        schema = _json.loads(
            (Path(__file__).parent / "fixtures" / "pipeline-ir-2.1.json").read_text()
        )
        monkeypatch.chdir(tmp_path)
        jsonschema.validate(self._pipeline(tmp_path).to_json(), schema)


def _upload_through_cli(server, auth, bundle):
    from brokoli import cli

    fake = type("_Deployable", (), {"name": "orders", "task_bundles": (bundle,)})()
    cli._upload_task_bundles(server, auth, [fake])


class _CorruptBundle:
    """A bundle with a valid digest but flipped archive bytes — what a
    truly adversarial or bit-rotted client would POST."""

    def __init__(self, bundle):
        self.digest = bundle.digest
        corrupt = bytearray(bundle.archive)
        corrupt[len(corrupt) // 2] ^= 0xFF
        self.archive = bytes(corrupt)


# ---------------------------------------------------------------------------
# Deploy upload ordering (FakeBrokoli server fixture from conftest)
# ---------------------------------------------------------------------------


class TestDeployUpload:
    def _bundle_pipeline(self, tmp_path, name="orders", pid="orders"):
        (tmp_path / ".git").mkdir(exist_ok=True)
        (tmp_path / "mod.py").write_text(
            "def enrich(rows):\n"
            "    return [{**r, 'v': r.get('v', 1) * 2} for r in rows]\n"
        )
        mod = _load_module("mod", tmp_path)
        from brokoli import Pipeline, task

        with Pipeline(name, pipeline_id=pid) as pl:
            task("Enrich", package="bundle")(mod.enrich)()
        return pl

    def test_bundle_uploaded_before_create(self, server, monkeypatch, tmp_path):
        self._quiet_preflight(monkeypatch)
        monkeypatch.chdir(tmp_path)
        from brokoli.client import Client

        pl = self._bundle_pipeline(tmp_path)
        bundle = list(pl.task_bundles)[0]

        FakeBrokoli.tokens.add("static-key")
        client = Client(server, api_key="static-key")
        result = client.deploy(pl, validate=False)

        assert result["id"].startswith("created-")
        assert any(kind == "created" and digest == bundle.digest for kind, digest in FakeBrokoli.task_uploads)
        upload_i = FakeBrokoli.events.index(f"upload {bundle.digest}")
        create_i = FakeBrokoli.events.index("create")
        assert upload_i < create_i
        method, payload = FakeBrokoli.deployed[0]
        assert method == "POST"
        assert payload["nodes"][0]["config"]["task_bundle"]["digest"] == bundle.digest

    def test_same_digest_uploaded_once(self, server, monkeypatch, tmp_path):
        # The CLI deployer uploads across many pipelines in one shot,
        # deduplicated by digest: two pipelines from the same project must
        # POST the archive exactly once (the second is an unchanged 200).
        monkeypatch.chdir(tmp_path)
        from brokoli import cli

        pl = self._bundle_pipeline(tmp_path, name="a", pid="a1")
        pl2 = self._bundle_pipeline(tmp_path, name="b", pid="b1")
        digest = list(pl.task_bundles)[0].digest
        assert digest == list(pl2.task_bundles)[0].digest  # identical project

        FakeBrokoli.tokens.add("static-key")
        cli._upload_task_bundles(server, "Bearer static-key", [pl, pl2])
        assert [d for _, d in FakeBrokoli.task_uploads if d == digest] == [digest]
        assert FakeBrokoli.task_stored_digests == {digest}

    def test_upload_mismatched_bytes_refused(self, server, monkeypatch, tmp_path):
        # The server re-hashes the body: a bundle claiming a digest it does
        # not hash to is refused with a 400, exactly like the real handler.
        monkeypatch.chdir(tmp_path)
        FakeBrokoli.tokens.add("static-key")

        pl = self._bundle_pipeline(tmp_path)
        bundle = list(pl.task_bundles)[0]

        with pytest.raises(Exception) as exc:
            _upload_through_cli(server, "Bearer static-key", _CorruptBundle(bundle))
        assert "HTTP 400" in str(exc.value) or "digest mismatch" in str(exc.value)

    @staticmethod
    def _quiet_preflight(monkeypatch):
        import brokoli.compatibility as compatibility

        monkeypatch.setattr(compatibility, "preflight_server_compatibility", lambda *a, **k: None)


from conftest import FakeBrokoli  # noqa: E402