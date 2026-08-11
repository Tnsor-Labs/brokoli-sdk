"""brokoli-sdk#15 M3: auditable IR digests.

`ir_digest` is a stable content hash of the normalized IR — the audit
primitive behind `deploy`'s printed digest and `compile --digest`.
"""

import argparse

import pytest

from brokoli import Pipeline, source_api, sink_db, ir_digest
import brokoli.cli as cli


def _build(pipeline_id="orders"):
    with Pipeline("Orders", pipeline_id=pipeline_id) as p:
        src = source_api("Fetch", url="https://api/orders")
        src >> sink_db("Load", table="orders", conn_id="dw")
    return p


class TestIrDigest:
    def test_shape_is_sha256_prefixed(self):
        d = ir_digest(_build().to_json())
        assert d.startswith("sha256:")
        assert len(d) == len("sha256:") + 64  # hex sha256

    def test_stable_across_rebuilds(self):
        assert ir_digest(_build().to_json()) == ir_digest(_build().to_json())

    def test_ignores_server_only_fields(self):
        # A create (no id) and an update (server id + timestamps) of the
        # same content must digest identically — normalize_ir strips them.
        base = _build().to_json()
        as_update = dict(base, id="srv-uuid-123", created_at="2026-01-01T00:00:00Z")
        assert ir_digest(base) == ir_digest(as_update)

    def test_changes_with_semantics(self):
        other = _build().to_json()
        other["nodes"][0]["config"]["url"] = "https://api/DIFFERENT"
        assert ir_digest(_build().to_json()) != ir_digest(other)

    def test_distinct_pipelines_differ(self):
        assert ir_digest(_build("a").to_json()) != ir_digest(_build("b").to_json())


class TestCompileDigestCommand:
    def test_prints_digest_and_name_per_pipeline(self, monkeypatch, capsys, tmp_path):
        f = tmp_path / "p.py"
        f.write_text(
            "from brokoli import Pipeline, source_api, sink_db\n"
            "with Pipeline('Orders', pipeline_id='orders') as p:\n"
            "    src = source_api('Fetch', url='https://api/orders')\n"
            "    src >> sink_db('Load', table='orders', conn_id='dw')\n"
        )
        args = argparse.Namespace(
            file=str(f), format="yaml", check=False, normalized=False, digest=True
        )
        rc = cli.compile_cmd(args)
        assert rc == 0
        out = capsys.readouterr().out.strip()
        assert out.startswith("sha256:")
        assert out.endswith("Orders")
        # matches the library digest for the same pipeline
        assert out.split()[0] == ir_digest(_build().to_json())
