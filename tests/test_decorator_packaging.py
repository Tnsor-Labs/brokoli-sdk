"""Tests for brokoli-sdk#22 M3: all eight decorators share @task's
packaging, and package metadata declares third-party requirements.

@source/@sink/@filter/@map/@validate/@sensor used to ship isolated
function source only -- a module helper or import referenced from any of
them deployed clean and NameError'd remotely, the pre-#7 behavior @task
was cured of. They now flow through the same auto packaging: helpers,
constants, imports, and closures are included, and unsupported shapes
fail locally.

Packaging also emits ``requires_modules``: the non-stdlib root modules of
re-emitted imports, so the package block says what the host Python must
provide instead of leaving it to a remote ImportError.
"""

import pytest

import module_packaging_fixtures as fx

TEST_BASE = "https://api.test"


def local_helper(row):
    """Same-module helper -- must be inlined, not imported."""
    return dict(row, seen=True)

from brokoli import Pipeline, sensor, sink, source, task
from brokoli import filter as brokoli_filter
from brokoli import map as brokoli_map
from brokoli import validate as brokoli_validate


def _cfg(p, name):
    return [n for n in p.to_json()["nodes"] if n["name"] == name][0]["config"]


class TestAllDecoratorsPackage:
    def test_source_includes_module_helper(self):
        with Pipeline("test") as p:

            @source("Fetch")
            def fetch():
                return [local_helper({"id": 1})]

            fetch()
        cfg = _cfg(p, "Fetch")
        assert "def local_helper" in cfg["script"]
        assert "local_helper" in cfg["package"]["included"]

    def test_sink_includes_constant(self):
        with Pipeline("test") as p:

            @source("Fetch")
            def fetch():
                return [{"id": 1}]

            @sink("Save")
            def save(rows):
                return [dict(r, base=TEST_BASE) for r in rows] and None

            fetch() >> save()
        assert "TEST_BASE" in _cfg(p, "Save")["package"]["included"]

    def test_map_filter_validate_sensor_all_package(self):
        with Pipeline("test") as p:

            @source("Fetch")
            def fetch():
                return [{"id": 1}]

            @brokoli_map("Map")
            def scale(row):
                return local_helper(row)

            @brokoli_filter("Filter")
            def keep(row):
                return local_helper(row) is not None

            @brokoli_validate("Validate")
            def check(rows):
                return local_helper(rows[0]) is not None

            @sensor("Sensor", poll_interval=1, timeout=5)
            def ready():
                return local_helper({}) is not None

            fetch() >> scale() >> keep() >> check()
            ready()
        for name in ("Map", "Filter", "Validate", "Sensor"):
            cfg = _cfg(p, name)
            assert "def local_helper" in cfg["script"], name
            assert "local_helper" in cfg["package"]["included"], name

    def test_self_contained_functions_stay_byte_identical(self):
        # The no-references case must not grow a package block -- same
        # compatibility contract @task has had since #7.
        with Pipeline("test") as p:

            @source("Fetch")
            def fetch():
                return [{"id": 1}]

            @brokoli_filter("Keep")
            def keep(row):
                return True

            fetch() >> keep()
        assert "package" not in _cfg(p, "Fetch")
        assert "package" not in _cfg(p, "Keep")


class TestRequiresModules:
    def test_stdlib_imports_not_declared(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_constant, name="Work")
            t()
        assert "requires_modules" not in _cfg(p, "Work").get("package", {})

    def test_third_party_import_declared(self, tmp_path):
        import sys

        # requires_modules is derived from sys.stdlib_module_names, which
        # only exists on 3.10+. On 3.9 the SDK documents that it can't tell
        # stdlib from third-party cheaply and lists nothing — so there's no
        # requires_modules to assert. (The matrix surfaced this.)
        if not hasattr(sys, "stdlib_module_names"):
            pytest.skip("requires_modules needs sys.stdlib_module_names (Python 3.10+)")

        from brokoli.cli import load_pipeline_from_file

        f = tmp_path / "third_party.py"
        f.write_text(
            "import yaml\n"
            "\n"
            "from brokoli import Pipeline, task, source_file\n"
            "\n"
            'with Pipeline("tp", pipeline_id="tp") as p:\n'
            '    src = source_file("Read", path="/tmp/in.csv", format="csv")\n'
            "\n"
            '    @task("Work")\n'
            "    def work(rows):\n"
            "        return yaml.safe_load(yaml.safe_dump(rows))\n"
            "\n"
            "    src >> work(src)\n"
        )
        pipelines = load_pipeline_from_file(str(f))
        cfg = [
            n for n in pipelines[0].to_json()["nodes"] if n["name"] == "Work"
        ][0]["config"]
        assert cfg["package"]["requires_modules"] == ["yaml"]
