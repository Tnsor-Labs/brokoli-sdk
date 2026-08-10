"""Task packaging must behave identically under the CLI's file loader.

`brokoli deploy <file>` loads pipeline files via
``importlib.util.spec_from_file_location`` (``load_pipeline_from_file``),
not a normal ``import``. Packaging resolves each task's containing module
with ``inspect.getmodule`` -- which walks ``sys.modules`` -- so the loader
must register what it loads. Before it did, import detection silently came
back empty under the CLI: the same file whose ``import json`` packaged
fine when imported normally failed with "references json, which can't be
safely auto-included" on the primary deploy path (found live-testing
brokoli-sdk#24; see #22).
"""

import sys
import textwrap

from brokoli.cli import load_pipeline_from_file

PIPELINE_WITH_IMPORTS = textwrap.dedent(
    '''
    import json
    from datetime import timezone

    RATE = 2.5

    def _scale(row):
        return {k: v for k, v in row.items()}

    from brokoli import Pipeline, task, source_file, sink_file

    with Pipeline("loader-test-{n}", pipeline_id="loader-test-{n}") as p:
        src = source_file("Read", path="/tmp/in.csv", format="csv")

        @task("Work")
        def work(rows):
            _ = timezone.utc
            return json.loads(json.dumps([_scale(r) for r in rows]))

        src >> work(src) >> sink_file("Save", path="/tmp/out.csv", format="csv")
    '''
)


def _write(tmp_path, name, n):
    f = tmp_path / name
    f.write_text(PIPELINE_WITH_IMPORTS.replace("{n}", str(n)))
    return str(f)


def _work_node(pipeline):
    return [n for n in pipeline.to_json()["nodes"] if n["name"] == "Work"][0]


class TestLoaderContextPackaging:
    def test_top_level_imports_package_under_cli_loader(self, tmp_path):
        pipelines = load_pipeline_from_file(_write(tmp_path, "etl.py", 1))
        node = _work_node(pipelines[0])
        script = node["config"]["script"]
        assert "import json" in script
        assert "from datetime import timezone" in script
        assert "def _scale" in script
        included = node["config"]["package"]["included"]
        assert "json" in included and "timezone" in included
        assert "RATE" not in included  # unreferenced constant stays out

    def test_directory_deploy_does_not_shadow_earlier_files(self, tmp_path):
        first = load_pipeline_from_file(_write(tmp_path, "alpha.py", 1))
        second = load_pipeline_from_file(_write(tmp_path, "beta.py", 2))
        # Serialize the FIRST file's pipeline *after* the second loaded:
        # module resolution must still find the right module.
        node = _work_node(first[0])
        assert "import json" in node["config"]["script"]
        node2 = _work_node(second[0])
        assert "import json" in node2["config"]["script"]

    def test_same_stem_files_get_distinct_module_names(self, tmp_path):
        d1 = tmp_path / "a"
        d2 = tmp_path / "b"
        d1.mkdir()
        d2.mkdir()
        p1 = load_pipeline_from_file(_write(d1, "pipeline.py", 1))
        p2 = load_pipeline_from_file(_write(d2, "pipeline.py", 2))
        assert "import json" in _work_node(p1[0])["config"]["script"]
        assert "import json" in _work_node(p2[0])["config"]["script"]

    def test_failed_exec_does_not_leak_module_registration(self, tmp_path):
        bad = tmp_path / "boom.py"
        bad.write_text("raise RuntimeError('boom')\n")
        before = set(sys.modules)
        try:
            load_pipeline_from_file(str(bad))
        except RuntimeError:
            pass
        leaked = {m for m in set(sys.modules) - before if m.startswith("_brokoli_")}
        assert not leaked
