"""Tests for brokoli-sdk#22 M2 (remainder): relative-import detection and
decorated-helper preservation.

Relative imports re-emitted into a standalone deployed script raise
``ImportError: attempted relative import with no known parent package``
remotely -- packaging now rejects them locally, naming the binding, in
both auto and module modes.

Auto-included helpers used to have their decorator lines stripped by
source extraction, silently changing behavior (@lru_cache stopped
caching). Helpers now keep their decorators, and decorator names resolve
through the same import machinery.
"""

import textwrap

import pytest

import module_packaging_fixtures as fx
from brokoli import Pipeline, task
from brokoli.exceptions import PipelineError


def _script(p, name="Work"):
    return [n for n in p.to_json()["nodes"] if n["name"] == name][0]["config"]


class TestDecoratedHelpers:
    def test_helper_keeps_decorator_and_its_import(self):
        with Pipeline("test") as p:
            t = task(fx.work_with_cached_helper, name="Work")
            t()
        cfg = _script(p)
        assert "@functools.lru_cache(maxsize=None)" in cfg["script"]
        assert "import functools" in cfg["script"]
        assert "functools" in cfg["package"]["included"]

    def test_deployed_script_with_decorated_helper_executes(self):
        # The emitted script must be runnable Python end to end, not just
        # string-plausible: exec it the way the server-side wrapper does.
        with Pipeline("test") as p:
            t = task(fx.work_with_cached_helper, name="Work")
            t()
        script = _script(p)["script"]
        scope = {"rows": [{"k": "a"}, {"k": "b"}], "columns": ["k"], "config": {}, "params": {}}
        exec(script, scope)  # noqa: S102 -- deliberate: proves the artifact runs
        assert scope["output_data"]["rows"][0]["v"] == 1
        assert scope["output_data"]["rows"][1]["v"] == 2


class TestRelativeImportRejection:
    def _make_package(self, tmp_path, task_line):
        pkg = tmp_path / "mypkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")
        (pkg / "helpers.py").write_text("def scale(row):\n    return row\n")
        (pkg / "pipe.py").write_text(
            textwrap.dedent(
                f'''
                from .helpers import scale

                from brokoli import Pipeline, task, source_file, sink_file

                with Pipeline("rel-import", pipeline_id="rel-import") as p:
                    src = source_file("Read", path="/tmp/in.csv", format="csv")

                    @task("Work"{task_line})
                    def work(rows):
                        return [scale(r) for r in rows]

                    src >> work(src) >> sink_file("Save", path="/tmp/out.csv", format="csv")
                '''
            )
        )
        return pkg

    def _import_pipe(self, tmp_path, monkeypatch, task_line=""):
        import importlib
        import sys

        self._make_package(tmp_path, task_line)
        monkeypatch.syspath_prepend(str(tmp_path))
        for mod in ("mypkg", "mypkg.helpers", "mypkg.pipe"):
            sys.modules.pop(mod, None)
        return importlib.import_module("mypkg.pipe")

    def test_auto_mode_rejects_relative_import_naming_binding(self, tmp_path, monkeypatch):
        with pytest.raises(PipelineError, match="scale.*relative import|relative import"):
            self._import_pipe(tmp_path, monkeypatch)

    def test_module_mode_rejects_relative_import(self, tmp_path, monkeypatch):
        with pytest.raises(PipelineError, match="relative import"):
            self._import_pipe(tmp_path, monkeypatch, task_line=', package="module"')
