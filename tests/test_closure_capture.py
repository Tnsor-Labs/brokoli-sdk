"""Tests for brokoli-sdk#22 M2: closure capture in @task auto packaging.

A task defined inside a factory closes over enclosing-scope locals. The
deployed script is a top-level ``def``, so those names resolve as globals
there -- auto packaging now materializes JSON-serializable closure values
as constants in the script (frozen at serialization time, same contract
as module constants), includes module-level functions captured under
their own name as helpers, and fails locally naming anything else.
Decorators without packaging reject closures loudly instead of emitting
a script that NameErrors remotely.
"""

import textwrap

import pytest

import module_packaging_fixtures as fx
from brokoli import Pipeline, task
from brokoli.cli import load_pipeline_from_file
from brokoli.exceptions import PipelineError


def _script(p, name="Work"):
    return [n for n in p.to_json()["nodes"] if n["name"] == name][0]["config"]


def make_threshold_task(threshold):
    @task("Work")
    def work(rows):
        return [r for r in rows if r.get("score", 0) > threshold]

    return work


class TestFactoryCapture:
    def test_serializable_closure_value_is_captured(self):
        with Pipeline("test") as p:
            make_threshold_task(50)()
        cfg = _script(p)
        assert "threshold = 50" in cfg["script"]
        assert "threshold" in cfg["package"]["included"]

    def test_each_factory_instance_captures_its_own_value(self):
        with Pipeline("test") as p:
            t1 = make_threshold_task(10)
            t2 = make_threshold_task(99)
            a = t1()
            b = t2()
            a >> b
        nodes = {n["id"]: n["config"]["script"] for n in p.to_json()["nodes"]}
        scripts = sorted(nodes.values())
        assert any("threshold = 10" in s for s in scripts)
        assert any("threshold = 99" in s for s in scripts)

    def test_factory_local_helper_captured_through_closure(self):
        with Pipeline("test") as p:
            t = task(fx.make_work_with_local_helper(), name="Work")
            t()
        cfg = _script(p)
        assert "def local_scale" in cfg["script"]
        assert "local_scale" in cfg["package"]["included"]

    def test_cross_module_function_captured_by_closure_fails(self):
        # Same contract as name-referenced helpers: only same-module
        # functions can be inlined; an aliased import must fail naming
        # the variable, not deploy a script missing it.
        renamed = fx.shared_helper

        with Pipeline("test"):

            @task("Work")
            def work(rows):
                return [renamed(r) for r in rows]

            with pytest.raises(PipelineError, match="renamed"):
                work()

    def test_unserializable_closure_fails_naming_the_variable(self):
        class Conn:
            pass

        conn = Conn()
        with Pipeline("test"):

            @task("Work")
            def work(rows):
                return [r for r in rows if conn]

            with pytest.raises(PipelineError, match="conn"):
                work()

    def test_module_mode_rejects_closures_with_guidance(self):
        limit = 5
        with Pipeline("test"):

            @task("Work", package="module")
            def work(rows):
                return rows[:limit]

            with pytest.raises(PipelineError, match="limit"):
                work()


class TestNonPackagingDecoratorsRejectClosures:
    def test_filter_decorator_rejects_closure(self):
        from brokoli import filter as brokoli_filter

        floor = 10
        with Pipeline("test"):

            @brokoli_filter("Keep")
            def keep(row):
                return row.get("v", 0) > floor

            with pytest.raises(PipelineError, match="floor"):
                keep()


class TestClosureCaptureThroughCLILoader:
    def test_factory_pipeline_packages_via_loader(self, tmp_path):
        f = tmp_path / "factory_pipeline.py"
        f.write_text(
            textwrap.dedent(
                '''
                from brokoli import Pipeline, task, source_file, sink_file

                def make(threshold):
                    @task("Work")
                    def work(rows):
                        return [r for r in rows if r.get("v", 0) > threshold]
                    return work

                with Pipeline("factory-loader", pipeline_id="factory-loader") as p:
                    src = source_file("Read", path="/tmp/in.csv", format="csv")
                    src >> make(42)(src) >> sink_file("Save", path="/tmp/out.csv", format="csv")
                '''
            )
        )
        pipelines = load_pipeline_from_file(str(f))
        cfg = [
            n for n in pipelines[0].to_json()["nodes"] if n["name"] == "Work"
        ][0]["config"]
        assert "threshold = 42" in cfg["script"]
        assert "threshold" in cfg["package"]["included"]
