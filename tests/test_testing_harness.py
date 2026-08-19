"""brokoli-sdk#15 M2: the local test harness. brokoli-sdk#57 item 9:
live_pipeline, at the bottom of this file.

Proves you can assert graph shape and task logic, and pin IR, without a
server -- and that the harness inspects rather than executes the DAG.
"""

import pytest

from brokoli import Client, Pipeline, source_api, source_file, transform, sink_db, task, map as bmap
from brokoli.client import APIError
from brokoli.testing import Graph, graph, run_task, ir_snapshot, assert_stable_ir, live_pipeline

from conftest import FakeBrokoli


def _static_client(server):
    FakeBrokoli.tokens.add("static-key")
    return Client(server, api_key="static-key")


def _build():
    with Pipeline("orders", pipeline_id="orders") as p:
        src = source_api("Fetch", url="https://api/orders")
        clean = transform(
            "Clean",
            input=src,
            rules=[{"type": "drop_columns", "columns": ["raw"]}],
        )
        load = sink_db("Load", table="orders", conn_id="dw")
        src >> clean >> load
    return p


class TestGraph:
    def test_nodes_edges_and_config(self):
        g = graph(_build())
        assert isinstance(g, Graph)
        assert len(g) == 3
        g.assert_nodes("Fetch", "Clean", "Load")
        g.assert_edge("Fetch", "Clean")
        g.assert_edge("Clean", "Load")
        assert g.kind("Fetch") == "source_api"
        assert g.config("Fetch")["url"] == "https://api/orders"

    def test_upstream_downstream_and_membership(self):
        g = graph(_build())
        assert g.downstream("Fetch") == ["Clean"]
        assert g.upstream("Load") == ["Clean"]
        assert g.downstream("Load") == []
        assert "Clean" in g
        assert "Nope" not in g

    def test_lookup_by_id_and_by_name_agree(self):
        g = graph(_build())
        # ids are node-scoped slugs; name and id resolve to the same node.
        fetch_id = g.node("Fetch")["id"]
        assert g.node(fetch_id) is g.node("Fetch")

    def test_missing_node_and_edge_errors_are_helpful(self):
        g = graph(_build())
        with pytest.raises(KeyError, match="nodes are"):
            g.node("Ghost")
        with pytest.raises(AssertionError, match="expected edge"):
            g.assert_edge("Fetch", "Load")
        with pytest.raises(AssertionError, match="unexpected"):
            g.assert_nodes("Fetch")


class TestRunTask:
    def test_calls_underlying_function_of_a_wrapper(self):
        with Pipeline("t", pipeline_id="t"):

            @task("Enrich")
            def enrich(rows):
                return [dict(r, seen=True) for r in rows]

            @bmap("Double")
            def double(row):
                return {**row, "n": row["n"] * 2}

        # run_task bypasses node registration and calls the real function.
        assert run_task(enrich, [{"id": 1}]) == [{"id": 1, "seen": True}]
        assert run_task(double, {"n": 3}) == {"n": 6}

    def test_accepts_a_plain_function(self):
        def logic(x):
            return x + 1

        assert run_task(logic, 41) == 42

    def test_rejects_non_callable(self):
        with pytest.raises(TypeError, match="callable or a brokoli task"):
            run_task(123)


class TestIRSnapshot:
    def test_snapshot_is_deterministic(self):
        assert ir_snapshot(_build()) == ir_snapshot(_build())

    def test_snapshot_reflects_semantic_change(self):
        def variant():
            with Pipeline("orders", pipeline_id="orders") as p:
                src = source_api("Fetch", url="https://api/orders")
                src >> sink_db("Load", table="orders", conn_id="dw")
            return p

        assert ir_snapshot(_build()) != ir_snapshot(variant())

    def test_assert_stable_ir_passes_for_deterministic_builder(self):
        assert_stable_ir(_build)  # must not raise

    def test_assert_stable_ir_flags_nondeterminism(self):
        # A builder whose node NAME changes each call yields unstable IR.
        counter = {"n": 0}

        def flaky():
            counter["n"] += 1
            with Pipeline("f", pipeline_id="f") as p:
                source_api(f"Fetch-{counter['n']}", url="https://x")
            return p

        with pytest.raises(AssertionError, match="not stable across rebuilds"):
            assert_stable_ir(flaky)


class TestLivePipeline:
    def _pipeline(self, name="Live Test", pipeline_id="live-test"):
        with Pipeline(name, pipeline_id=pipeline_id) as p:
            source_file("Src", path="/tmp/x.csv", format="csv")
        return p

    def test_deploys_under_a_unique_id_and_runs(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)
        pipeline = self._pipeline()

        with live_pipeline(client, pipeline, validate=False) as lp:
            assert lp.pipeline_id.startswith("live-test-")
            assert lp.pipeline_id != "live-test"
            assert lp.id.startswith("created-")

            run = lp.run()
            assert FakeBrokoli.triggered[0]["pipeline"] == lp.id
            assert run.status() == "success"

    def test_deletes_on_exit_even_after_an_exception(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)

        with pytest.raises(RuntimeError, match="boom"):
            with live_pipeline(client, self._pipeline(), validate=False) as lp:
                deployed_id = lp.id
                raise RuntimeError("boom")

        assert FakeBrokoli.deleted_pipeline_ids == [deployed_id]

    def test_cleanup_false_leaves_it_deployed(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)

        with live_pipeline(client, self._pipeline(), validate=False, cleanup=False) as lp:
            deployed_id = lp.id

        assert FakeBrokoli.deleted_pipeline_ids == []
        # Still resolvable -- never got deleted.
        assert client.pipeline(deployed_id)["id"] == deployed_id

    def test_two_live_pipelines_from_the_same_builder_do_not_collide(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)

        with live_pipeline(client, self._pipeline(), validate=False) as first:
            with live_pipeline(client, self._pipeline(), validate=False) as second:
                assert first.pipeline_id != second.pipeline_id

    def test_delete_pipeline_tolerates_already_gone(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)

        with live_pipeline(client, self._pipeline(), validate=False) as lp:
            deployed_id = lp.id
            client.delete_pipeline(deployed_id)  # deleted early, inside the test

        # No error propagates from teardown's own delete of an
        # already-gone pipeline.
        assert FakeBrokoli.deleted_pipeline_ids == [deployed_id]

    def test_delete_pipeline_raises_for_a_real_404(self, server):
        FakeBrokoli.use_cursor_shape = False
        FakeBrokoli.pipelines_flat = []
        client = _static_client(server)

        with pytest.raises(APIError):
            client.delete_pipeline("never-deployed")
