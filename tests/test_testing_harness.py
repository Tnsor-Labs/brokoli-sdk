"""brokoli-sdk#15 M2: the local test harness.

Proves you can assert graph shape and task logic, and pin IR, without a
server -- and that the harness inspects rather than executes the DAG.
"""

import pytest

from brokoli import Pipeline, source_api, transform, sink_db, task, map as bmap
from brokoli.testing import Graph, graph, run_task, ir_snapshot, assert_stable_ir


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
