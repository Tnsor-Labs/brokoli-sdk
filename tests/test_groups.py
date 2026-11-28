"""Mapped groups in the Python SDK (ADR-050 phase 2, IR 2.3).

``group(...)`` is a context manager: the nodes built in its body are the
group's subgraph, run once per item of the collection its ``map`` ranges
over, their per-item outputs combined by ``collect``. These tests pin the
emitted IR shape against core's contract (the group node carries
``nodes``/``edges``/``collect``, the map block stays in ``config.map``, and
the group wires like an ordinary node), the structural rules the SDK enforces
locally so the author sees a fault before deploy, and the execution-feature
gate.
"""

import pytest

from brokoli import (
    Map,
    Pipeline,
    code,
    condition_node,
    group,
    sink_db,
    source_db,
    transform,
    union,
)
from brokoli.compatibility import required_execution_features
from brokoli.exceptions import PipelineError


def _flat_group_pipeline():
    with Pipeline("per-table", pipeline_id="per-table") as p:
        tables = source_db(
            "List tables", query="SELECT 1", conn_id="warehouse", node_key="tables"
        )
        with group(
            "Per table",
            map=Map(over=tables, key="table_name", on_item_failure="continue"),
            collect="union",
            node_key="per_table",
        ) as per_table:
            read = code(
                "Read",
                script='output_data = {"columns": ["c"], "rows": rows}',
                node_key="read",
            )
            transform("Shape", input=read, rules=[], node_key="shape")
        sink_db("Write", input=per_table, table="t", conn_id="warehouse", node_key="sink")
    return p


# -- Emitted IR shape -------------------------------------------------------


def test_group_emits_the_core_contract_shape():
    ir = _flat_group_pipeline().to_json()
    assert ir["ir_version"] == "2.3"

    # Top level: source, group, sink -- the group is an ordinary node here.
    assert [(n["id"], n["type"]) for n in ir["nodes"]] == [
        ("tables", "source_db"),
        ("per_table", "group"),
        ("sink", "sink_db"),
    ]
    # The group's single input comes from the mapped collection, and its
    # output feeds downstream -- exactly the two outer edges.
    assert {(e["from"], e["to"]) for e in ir["edges"]} == {
        ("tables", "per_table"),
        ("per_table", "sink"),
    }

    grp = next(n for n in ir["nodes"] if n["type"] == "group")
    # collect is a top-level node field; the map block stays in config, the
    # same block every mapped node uses.
    assert grp["collect"] == "union"
    assert grp["config"] == {
        "map": {"over": "tables", "key": "table_name", "on_item_failure": "continue"}
    }
    assert grp["capabilities"] == ["compute", "dataset-output"]
    # The subgraph: read -> shape, read the entry (no inner input), shape the
    # terminal (no inner output).
    assert [(n["id"], n["type"]) for n in grp["nodes"]] == [
        ("read", "code"),
        ("shape", "transform"),
    ]
    assert [(e["from"], e["to"]) for e in grp["edges"]] == [("read", "shape")]


def test_inner_nodes_do_not_leak_to_the_top_level():
    ir = _flat_group_pipeline().to_json()
    top_ids = {n["id"] for n in ir["nodes"]}
    assert "read" not in top_ids and "shape" not in top_ids
    # The inner edge is not a top-level edge either.
    assert ("read", "shape") not in {(e["from"], e["to"]) for e in ir["edges"]}


@pytest.mark.parametrize("mode", ["union", "none", "list"])
def test_every_collect_mode_round_trips(mode):
    with Pipeline("p") as p:
        t = source_db("T", query="SELECT 1", conn_id="w")
        with group("G", map=Map(over=t, key="k"), collect=mode) as g:
            code("C", script='output_data = {"columns": [], "rows": rows}')
        sink_db("S", input=g, table="t", conn_id="w")
    grp = next(n for n in p.to_json()["nodes"] if n["type"] == "group")
    assert grp["collect"] == mode


def test_collect_defaults_to_union():
    with Pipeline("p") as p:
        t = source_db("T", query="SELECT 1", conn_id="w")
        with group("G", map=Map(over=t, key="k")) as g:
            code("C", script='output_data = {"columns": [], "rows": rows}')
        sink_db("S", input=g, table="t", conn_id="w")
    grp = next(n for n in p.to_json()["nodes"] if n["type"] == "group")
    assert grp["collect"] == "union"


# -- Nesting ----------------------------------------------------------------


def test_nested_group_nests_in_the_ir():
    with Pipeline("nested") as p:
        tables = source_db("Tables", query="SELECT 1", conn_id="w", node_key="tables")
        with group(
            "Per table", map=Map(over=tables, key="tn"), collect="list", node_key="outer"
        ) as outer:
            parts = source_db("Parts", query="SELECT 1", conn_id="w", node_key="parts")
            with group(
                "Per part", map=Map(over=parts, key="pn"), collect="union", node_key="inner"
            ):
                code("Load", script='output_data = {"columns": [], "rows": rows}', node_key="load")
        sink_db("Sink", input=outer, table="t", conn_id="w")

    ir = p.to_json()
    outer_node = next(n for n in ir["nodes"] if n["id"] == "outer")
    assert outer_node["collect"] == "list"
    # The inner group is a member of the outer group's subgraph.
    inner_node = next(n for n in outer_node["nodes"] if n["id"] == "inner")
    assert inner_node["type"] == "group"
    assert inner_node["collect"] == "union"
    # parts -> inner is an edge of the OUTER subgraph (both are its members).
    assert {(e["from"], e["to"]) for e in outer_node["edges"]} == {("parts", "inner")}
    # The innermost code node is nested one level deeper.
    assert [n["id"] for n in inner_node["nodes"]] == ["load"]


# -- Per-item conditions ----------------------------------------------------


def test_per_item_condition_inside_a_group():
    with Pipeline("cond") as p:
        t = source_db("Tables", query="SELECT 1", conn_id="w", node_key="t")
        with group("Per table", map=Map(over=t, key="tn"), collect="none", node_key="g") as g:
            read = code("Read", script='output_data = {"columns": ["c"], "rows": rows}', node_key="read")
            gate = condition_node("Gate", input=read, expression="row_count > 0", node_key="gate")
            keep = transform("Keep", rules=[], node_key="keep")
            skip = transform("Skip", rules=[], node_key="skip")
            gate.when(keep)
            gate.otherwise(skip)
            union("Both", keep, skip, node_key="both")
        sink_db("Sink", input=g, table="t", conn_id="w")

    ir = p.to_json()
    grp = next(n for n in ir["nodes"] if n["id"] == "g")
    # The conditional routing lives on inner edges leaving the condition node.
    conditional = [e for e in grp["edges"] if "condition" in e]
    assert {(e["from"], e["to"], e["condition"]) for e in conditional} == {
        ("gate", "keep", True),
        ("gate", "skip", False),
    }


# -- Execution-feature gate -------------------------------------------------


def test_group_requires_mapped_groups_not_node_map():
    ir = _flat_group_pipeline().to_json()
    features = required_execution_features(ir)
    assert "mapped-groups" in features
    # A group's map is run by the group executor, not the single-node mapper,
    # so node-map must NOT be required for it.
    assert "node-map" not in features


def test_group_gate_recurses_into_the_subgraph():
    # An inner conditional edge needs conditional-routing even though it is
    # not at the top level.
    with Pipeline("cond") as p:
        t = source_db("T", query="SELECT 1", conn_id="w")
        with group("G", map=Map(over=t, key="k"), collect="none") as g:
            read = code("Read", script='output_data = {"columns": ["c"], "rows": rows}')
            gate = condition_node("Gate", input=read, expression="row_count > 0")
            keep = transform("Keep", rules=[])
            skip = transform("Skip", rules=[])
            gate.when(keep)
            gate.otherwise(skip)
            union("Both", keep, skip)
        sink_db("S", input=g, table="t", conn_id="w")
    features = required_execution_features(p.to_json())
    assert {"mapped-groups", "conditional-routing"} <= features


# -- Local structural validation (fail before deploy) -----------------------


def test_group_outside_pipeline_is_refused():
    t_pipeline = Pipeline("p")
    with t_pipeline:
        src = source_db("T", query="SELECT 1", conn_id="w")
    # Outside the `with`, there is no current pipeline.
    with pytest.raises(PipelineError, match="inside a `with Pipeline"):
        with group("G", map=Map(over=src, key="k")):
            pass


def test_bad_collect_is_refused():
    with Pipeline("p"):
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="collect"):
            with group("G", map=Map(over=t, key="k"), collect="concat"):
                code("C", script="output_data = {}")


def test_missing_map_is_refused():
    with Pipeline("p"):
        with pytest.raises(PipelineError, match="is required: a group that does not map"):
            with group("G", map=None):  # type: ignore[arg-type]
                code("C", script="output_data = {}")


def test_non_map_map_is_refused():
    with Pipeline("p"):
        source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="must be a brokoli.Map"):
            with group("G", map="tables"):  # type: ignore[arg-type]
                code("C", script="output_data = {}")


def test_empty_group_is_refused():
    with Pipeline("p"):
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="no inner nodes"):
            with group("G", map=Map(over=t, key="k")):
                pass


def test_two_terminals_is_refused():
    with Pipeline("p"):
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="exactly one terminal"):
            with group("G", map=Map(over=t, key="k")):
                read = code("Read", script='output_data = {"columns": [], "rows": rows}')
                # Two branches that never reconverge -> two terminals.
                transform("A", input=read, rules=[])
                transform("B", input=read, rules=[])


def test_two_entries_is_refused():
    with Pipeline("p"):
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="exactly one entry"):
            with group("G", map=Map(over=t, key="k")):
                a = code("A", script='output_data = {"columns": [], "rows": rows}')
                b = code("B", script='output_data = {"columns": [], "rows": rows}')
                # Both a and b have no inner input -> two entries.
                union("Join", a, b)


def test_inner_non_group_may_not_map():
    with Pipeline("p"):
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="only a nested group maps"):
            with group("G", map=Map(over=t, key="k")):
                items = source_db("Items", query="SELECT 1", conn_id="w")
                # A plain node carrying its own map block is refused.
                source_db("Rows", query="SELECT 1", conn_id="w", map=Map(over=items, key="id"))


def test_edge_across_a_group_boundary_is_refused():
    with Pipeline("p"):
        outside = source_db("Outside", query="SELECT 1", conn_id="w")
        t = source_db("T", query="SELECT 1", conn_id="w")
        with pytest.raises(PipelineError, match="crosses a group boundary"):
            with group("G", map=Map(over=t, key="k")):
                inner = code("Inner", script='output_data = {"columns": [], "rows": rows}')
                # Wiring an inner node to an outside node is not allowed.
                outside >> inner


def test_failed_group_body_leaves_the_pipeline_unchanged():
    with Pipeline("p") as p:
        src = source_db("Src", query="SELECT 1", conn_id="w", node_key="src")
        before_nodes = dict(p._nodes)
        before_edges = list(p._edges)
        with pytest.raises(PipelineError):
            with group("G", map=Map(over=src, key="k")):
                code("C", script='output_data = {"columns": [], "rows": rows}')
                raise PipelineError("boom from user code")
        # The group node and its inner node are rolled back; src remains.
        assert set(p._nodes) == set(before_nodes)
        assert p._edges == before_edges
        assert p._group_stack == []
        # The pipeline is still usable afterwards.
        sink_db("S", input=src, table="t", conn_id="w")
    assert p.to_json()["ir_version"] == "2.0"


def test_group_without_any_group_stays_pre_2_3():
    # A pipeline that never opens a group must compile exactly as before.
    with Pipeline("p") as p:
        src = source_db("Src", query="SELECT 1", conn_id="w")
        src >> sink_db("Sink", table="t", conn_id="w")
    assert p.to_json()["ir_version"] == "2.0"
