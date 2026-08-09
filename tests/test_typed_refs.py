"""Tests for typed dataset/collection references and dynamic-expansion
primitives (brokoli-sdk#2): ScalarRef/ArtifactRef/DatasetRef/CollectionRef,
``.expand()``, ``union()``/``.collect(mode="union")``, and
``DatasetRef.map()``/``.filter()``.

Scope reminder: everything here is SDK-side API surface and IR
compilation. None of it schedules dynamic per-item task instances,
combines dataset manifests, or runs partition transforms at run time --
that's backend (physical-planner) work, tracked separately and not yet
started (RFC §11-13).
"""

import pytest

from brokoli import (
    Pipeline, task,
    source_api, source_db, source_file, transform, join, migrate, dbt,
    sink_db, quality_check, code, notify, condition_node,
    union,
    NodeRef, ConditionRef, ScalarRef, ArtifactRef, DatasetRef, CollectionRef,
)
from brokoli.pipeline import _build_union_node
from brokoli.validation import validate_pipeline


# ---------------------------------------------------------------------------
# Typed refs returned from node-building functions
# ---------------------------------------------------------------------------

class TestTypedRefsReturned:
    def test_source_api_dataset_response_returns_dataset_ref(self):
        with Pipeline("test") as p:
            ref = source_api("S", url="https://x")  # default response="dataset"
        assert type(ref) is DatasetRef
        assert isinstance(ref, NodeRef)

    def test_source_api_scalar_response_returns_scalar_ref(self):
        with Pipeline("test") as p:
            ref = source_api("S", url="https://x", response="scalar", value_path="count")
        assert type(ref) is ScalarRef

    def test_source_api_artifact_response_returns_artifact_ref(self):
        with Pipeline("test") as p:
            ref = source_api("S", url="https://x", response="artifact")
        assert type(ref) is ArtifactRef

    def test_source_db_returns_dataset_ref(self):
        with Pipeline("test") as p:
            ref = source_db("S", query="SELECT 1", conn_id="pg")
        assert type(ref) is DatasetRef

    def test_source_file_returns_dataset_ref(self):
        with Pipeline("test") as p:
            ref = source_file("S", path="/data.csv")
        assert type(ref) is DatasetRef

    def test_transform_returns_dataset_ref(self):
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")
            ref = transform("T", input=src, rules=["FILTER x > 0"])
        assert type(ref) is DatasetRef

    def test_join_returns_dataset_ref(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1", conn_id="pg")
            b = source_db("B", query="SELECT 2", conn_id="pg")
            ref = join("J", left=a, right=b, on="id")
        assert type(ref) is DatasetRef

    def test_migrate_returns_dataset_ref(self):
        with Pipeline("test") as p:
            ref = migrate("M", source_uri="a", target_uri="b", query="q", table="t")
        assert type(ref) is DatasetRef

    def test_dbt_returns_dataset_ref(self):
        with Pipeline("test") as p:
            ref = dbt("D", command="run", project_dir="/dbt")
        assert type(ref) is DatasetRef

    def test_typed_refs_still_support_rshift_chaining(self):
        """Typed refs are NodeRef subclasses -- >>, _resolve, and edge
        wiring all keep working exactly as with a plain NodeRef."""
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")
            assert type(src) is DatasetRef
            src >> sink_db("Out", table="t", conn_id="pg")
        data = p.to_json()
        assert len(data["edges"]) == 1

    def test_sinks_and_ambiguous_nodes_remain_plain_node_ref(self):
        """Sinks, quality gates, and generic code nodes are deliberately
        NOT force-fit into one of the four typed refs (see PR description
        for rationale)."""
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")
            assert type(sink_db("Out", input=src, table="t", conn_id="pg")) is NodeRef
            assert type(quality_check("Q", input=src, rules=["not_null(id)"])) is NodeRef
            assert type(code("C", script="x = 1")) is NodeRef
            assert type(notify("N", webhook_url="https://hook")) is NodeRef
            assert type(condition_node("Gate", expression="row_count > 0", input=src)) is ConditionRef


# ---------------------------------------------------------------------------
# @task.expand()
# ---------------------------------------------------------------------------

class TestExpand:
    def test_expand_compiles_to_single_node_with_expansion_policy(self):
        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x", response="dataset").node_id, p
            )

            @task("Parse")
            def parse(rows):
                return rows

            parsed = parse.expand(file=files)

        assert isinstance(parsed, CollectionRef)

        data = p.to_json()
        # 1 source node + 1 expand node -- not N static nodes.
        assert len(data["nodes"]) == 2

        expand_node = next(n for n in data["nodes"] if n["name"] == "Parse")
        assert expand_node["type"] == "code"
        assert "dynamic-expansion" in expand_node["capabilities"]
        assert "collection-output" in expand_node["capabilities"]
        assert expand_node["config"]["expansion"]["over"] == {"file": files.node_id}
        assert "key" not in expand_node["config"]["expansion"]

        # Edge from the collection into the expand node.
        assert {"from": files.node_id, "to": expand_node["id"]} in data["edges"]

    def test_expand_with_key_serializes_reference_not_executable_code(self):
        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x").node_id, p
            )

            def stable_key(item):
                """Use the file path as the stable instance key."""
                return item["path"]

            @task("Parse")
            def parse(rows):
                return rows

            parse.expand(file=files, key=stable_key)

        node = p.to_json()["nodes"][1]
        expansion = node["config"]["expansion"]
        assert expansion["key"]["name"] == "stable_key"
        assert "path" in expansion["key"]["doc"]
        # The key function is never turned into a runnable script.
        assert "stable_key(item)" not in node["config"]["script"]

    def test_expand_requires_at_least_one_kwarg(self):
        with Pipeline("test") as p:
            @task("Parse")
            def parse(rows):
                return rows

            with pytest.raises(ValueError):
                parse.expand()

    def test_expand_rejects_non_collection_ref(self):
        with Pipeline("test") as p:
            not_a_collection = source_db("S", query="SELECT 1", conn_id="pg")

            @task("Parse")
            def parse(rows):
                return rows

            with pytest.raises(TypeError):
                parse.expand(file=not_a_collection)

    def test_expand_validates_via_validate_pipeline(self):
        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x").node_id, p
            )

            @task("Parse")
            def parse(rows):
                return rows

            parse.expand(file=files)

        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]

    def test_expand_reused_call_creates_fresh_node_each_time(self):
        """Unlike the zero-arg auto-call cache on __call__, .expand() is
        an explicit invocation and always registers a new node."""
        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x").node_id, p
            )

            @task("Parse")
            def parse(rows):
                return rows

            parse.expand(file=files)
            parse.expand(file=files)

        code_nodes = [n for n in p.to_json()["nodes"] if n["type"] == "code"]
        assert len(code_nodes) == 2


# ---------------------------------------------------------------------------
# union() / .collect(mode="union")
# ---------------------------------------------------------------------------

class TestUnionAndCollect:
    def test_union_function_compiles_to_union_node(self):
        with Pipeline("test") as p:
            page_a = source_api("Page A", url="https://x/1")
            page_b = source_api("Page B", url="https://x/2")
            page_c = source_api("Page C", url="https://x/3")
            combined = union("Combine Pages", page_a, page_b, page_c)

        assert type(combined) is DatasetRef
        data = p.to_json()
        node = next(n for n in data["nodes"] if n["name"] == "Combine Pages")
        assert node["type"] == "union"
        assert set(node["capabilities"]) == {"compute", "dataset-output"}
        assert node["config"] == {"mode": "union"}

        incoming = {e["from"] for e in data["edges"] if e["to"] == node["id"]}
        assert incoming == {page_a.node_id, page_b.node_id, page_c.node_id}

    def test_collect_on_collection_ref_compiles_to_same_node_shape(self):
        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x").node_id, p
            )
            collected = files.collect(mode="union")

        assert type(collected) is DatasetRef
        data = p.to_json()
        node = next(n for n in data["nodes"] if n["id"] == collected.node_id)
        assert node["type"] == "union"
        assert set(node["capabilities"]) == {"compute", "dataset-output"}
        assert node["config"] == {"mode": "union"}

        incoming = [e for e in data["edges"] if e["to"] == node["id"]]
        assert len(incoming) == 1
        assert incoming[0]["from"] == files.node_id

    def test_union_and_collect_produce_identical_node_shape(self):
        """Acceptance criterion: union(name, *refs) and
        ref.collect(mode='union') both compile to the same IR shape --
        same type/capabilities/config, differing only in edge count."""
        with Pipeline("test") as p1:
            a = source_api("A", url="https://x/1")
            b = source_api("B", url="https://x/2")
            via_union = union("Combine", a, b)
        union_node = next(n for n in p1.to_json()["nodes"] if n["id"] == via_union.node_id)

        with Pipeline("test") as p2:
            files = CollectionRef(source_api("Files", url="https://x").node_id, p2)
            via_collect = files.collect(mode="union")
        collect_node = next(n for n in p2.to_json()["nodes"] if n["id"] == via_collect.node_id)

        assert union_node["type"] == collect_node["type"] == "union"
        assert union_node["capabilities"] == collect_node["capabilities"]
        assert union_node["config"] == collect_node["config"] == {"mode": "union"}

    def test_union_requires_at_least_one_ref(self):
        with Pipeline("test"):
            with pytest.raises(ValueError):
                union("Empty")

    def test_union_node_validates(self):
        with Pipeline("test") as p:
            a = source_api("A", url="https://x/1")
            b = source_api("B", url="https://x/2")
            union("Combine", a, b)
        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]

    def test_direct_union_node_helper_rejects_unsupported_mode(self):
        with Pipeline("test") as p:
            a = source_api("A", url="https://x/1")
            with pytest.raises(ValueError):
                _build_union_node(p, "Bad", [a], mode="intersect")


# ---------------------------------------------------------------------------
# DatasetRef.map() / .filter() vs @map / @filter decorators
# ---------------------------------------------------------------------------

class TestDatasetMapFilter:
    def test_dataset_map_compiles_to_distinct_node_type(self):
        with Pipeline("test") as p:
            data = source_db("S", query="SELECT 1", conn_id="pg")

            def double_amount(row):
                return row

            mapped = data.map(double_amount)

        assert type(mapped) is DatasetRef
        node = p.to_json()["nodes"][-1]
        assert node["type"] == "dataset_map"
        assert node["type"] != "code"
        assert node["config"]["function"]["name"] == "double_amount"

    def test_dataset_filter_compiles_to_distinct_node_type(self):
        with Pipeline("test") as p:
            data = source_db("S", query="SELECT 1", conn_id="pg")

            def is_active(row):
                return row.get("status") == "active"

            filtered = data.filter(is_active)

        assert type(filtered) is DatasetRef
        node = p.to_json()["nodes"][-1]
        assert node["type"] == "dataset_filter"
        assert node["config"]["function"]["name"] == "is_active"

    def test_dataset_map_and_decorator_map_are_distinguishable_in_json(self):
        """The two styles must not look interchangeable in serialized
        output: @map registers a 'code' node with a runnable script;
        DatasetRef.map() registers a 'dataset_map' node with a function
        reference and no script at all."""
        with Pipeline("test") as p:
            from brokoli import map as map_decorator

            data = source_db("S", query="SELECT 1", conn_id="pg")

            @map_decorator("Enrich")
            def enrich(row):
                row["x"] = 1
                return row

            enrich(data)
            data.map(enrich.__wrapped__)

        nodes = p.to_json()["nodes"]
        decorator_node = next(n for n in nodes if n["name"] == "Enrich")
        method_node = next(n for n in nodes if n["type"] == "dataset_map")

        assert decorator_node["type"] == "code"
        assert "script" in decorator_node["config"]
        assert method_node["type"] == "dataset_map"
        assert "script" not in method_node["config"]
        assert decorator_node["type"] != method_node["type"]

    def test_dataset_transform_node_edge_wiring(self):
        with Pipeline("test") as p:
            data = source_db("S", query="SELECT 1", conn_id="pg")
            mapped = data.map(lambda r: r)

        edges = p.to_json()["edges"]
        assert {"from": data.node_id, "to": mapped.node_id} in edges

    def test_dataset_transform_validates(self):
        with Pipeline("test") as p:
            data = source_db("S", query="SELECT 1", conn_id="pg")
            data.map(lambda r: r)
            data.filter(lambda r: True)
        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]

    def test_dataset_transform_missing_function_name_rejected(self):
        with Pipeline("test") as p:
            data = source_db("S", query="SELECT 1", conn_id="pg")
            data.map(lambda r: r)
            p._nodes[p._node_order[-1]]["config"]["function"] = {}
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any(e.field == "function" for e in vr.errors)
