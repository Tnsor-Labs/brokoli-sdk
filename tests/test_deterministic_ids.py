"""Focused tests for deterministic logical node identity."""

import inspect

import pytest

from brokoli import (
    CollectionRef,
    DatasetRef,
    NodeRef,
    Pipeline,
    code,
    condition,
    condition_node,
    dbt,
    filter,
    join,
    map,
    migrate,
    notify,
    parallel,
    quality_check,
    sensor,
    sink,
    sink_api,
    sink_db,
    sink_file,
    source,
    source_api,
    source_db,
    source_file,
    task,
    transform,
    union,
    validate,
)
from brokoli.exceptions import PipelineError
from brokoli.pipeline import _make_id, _MultiRef


def _build_pipeline():
    with Pipeline("Stable") as pipeline:
        raw = source_db("Extract", query="SELECT 1")
        clean = transform("Clean Data", raw)
        sink_db("Load", clean, table="out")
    return pipeline


def test_identical_pipelines_have_identical_ids_edges_and_serialization():
    first = _build_pipeline().to_json()
    second = _build_pipeline().to_json()

    assert [node["id"] for node in first["nodes"]] == [
        "extract_1",
        "clean_data_1",
        "load_1",
    ]
    assert first["nodes"] == second["nodes"]
    assert first["edges"] == second["edges"]


def test_canonical_helper_and_per_base_literal_counters():
    assert _make_id("A Display Name!") == "a_display_name"
    assert _make_id("x" * 30) == "x" * 20
    assert _make_id("!!!") == "node"

    with Pipeline("Counters"):
        first = source_db("Extract")
        unrelated = source_db("Other")
        second = source_db("Extract")

    assert first.node_id == "extract_1"
    assert unrelated.node_id == "other_1"
    assert second.node_id == "extract_2"


def test_counters_are_pipeline_local():
    with Pipeline("First"):
        first = source_db("Extract")
    with Pipeline("Second"):
        second = source_db("Extract")
    assert first.node_id == second.node_id == "extract_1"


def test_explicit_key_wins_is_not_normalized_and_does_not_consume_counter():
    with Pipeline("Keys"):
        explicit = source_db("Old Display", node_key="stable-source")
        generated = source_db("Old Display")

    with Pipeline("Renamed"):
        renamed = source_db("New Display", node_key="stable-source")

    assert explicit.node_id == "stable-source"
    assert renamed.node_id == explicit.node_id
    assert generated.node_id == "old_display_1"


@pytest.mark.parametrize(
    "node_key",
    ["", "Upper", "1starts-wrong", "has space", "a.b", "a" * 65, 123],
)
def test_invalid_explicit_keys_raise_pipeline_error(node_key):
    with Pipeline("Invalid") as pipeline:
        with pytest.raises(PipelineError, match="node_key must match"):
            source_db("Source", node_key=node_key)
    assert pipeline._nodes == {}


def test_duplicate_explicit_and_generated_collisions_raise_or_skip():
    with Pipeline("Collisions"):
        source_db("First", node_key="shared")
        with pytest.raises(PipelineError, match="Duplicate node id"):
            source_db("Second", node_key="shared")

    with Pipeline("Generated"):
        source_db("Reserved", node_key="extract_1")
        second = source_db("Extract")
        third = source_db("Extract")
    assert second.node_id == "extract_2"
    assert third.node_id == "extract_3"


def test_all_builtins_accept_exact_node_keys():
    with Pipeline("Builtins"):
        left = source_db("DB", node_key="db")
        right = source_api("API", node_key="api")
        file_ref = source_file("File", node_key="file")
        transformed = transform("Transform", left, node_key="transform")
        joined = join("Join", left, right, node_key="join")
        quality_check("Quality", joined, node_key="quality")
        code("Code", transformed, node_key="code")
        sink_db("Sink DB", file_ref, node_key="sink-db")
        sink_file("Sink File", file_ref, node_key="sink-file")
        sink_api("Sink API", file_ref, node_key="sink-api")
        migrate("Migrate", node_key="migrate")
        dbt("dbt", node_key="dbt")
        notify("Notify", node_key="notify")
        condition_node("Gate", input=left, expression="true", node_key="gate")


def test_dataset_methods_union_and_collect_accept_node_keys():
    with Pipeline("Typed") as pipeline:
        dataset = source_api("Rows")
        mapped = dataset.map(lambda row: row, node_key="mapped")
        filtered = dataset.filter(lambda row: True, node_key="filtered")
        combined = union("Combined", mapped, filtered, node_key="combined")
        collection = CollectionRef(dataset.node_id, pipeline)
        collected = collection.collect(node_key="collected")

    assert [mapped.node_id, filtered.node_id, combined.node_id, collected.node_id] == [
        "mapped",
        "filtered",
        "combined",
        "collected",
    ]


def test_all_decorator_families_accept_defaults_and_call_overrides():
    with Pipeline("Decorators") as pipeline:
        upstream = source_db("Input")

        @source(node_key="source-default")
        def custom_source():
            return []

        @task(node_key="task-default")
        def custom_task(rows):
            return rows

        @sink(node_key="sink-default")
        def custom_sink(rows):
            pass

        @filter(node_key="filter-default")
        def custom_filter(row):
            return True

        @map(node_key="map-default")
        def custom_map(row):
            return row

        @validate(node_key="validate-default")
        def custom_validate(rows):
            return True

        @sensor(node_key="sensor-default")
        def custom_sensor():
            return True

        refs = [
            custom_source(node_key="source-call"),
            custom_task(upstream, node_key="task-call"),
            custom_sink(upstream, node_key="sink-call"),
            custom_filter(upstream, node_key="filter-call"),
            custom_map(upstream, node_key="map-call"),
            custom_validate(upstream, node_key="validate-call"),
            custom_sensor(upstream, node_key="sensor-call"),
        ]
        defaults = [
            custom_source(),
            custom_task(upstream),
            custom_sink(upstream),
            custom_filter(upstream),
            custom_map(upstream),
            custom_validate(upstream),
            custom_sensor(upstream),
        ]

        @condition(node_key="condition-default")
        def unsupported_condition(rows):
            return True

    assert [ref.node_id for ref in refs] == [
        "source-call",
        "task-call",
        "sink-call",
        "filter-call",
        "map-call",
        "validate-call",
        "sensor-call",
    ]
    assert [ref.node_id for ref in defaults] == [
        "source-default",
        "task-default",
        "sink-default",
        "filter-default",
        "map-default",
        "validate-default",
        "sensor-default",
    ]
    assert "node_key" in inspect.signature(
        unsupported_condition, follow_wrapped=False
    ).parameters
    assert len(pipeline._nodes) == 15


def test_wrapper_cache_and_repeated_explicit_default_behavior():
    with Pipeline("Wrappers") as pipeline:
        upstream = source_db("Input")

        @task(node_key="cached-task")
        def cached(rows):
            return rows

        first = cached()
        assert cached() is first
        assert len(pipeline._nodes) == 2

        @task(node_key="one-task")
        def repeated(rows):
            return rows

        repeated(upstream)
        with pytest.raises(PipelineError, match="Duplicate node id"):
            repeated(upstream)


def test_expand_node_key_is_separate_from_per_item_key_callable():
    with Pipeline("Expand") as pipeline:
        source_ref = source_api("Files")
        files = CollectionRef(source_ref.node_id, pipeline)

        def item_key(item):
            return item["path"]

        @task(node_key="parse-default")
        def parse(rows):
            return rows

        expanded = parse.expand(
            key=item_key, node_key="parse-expanded", file=files
        )

    node = pipeline._nodes[expanded.node_id]
    assert expanded.node_id == "parse-expanded"
    assert node["config"]["expansion"]["key"]["name"] == "item_key"


def test_expand_preserves_node_key_as_a_task_parameter_name():
    with Pipeline("Expand compatibility") as pipeline:
        first_source = source_api("First")
        second_source = source_api("Second")
        first = CollectionRef(first_source.node_id, pipeline)
        second = CollectionRef(second_source.node_id, pipeline)

        @task(node_key="decorator-default")
        def parse(rows):
            return rows

        expanded = parse.expand(node_key=first, other=second)

    node = pipeline._nodes[expanded.node_id]
    assert expanded.node_id == "decorator-default"
    assert node["config"]["expansion"]["over"] == {
        "node_key": first.node_id,
        "other": second.node_id,
    }


def test_expand_string_node_key_remains_logical_and_key_remains_per_item():
    with Pipeline("Expand logical key") as pipeline:
        source_ref = source_api("Files")
        files = CollectionRef(source_ref.node_id, pipeline)

        def item_key(item):
            return item["id"]

        @task
        def parse(rows):
            return rows

        expanded = parse.expand(
            key=item_key, node_key="logical-expand", item=files
        )

    expansion = pipeline._nodes[expanded.node_id]["config"]["expansion"]
    assert expanded.node_id == "logical-expand"
    assert expansion["over"] == {"item": files.node_id}
    assert expansion["key"]["name"] == "item_key"


def test_cross_pipeline_node_production_fails_before_allocation():
    with Pipeline("First"):
        foreign = source_db("Foreign")

    with Pipeline("Second") as pipeline:
        with pytest.raises(PipelineError, match="different pipelines"):
            transform("Local", foreign)
        local = transform("Local")
    assert local.node_id == "local_1"
    assert len(pipeline._nodes) == 1


def test_cross_pipeline_chaining_fails_before_lazy_wrapper_creation():
    with Pipeline("First"):
        foreign = source_db("Foreign")

    with Pipeline("Second") as pipeline:
        @task("Lazy")
        def lazy(rows):
            return rows

        with pytest.raises(PipelineError, match="different pipelines"):
            foreign >> lazy

    assert pipeline._nodes == {}
    assert lazy._auto_ref is None


def test_parallel_and_multiref_reject_foreign_refs_with_identical_ids():
    with Pipeline("Foreign"):
        foreign = source_db("Same")

    with Pipeline("Local") as pipeline:
        local = source_db("Same")
        assert local.node_id == foreign.node_id == "same_1"

        with pytest.raises(PipelineError, match="different pipelines"):
            parallel(local, foreign)
        with pytest.raises(PipelineError, match="different pipelines"):
            _MultiRef([local, foreign], pipeline)

    assert len(pipeline._nodes) == 1


def _graph_state(pipeline):
    return (
        dict(pipeline._nodes),
        list(pipeline._edges),
        list(pipeline._node_order),
        {
            node_id: {
                branch: list(destinations)
                for branch, destinations in branch_map.items()
            }
            for node_id, branch_map in pipeline._branches.items()
        },
        dict(pipeline._node_id_counters),
    )


def test_list_fanout_is_atomic_for_later_foreign_target():
    with Pipeline("Foreign"):
        foreign = source_db("Target")

    with Pipeline("Local") as pipeline:
        start = source_db("Start")

        @task("Lazy")
        def lazy(rows):
            return rows

        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="different pipelines"):
            start >> [lazy, foreign]
        assert _graph_state(pipeline) == before
        assert lazy._auto_ref is None
        later = code("Lazy")

    assert later.node_id == "lazy_1"


def test_multiref_list_operation_is_atomic_for_later_foreign_target():
    with Pipeline("Foreign"):
        foreign = source_db("Target")

    with Pipeline("Local") as pipeline:
        first = source_db("First")
        second = source_db("Second")
        grouped = parallel(first, second)

        @task("Lazy")
        def lazy(rows):
            return rows

        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="different pipelines"):
            grouped >> [lazy, foreign]
        assert _graph_state(pipeline) == before
        assert lazy._auto_ref is None
        later = code("Lazy")

    assert later.node_id == "lazy_1"


def test_multiref_fanin_rolls_back_earlier_edge_on_later_conflict():
    with Pipeline("Atomic fanin") as pipeline:
        first = source_db("First")
        second = source_db("Second")
        target = code("Target")
        grouped = parallel(first, second)
        pipeline._add_edge(second.node_id, target.node_id, condition=True)

        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="multiple branches"):
            grouped >> target

    assert _graph_state(pipeline) == before


def test_missing_synthetic_ref_rejected_before_builtin_allocation():
    with Pipeline("Missing built-in") as pipeline:
        missing = NodeRef("missing", pipeline)
        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="does not exist"):
            transform("Produced", missing)
        assert _graph_state(pipeline) == before
        produced = transform("Produced")
    assert produced.node_id == "produced_1"


def test_missing_synthetic_ref_rejected_before_wrapper_allocation():
    with Pipeline("Missing wrapper") as pipeline:
        missing = NodeRef("missing", pipeline)

        @task("Produced")
        def produced_task(rows):
            return rows

        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="does not exist"):
            produced_task(missing)
        assert _graph_state(pipeline) == before
        produced = code("Produced")
    assert produced.node_id == "produced_1"


def test_missing_synthetic_refs_rejected_by_typed_union_and_expand_paths():
    with Pipeline("Missing typed") as pipeline:
        valid = source_api("Valid")
        missing_dataset = DatasetRef("missing-dataset", pipeline)
        missing_collection = CollectionRef("missing-collection", pipeline)

        @task("Expand")
        def expand_task(rows):
            return rows

        before = _graph_state(pipeline)
        with pytest.raises(PipelineError, match="does not exist"):
            missing_dataset.map(lambda row: row)
        with pytest.raises(PipelineError, match="does not exist"):
            missing_collection.collect()
        with pytest.raises(PipelineError, match="does not exist"):
            union("Union", valid, missing_dataset)
        with pytest.raises(PipelineError, match="does not exist"):
            expand_task.expand(item=missing_collection)
        assert _graph_state(pipeline) == before

        mapped = DatasetRef(valid.node_id, pipeline).map(
            lambda row: row, name="dataset_map(fn)"
        )

    assert mapped.node_id == "dataset_mapfn_1"


def test_failed_lazy_conditional_branch_restores_allocator_counters():
    with Pipeline("Conditional") as pipeline:
        source_ref = source_db("Input")
        gate = condition_node("Gate", "true", source_ref)
        nested = condition_node("Nested", "false", source_ref)

        @task("Lazy")
        def lazy(rows):
            return rows

        with pytest.raises(PipelineError, match="Nested conditional routing"):
            gate.when([lazy, nested])

        later = code("Lazy")

    assert lazy._auto_ref is None
    assert later.node_id == "lazy_1"
    assert [node["name"] for node in pipeline._nodes.values()].count("Lazy") == 1
