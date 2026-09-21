import pytest

from brokoli import Pipeline, column, dataset_schema, join, project, source_api
from brokoli.exceptions import PipelineError


def test_dataset_schema_builds_ordered_bptd_columns():
    declared = dataset_schema(
        {
            "id": {"kind": "int64"},
            "name": {"kind": "string", "nullable": True},
        },
        additional_columns="closed",
    )

    assert declared == {
        "contract": "brokoli.dataset-schema/v1",
        "columns": [
            {"name": "id", "type": {"kind": "int64"}},
            {"name": "name", "type": {"kind": "string", "nullable": True}},
        ],
        "additional_columns": "closed",
    }


def test_source_api_emits_dataset_schema():
    declared = dataset_schema({"id": {"kind": "int64"}})
    with Pipeline("schema") as p:
        source = source_api("Fetch", url="https://example.test", schema=declared)

    assert p._nodes[source.node_id]["config"]["schema"] == declared


def test_source_api_rejects_schema_for_non_dataset_response():
    with pytest.raises(PipelineError, match="response='dataset'"):
        source_api(
            "Fetch",
            url="https://example.test",
            response="scalar",
            value_path="count",
            schema=dataset_schema({"id": {"kind": "int64"}}),
        )


def test_join_emits_derived_alias_schema_when_inputs_are_declared():
    left_schema = dataset_schema({"id": {"kind": "int64"}, "name": {"kind": "string"}}, "closed")
    right_schema = dataset_schema({"id": {"kind": "int64"}, "name": {"kind": "string"}}, "closed")
    with Pipeline("join-schema") as p:
        left = source_api("Left", url="https://example.test/left", schema=left_schema)
        right = source_api("Right", url="https://example.test/right", schema=right_schema)
        merged = join("Merge", left, right, on="id", collision_policy="alias", right_alias="right_row")

    assert p._nodes[merged.node_id]["config"]["schema"] == dataset_schema(
        {"id": {"kind": "int64"}, "name": {"kind": "string"}, "right_row_name": {"kind": "string"}},
        "closed",
    )


def test_join_rejects_incompatible_declared_key_types():
    with Pipeline("join-schema") as p:
        left = source_api("Left", url="https://example.test/left", schema=dataset_schema({"id": {"kind": "int64"}}))
        right = source_api("Right", url="https://example.test/right", schema=dataset_schema({"id": {"kind": "string"}}))
        with pytest.raises(PipelineError, match="join keys"):
            join("Merge", left, right, on="id")


def test_join_preserves_decimal_precision_in_derived_schema():
    left_schema = dataset_schema({"id": {"kind": "int64"}, "amount": {"kind": "decimal", "precision": 20, "scale": 4}})
    right_schema = dataset_schema({"id": {"kind": "int64"}, "label": {"kind": "string"}})
    with Pipeline("join-decimal-schema") as p:
        left = source_api("Left", url="https://example.test/left", schema=left_schema)
        right = source_api("Right", url="https://example.test/right", schema=right_schema)
        merged = join("Merge", left, right, on="id")

    amount = next(column for column in p._nodes[merged.node_id]["config"]["schema"]["columns"] if column["name"] == "amount")
    assert amount["type"] == {"kind": "decimal", "precision": 20, "scale": 4}


def test_project_preserves_referenced_field_schema():
    source_schema = dataset_schema({"amount": {"kind": "decimal", "precision": 20, "scale": 4}})
    with Pipeline("project-decimal-schema") as p:
        source = source_api("Source", url="https://example.test/source", schema=source_schema)
        shaped = project("Project", source, {"total": column("amount")})

    assert p._nodes[shaped.node_id]["config"]["schema"]["columns"] == [{
        "name": "total", "type": {"kind": "decimal", "precision": 20, "scale": 4},
    }]


def test_project_rejects_missing_declared_field():
    source_schema = dataset_schema({"amount": {"kind": "decimal", "precision": 20, "scale": 4}})
    with Pipeline("project-missing-field") as p:
        source = source_api("Source", url="https://example.test/source", schema=source_schema)
        with pytest.raises(PipelineError, match="missing field"):
            project("Project", source, {"total": column("missing")})


@pytest.mark.parametrize(
    "columns, additional_columns, message",
    [
        ({"id": {"kind": "not-a-type"}}, "unknown", "BPTD kind"),
        ({"id": {"kind": "int64"}}, "maybe", "additional_columns"),
        ({"": {"kind": "int64"}}, "unknown", "column name"),
    ],
)
def test_dataset_schema_rejects_invalid_declarations(columns, additional_columns, message):
    with pytest.raises(PipelineError, match=message):
        dataset_schema(columns, additional_columns=additional_columns)
