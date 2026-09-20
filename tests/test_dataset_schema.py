import pytest

from brokoli import Pipeline, dataset_schema, source_api
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
