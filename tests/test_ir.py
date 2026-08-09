"""Tests for normalized pipeline IR comparison artifacts."""

import copy
import json

import pytest

from brokoli import Pipeline, canonical_json, diff_ir, normalize_ir, source_db


def test_normalization_ignores_server_metadata_layout_and_unordered_values():
    left = {
        "id": "server-id",
        "workspace_id": "workspace",
        "name": "café",
        "nodes": [
            {"id": "b", "position": {"x": 1}, "capabilities": ["sink", "compute"]},
            {"id": "a", "position": {"x": 2}, "capabilities": ["source"]},
        ],
        "edges": [
            {"from": "b", "to": "c"},
            {"from": "a", "to": "b", "condition": False},
        ],
        "tags": ["z", "a"],
        "depends_on": ["two", "one"],
    }
    right = {
        "name": "café",
        "nodes": list(reversed(left["nodes"])),
        "edges": copy.deepcopy(left["edges"]),
        "tags": ["a", "z"],
        "depends_on": ["one", "two"],
    }

    assert normalize_ir(left) == normalize_ir(right)
    normalized = normalize_ir(left)
    assert normalized["edges"][1]["condition"] is False
    assert normalized["nodes"][1]["capabilities"] == ["compute", "sink"]


def test_edge_order_is_semantic_and_false_condition_is_preserved():
    first = {
        "nodes": [],
        "edges": [
            {"from": "left", "to": "join"},
            {"from": "right", "to": "join", "condition": False},
        ],
    }
    reversed_edges = {**first, "edges": list(reversed(first["edges"]))}

    assert normalize_ir(first)["edges"][1]["condition"] is False
    assert diff_ir(first, reversed_edges)


def test_normalization_is_deep_copy_and_preserves_semantic_and_unknown_fields():
    original = {
        "pipeline_id": "orders",
        "enabled": False,
        "schedule": "0 2 * * *",
        "sla_deadline": "03:00",
        "ir_version": "2.1",
        "nodes": [{"id": "a", "config": {"rules": ["second", "first"]}}],
        "edges": [],
        "hooks": {"on_success": {"script": "pass"}},
        "future": {"ordered": [2, 1]},
    }
    before = copy.deepcopy(original)
    normalized = normalize_ir(original)

    normalized["nodes"][0]["config"]["rules"].reverse()
    assert original == before
    assert normalized["pipeline_id"] == "orders"
    assert normalized["enabled"] is False
    assert normalized["future"] == {"ordered": [2, 1]}
    assert normalize_ir(original)["nodes"][0]["config"]["rules"] == ["second", "first"]
    assert normalize_ir(original)["hooks"] == original["hooks"]


def test_empty_server_defaults_and_generated_webhook_secret_are_equivalent():
    minimal = {"name": "orders"}
    server = {
        "name": "orders",
        "nodes": None,
        "edges": [],
        "tags": None,
        "depends_on": [],
        "params": None,
        "hooks": {},
        "dependency_rules": None,
        "webhook_url": "",
        "schedule_timezone": "UTC",
    }
    assert normalize_ir(minimal) == normalize_ir(server)
    assert normalize_ir({**minimal, "webhook_token": "generated"})[
        "webhook_token"
    ] == ""
    assert "webhook_token" not in normalize_ir(minimal)


def test_canonical_rendering_is_stable_unicode_json_with_one_newline():
    rendered = canonical_json({"z": "café", "a": 1})
    assert rendered == '{\n  "a": 1,\n  "z": "café"\n}\n'
    assert rendered.endswith("\n") and not rendered.endswith("\n\n")
    assert json.loads(rendered) == {"a": 1, "z": "café"}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_rendering_rejects_non_finite_numbers(value):
    with pytest.raises(ValueError, match="Out of range float values"):
        canonical_json({"value": value})


def test_missing_null_and_empty_capabilities_use_pipeline_defaults():
    absent = {
        "nodes": [{"id": "source", "type": "source_db"}],
        "edges": [],
    }
    null = copy.deepcopy(absent)
    null["nodes"][0]["capabilities"] = None
    empty = copy.deepcopy(absent)
    empty["nodes"][0]["capabilities"] = []
    explicit_default = copy.deepcopy(absent)
    explicit_default["nodes"][0]["capabilities"] = [
        "source",
        "dataset-output",
    ]

    assert (
        normalize_ir(absent)
        == normalize_ir(null)
        == normalize_ir(empty)
        == normalize_ir(explicit_default)
    )
    assert normalize_ir(absent)["nodes"][0]["capabilities"] == [
        "dataset-output",
        "source",
    ]


def test_explicit_non_default_capabilities_remain_semantic():
    absent = {
        "nodes": [{"id": "source", "type": "source_db"}],
        "edges": [],
    }
    explicit = copy.deepcopy(absent)
    explicit["nodes"][0]["capabilities"] = ["compute"]

    assert diff_ir(absent, explicit)


def test_diff_keeps_config_rule_order_and_semantic_fields_visible():
    remote = {
        "pipeline_id": "p",
        "enabled": True,
        "nodes": [{"id": "n", "config": {"rules": ["a", "b"]}}],
    }
    local = copy.deepcopy(remote)
    local["enabled"] = False
    local["nodes"][0]["config"]["rules"] = ["b", "a"]

    difference = diff_ir(local, remote)
    assert difference
    assert '"enabled": false' in difference
    assert '"rules"' in difference


def test_pipeline_normalized_convenience_does_not_mutate_internal_config():
    with Pipeline("orders") as pipeline:
        source = source_db("Source", query="SELECT 1", conn_id="warehouse")

    internal_before = copy.deepcopy(pipeline._nodes[source.node_id]["config"])
    snapshot = pipeline.to_normalized_json()

    assert pipeline._nodes[source.node_id]["config"] == internal_before
    assert "_schema_hint" not in pipeline._nodes[source.node_id]["config"]
    assert snapshot["nodes"][0]["config"]["_schema_hint"] == "query_result"
    assert "position" not in snapshot["nodes"][0]
