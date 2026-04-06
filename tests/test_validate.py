"""Tests for pipeline validation."""

import pytest
from brokoli import Pipeline, task, condition
from brokoli import source_db, source_api, source_file
from brokoli import transform, join, quality_check
from brokoli import sink_db, sink_file, sink_api
from brokoli.validate import validate_pipeline


class TestValidateSourceDB:
    def test_missing_query(self):
        with Pipeline("test") as p:
            source_db("DB", conn_id="pg")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("query" in e.message.lower() for e in vr.errors)

    def test_missing_conn_and_uri(self):
        with Pipeline("test") as p:
            source_db("DB", query="SELECT 1")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("conn_id" in e.message.lower() or "uri" in e.message.lower() for e in vr.errors)

    def test_valid_with_conn_id(self):
        with Pipeline("test") as p:
            source_db("DB", query="SELECT 1", conn_id="pg")
        vr = validate_pipeline(p)
        assert vr.valid

    def test_valid_with_uri(self):
        with Pipeline("test") as p:
            source_db("DB", query="SELECT 1", uri="postgres://localhost/db")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateSourceAPI:
    def test_missing_url(self):
        with Pipeline("test") as p:
            source_api("API")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("url" in e.message.lower() for e in vr.errors)

    def test_valid(self):
        with Pipeline("test") as p:
            source_api("API", url="https://api.example.com")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateSourceFile:
    def test_missing_path(self):
        with Pipeline("test") as p:
            source_file("CSV")
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid(self):
        with Pipeline("test") as p:
            source_file("CSV", path="/data/input.csv")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateSinkDB:
    def test_missing_table(self):
        with Pipeline("test") as p:
            sink_db("Out", conn_id="pg")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("table" in e.message.lower() for e in vr.errors)

    def test_missing_conn_and_uri(self):
        with Pipeline("test") as p:
            sink_db("Out", table="output")
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid(self):
        with Pipeline("test") as p:
            sink_db("Out", table="output", conn_id="pg")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateSinkFile:
    def test_missing_path(self):
        with Pipeline("test") as p:
            sink_file("Out")
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid(self):
        with Pipeline("test") as p:
            sink_file("Out", path="/out.csv")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateSinkAPI:
    def test_missing_url(self):
        with Pipeline("test") as p:
            sink_api("Hook")
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid(self):
        with Pipeline("test") as p:
            sink_api("Hook", url="https://hooks.slack.com")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateQualityCheck:
    def test_missing_rules(self):
        with Pipeline("test") as p:
            quality_check("QC")
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid(self):
        with Pipeline("test") as p:
            quality_check("QC", rules=["not_null(id)"])
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateCodeNode:
    def test_empty_task(self):
        """A @task with no code should fail."""
        with Pipeline("test") as p:
            # Manually create a code node with no script
            from brokoli.pipeline import _make_id
            nid = _make_id("empty")
            p._add_node(nid, "code", "Empty Task", {"language": "python", "script": ""})
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("script" in e.message.lower() for e in vr.errors)


class TestValidateCondition:
    def test_missing_expression(self):
        with Pipeline("test") as p:
            from brokoli.pipeline import _make_id
            nid = _make_id("cond")
            p._add_node(nid, "condition", "Empty Cond", {})
        vr = validate_pipeline(p)
        assert not vr.valid

    def test_valid_with_expression(self):
        with Pipeline("test") as p:
            from brokoli.pipeline import _make_id
            nid = _make_id("cond")
            p._add_node(nid, "condition", "Check", {"expression": "row_count > 0"})
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateJoin:
    def test_missing_keys(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1", conn_id="pg")
            b = source_db("B", query="SELECT 2", conn_id="pg")
            join("J", a, b)
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("key" in e.message.lower() for e in vr.errors)


class TestValidatePipelineLevel:
    def test_empty_pipeline(self):
        with Pipeline("empty") as p:
            pass
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("at least one node" in e.message.lower() for e in vr.errors)

    def test_disconnected_node_warning(self):
        with Pipeline("test") as p:
            source_db("A", query="SELECT 1", conn_id="pg")
            source_db("B", query="SELECT 2", conn_id="pg")
        vr = validate_pipeline(p)
        assert vr.valid  # warnings don't block
        assert len(vr.warnings) >= 2  # both nodes are disconnected


class TestValidateEdges:
    def test_valid_edges(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1", conn_id="pg")
            b = sink_db("B", a, table="out", conn_id="pg")
        vr = validate_pipeline(p)
        assert vr.valid


class TestValidateFullPipeline:
    def test_complete_valid_pipeline(self):
        with Pipeline("revenue", schedule="0 6 * * *", sla="07:30 UTC") as p:
            orders = source_db("Orders", query="SELECT * FROM orders", conn_id="prod")
            cleaned = transform("Clean", orders, rules=["drop_null(id)"])
            checked = quality_check("QC", cleaned, rules=["unique(id)"])
            checked >> sink_db("DWH", table="fact_orders", conn_id="dwh")
        vr = validate_pipeline(p)
        assert vr.valid
        assert len(vr.errors) == 0

    def test_multiple_errors(self):
        with Pipeline("bad") as p:
            source_db("No Query")  # missing query + conn_id
            sink_db("No Table")    # missing table + conn_id
            quality_check("No Rules")  # missing rules
        vr = validate_pipeline(p)
        assert not vr.valid
        assert len(vr.errors) >= 5  # multiple issues
