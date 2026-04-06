"""Tests for all decorators: @source, @sink, @filter, @map, @validate, @sensor."""

import json
import pytest
from brokoli import (
    Pipeline, source_db, sink_db, sink_file,
    source, sink, filter, map, validate, sensor,
)


class TestSourceDecorator:
    def test_basic(self):
        with Pipeline("test") as p:
            @source
            def fetch_data():
                return [{"id": 1}]

            node = fetch_data()

        data = p.to_json()
        assert len(data["nodes"]) == 1
        assert data["nodes"][0]["type"] == "code"
        assert "def fetch_data" in data["nodes"][0]["config"]["script"]
        assert "_source_result = fetch_data()" in data["nodes"][0]["config"]["script"]

    def test_with_name(self):
        with Pipeline("test") as p:
            @source("Stripe Charges", timeout=60)
            def fetch_charges():
                return []

            fetch_charges()

        node = p.to_json()["nodes"][0]
        assert node["name"] == "Stripe Charges"
        assert node["config"]["timeout"] == 60

    def test_rshift_chaining(self):
        with Pipeline("test") as p:
            @source
            def fetch():
                return [{"x": 1}]

            s = sink_file("Out", path="/tmp/out.csv")
            fetch >> s

        data = p.to_json()
        assert len(data["edges"]) == 1


class TestSinkDecorator:
    def test_basic(self):
        with Pipeline("test") as p:
            src = source_db("In", query="SELECT 1", conn_id="pg")

            @sink
            def write_to_api(rows):
                pass  # send somewhere

            src >> write_to_api

        data = p.to_json()
        assert len(data["nodes"]) == 2
        code_node = next(n for n in data["nodes"] if n["type"] == "code")
        assert "def write_to_api" in code_node["config"]["script"]
        assert "write_to_api(rows)" in code_node["config"]["script"]

    def test_with_name(self):
        with Pipeline("test") as p:
            @sink("Push to HubSpot", retries=3)
            def push(rows):
                pass

            src = source_db("In", query="SELECT 1", conn_id="pg")
            src >> push

        node = next(n for n in p.to_json()["nodes"] if n["type"] == "code")
        assert node["name"] == "Push to HubSpot"
        assert node["config"]["max_retries"] == 3


class TestFilterDecorator:
    def test_basic(self):
        with Pipeline("test") as p:
            src = source_db("In", query="SELECT 1", conn_id="pg")

            @filter
            def active_only(row):
                return row["status"] == "active"

            out = sink_db("Out", table="t", conn_id="pg")
            src >> active_only >> out

        data = p.to_json()
        assert len(data["nodes"]) == 3
        code_node = next(n for n in data["nodes"] if n["type"] == "code")
        assert "def active_only" in code_node["config"]["script"]
        assert "[r for r in rows if active_only(r)]" in code_node["config"]["script"]

    def test_with_name(self):
        with Pipeline("test") as p:
            @filter("Active Users Only")
            def active(row):
                return row.get("active")

            active()

        assert p.to_json()["nodes"][0]["name"] == "Active Users Only"


class TestMapDecorator:
    def test_basic(self):
        with Pipeline("test") as p:
            src = source_db("In", query="SELECT 1", conn_id="pg")

            @map
            def enrich(row):
                row["domain"] = row["email"].split("@")[1]
                return row

            out = sink_db("Out", table="t", conn_id="pg")
            src >> enrich >> out

        data = p.to_json()
        code_node = next(n for n in data["nodes"] if n["type"] == "code")
        assert "def enrich" in code_node["config"]["script"]
        assert "[enrich(r) for r in rows]" in code_node["config"]["script"]

    def test_with_name(self):
        with Pipeline("test") as p:
            @map("Add Domain")
            def add_domain(row):
                row["d"] = "test"
                return row

            add_domain()

        assert p.to_json()["nodes"][0]["name"] == "Add Domain"


class TestValidateDecorator:
    def test_basic_block(self):
        with Pipeline("test") as p:
            src = source_db("In", query="SELECT 1", conn_id="pg")

            @validate("Revenue check")
            def check_revenue(rows):
                total = sum(r.get("amount", 0) for r in rows)
                return total > 0, f"Total: {total}"

            out = sink_db("Out", table="t", conn_id="pg")
            src >> check_revenue >> out

        data = p.to_json()
        code_node = next(n for n in data["nodes"] if n["type"] == "code")
        assert "def check_revenue" in code_node["config"]["script"]
        assert "#VALIDATION_FAILED" in code_node["config"]["script"]
        assert "raise ValueError" in code_node["config"]["script"]

    def test_warn_mode(self):
        with Pipeline("test") as p:
            @validate(on_failure="warn")
            def soft_check(rows):
                return len(rows) > 0

            soft_check()

        code_node = p.to_json()["nodes"][0]
        assert "#VALIDATION_FAILED" in code_node["config"]["script"]
        # warn mode should NOT raise
        assert "raise ValueError" not in code_node["config"]["script"]


class TestSensorDecorator:
    def test_basic(self):
        with Pipeline("test") as p:
            @sensor(poll_interval=30, timeout=600)
            def wait_for_file():
                return True

            out = sink_db("Process", table="t", conn_id="pg")
            wait_for_file >> out

        data = p.to_json()
        code_node = next(n for n in data["nodes"] if n["type"] == "code")
        assert "def wait_for_file" in code_node["config"]["script"]
        assert "time.sleep(_poll_interval)" in code_node["config"]["script"]
        assert "_poll_interval = 30" in code_node["config"]["script"]
        assert "_timeout = 600" in code_node["config"]["script"]
        # Timeout on the node should be sensor timeout + buffer
        assert code_node["config"]["timeout"] == 660

    def test_with_name(self):
        with Pipeline("test") as p:
            @sensor("Wait for API", poll_interval=10, timeout=120)
            def api_ready():
                return True

            api_ready()

        assert p.to_json()["nodes"][0]["name"] == "Wait for API"

    def test_rrshift_chaining(self):
        with Pipeline("test") as p:
            src = source_db("In", query="SELECT 1", conn_id="pg")

            @sensor(poll_interval=5, timeout=60)
            def wait():
                return True

            out = sink_db("Out", table="t", conn_id="pg")
            src >> wait >> out

        data = p.to_json()
        assert len(data["edges"]) == 2


class TestDecoratorErrors:
    def test_decorator_outside_pipeline_raises(self):
        with pytest.raises(RuntimeError, match="must be used inside"):
            @source
            def bad():
                return []

    def test_filter_outside_pipeline_raises(self):
        with pytest.raises(RuntimeError, match="must be used inside"):
            @filter
            def bad(row):
                return True

    def test_map_outside_pipeline_raises(self):
        with pytest.raises(RuntimeError, match="must be used inside"):
            @map
            def bad(row):
                return row


class TestJsonSerialization:
    def test_all_decorators_produce_valid_json(self):
        """Every decorator-generated node must be JSON-serializable."""
        with Pipeline("full-test") as p:
            @source
            def fetch():
                return [{"id": 1}]

            @filter
            def only_valid(row):
                return row.get("id") is not None

            @map
            def add_field(row):
                row["new"] = True
                return row

            @validate
            def check(rows):
                return len(rows) > 0, "has data"

            @sink
            def write(rows):
                pass

            fetch >> only_valid >> add_field >> check >> write

        data = p.to_json()
        # Must serialize without error
        json.dumps(data)
        assert len(data["nodes"]) == 5
        assert len(data["edges"]) == 4
