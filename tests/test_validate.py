"""Tests for pipeline validation."""

import pytest
from brokoli import Pipeline, task, condition
from brokoli import source_db, source_api, source_file
from brokoli import transform, join, quality_check
from brokoli import sink_db, sink_file, sink_api
from brokoli import bash, contract_gate, migrate, wait
from brokoli import code as code_node
from brokoli.validation import validate_pipeline


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


class TestValidateBashNode:
    def test_valid_command_uses_environment_for_dynamic_values(self):
        with Pipeline("test") as p:
            bash("Run", command='tool "$VALUE"', env={"VALUE": "${param.value}"})
        assert validate_pipeline(p).valid

    @pytest.mark.parametrize(
        ("config", "field"),
        [
            ({"command": ""}, "command"),
            ({"command": "echo ${param.value}"}, "command"),
            ({"command": "true", "working_dir": 1}, "working_dir"),
            ({"command": "true", "env": "VALUE=x"}, "env"),
            ({"command": "true", "env": {"NOT-VALID": "x"}}, "env"),
            ({"command": "true", "env": {"VALUE": "bad\x00value"}}, "env"),
        ],
    )
    def test_rejects_unsafe_or_invalid_config(self, config, field):
        with Pipeline("test") as p:
            p._add_node("bash_run", "bash", "Run", config)
        result = validate_pipeline(p)
        assert not result.valid
        assert any(issue.field == field for issue in result.errors)


class TestValidateWaitNode:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"condition": "file_exists", "path": "/data/x"},
            {"condition": "http", "url": "https://x.example", "timeout": "2h30m"},
            {"condition": "interval_elapsed", "offset": "-5m"},
            {"condition": "interval_elapsed"},
            {"condition": "pipeline", "pipeline_id": "nightly", "poll_interval": 60},
        ],
    )
    def test_valid(self, kwargs):
        with Pipeline("test") as p:
            wait("Wait", **kwargs)
        assert validate_pipeline(p).valid

    @pytest.mark.parametrize(
        ("config", "field"),
        [
            ({}, "condition"),
            ({"condition": "sometime"}, "condition"),
            ({"condition": "file_exists"}, "path"),
            ({"condition": "http"}, "url"),
            ({"condition": "pipeline"}, "pipeline_id"),
            ({"condition": "interval_elapsed", "offset": "soon"}, "offset"),
            ({"condition": "interval_elapsed", "poll_interval": "0s"}, "poll_interval"),
            ({"condition": "interval_elapsed", "timeout": "-1h"}, "timeout"),
            ({"condition": "interval_elapsed", "timeout": 30}, "timeout"),
        ],
    )
    def test_rejects_what_the_server_refuses(self, config, field):
        with Pipeline("test") as p:
            p._add_node("wait_1", "wait", "Wait", config)
        result = validate_pipeline(p)
        assert not result.valid
        assert any(issue.field == field for issue in result.errors), result.errors


class TestValidateContractGateNode:
    def test_valid(self):
        with Pipeline("test") as p:
            contract_gate(
                "Gate",
                contract={"ir_version": "1.0", "contract": {"id": "c", "version": "1"}, "input": {"kind": "record-stream"}, "rules": []},
            )
        assert validate_pipeline(p).valid

    @pytest.mark.parametrize(
        "contract",
        [None, "orders.contract.json", {"ir_version": "1.0", "rules": []}],
    )
    def test_rejects_what_cannot_be_a_contract(self, contract):
        with Pipeline("test") as p:
            contract_gate("Gate", contract=contract)
        result = validate_pipeline(p)
        assert not result.valid
        assert any(issue.field == "contract" for issue in result.errors)


class TestValidateCodeAgainstServerRules:
    def test_param_reference_in_script_is_refused(self):
        with Pipeline("test") as p:
            code_node("Run", script='threshold = "${param.threshold}"')
        result = validate_pipeline(p)
        assert any(i.field == "script" and "${param.threshold}" in i.message for i in result.errors)

    def test_params_lookup_is_fine(self):
        with Pipeline("test") as p:
            code_node("Run", script='threshold = params["threshold"]')
        assert validate_pipeline(p).valid

    @pytest.mark.parametrize("key", ["max_memory_mb", "max_cpu_seconds"])
    @pytest.mark.parametrize("value", [0, -1, "256", True])
    def test_limits_must_be_positive_numbers(self, key, value):
        with Pipeline("test") as p:
            code_node("Run", script="pass", **{key: value})
        assert any(i.field == key for i in validate_pipeline(p).errors)


_PARTITIONED = dict(
    source_conn_id="a",
    target_conn_id="b",
    query="SELECT * FROM t",
    table="t",
    mode="upsert",
    key_columns=["id"],
)


class TestValidateMigratePartition:
    @pytest.mark.parametrize(
        "partition",
        [
            {"column": "id", "strategy": "numeric", "boundaries": [0, 100, 200]},
            {"column": "id", "strategy": "NUMERIC", "boundaries": [0.5, 1], "max_parallel": 64},
        ],
    )
    def test_valid(self, partition):
        with Pipeline("test") as p:
            migrate("Copy", partition=partition, **_PARTITIONED)
        assert validate_pipeline(p).valid, validate_pipeline(p).errors

    def test_valid_date_strategy(self):
        with Pipeline("test") as p:
            migrate(
                "Copy",
                partition={
                    "column": "day",
                    "strategy": "date",
                    "boundaries": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00+01:00"],
                },
                **{**_PARTITIONED, "key_columns": ["id", "day"]},
            )
        assert validate_pipeline(p).valid, validate_pipeline(p).errors

    @pytest.mark.parametrize(
        ("partition", "overrides", "field"),
        [
            ("id", {}, "partition"),
            ({"column": "id; drop", "strategy": "numeric", "boundaries": [0, 1]}, {}, "partition"),
            ({"column": "id", "strategy": "hash", "boundaries": [0, 1]}, {}, "partition"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0]}, {}, "partition"),
            ({"column": "id", "strategy": "numeric", "boundaries": [5, 1]}, {}, "partition"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0, "1"]}, {}, "partition"),
            ({"column": "d", "strategy": "date", "boundaries": ["2026-01-01", "2026-02-01"]}, {"key_columns": ["d"]}, "partition"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0, 1], "max_parallel": 65}, {}, "partition"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0, 1]}, {"mode": "append", "key_columns": ["id"]}, "mode"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0, 1]}, {"key_columns": ["other"]}, "key_columns"),
            ({"column": "id", "strategy": "numeric", "boundaries": [0, 1]}, {"create_table": True}, "create_table"),
        ],
    )
    def test_rejects_what_the_server_refuses(self, partition, overrides, field):
        with Pipeline("test") as p:
            migrate("Copy", partition=partition, **{**_PARTITIONED, **overrides})
        result = validate_pipeline(p)
        assert any(i.field == field for i in result.errors), result.errors


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
            src = source_db("A", query="SELECT 1", conn_id="pg")
            from brokoli.pipeline import _make_id

            nid = _make_id("cond")
            p._add_node(nid, "condition", "Check", {"expression": "row_count > 0"})
            p._add_edge(src.node_id, nid)
        vr = validate_pipeline(p)
        assert vr.valid

    def test_inputless_condition_is_an_error(self):
        # Mirrors the server: a condition node requires exactly 1 input.
        with Pipeline("test") as p:
            from brokoli.pipeline import _make_id

            nid = _make_id("cond")
            p._add_node(nid, "condition", "Check", {"expression": "row_count > 0"})
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("exactly 1 input" in str(e) for e in vr.errors)


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

    def test_disconnected_node_is_an_error(self):
        # Used to be a warning; the server has rejected disconnected
        # nodes at save time since v0.10.10, so a local pass was fiction.
        with Pipeline("test") as p:
            source_db("A", query="SELECT 1", conn_id="pg")
            source_db("B", query="SELECT 2", conn_id="pg")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert sum("disconnected" in str(e) for e in vr.errors) == 2


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
            sink_db("No Table")  # missing table + conn_id
            quality_check("No Rules")  # missing rules
        vr = validate_pipeline(p)
        assert not vr.valid
        assert len(vr.errors) >= 5  # multiple issues


class TestValidateSinkDBModes:
    """brokoli-sdk#12: sink_db write modes and their requirements."""

    def _pipe(self, **kw):
        with Pipeline("t", pipeline_id="t") as p:
            src = source_db("Src", query="SELECT 1", conn_id="pg")
            sink_db("Sink", input=src, table="t", conn_id="pg", **kw)
        return p

    def test_append_and_overwrite_valid(self):
        for mode in ("append", "overwrite"):
            assert validate_pipeline(self._pipe(mode=mode)).valid, mode

    def test_upsert_requires_key_columns(self):
        vr = validate_pipeline(self._pipe(mode="upsert"))
        assert not vr.valid
        assert any("key_columns" in e.message for e in vr.errors)

    def test_upsert_with_key_columns_valid_and_emitted(self):
        p = self._pipe(mode="upsert", key_columns=["id"])
        assert validate_pipeline(p).valid
        cfg = [n for n in p.to_json()["nodes"] if n["name"] == "Sink"][0]["config"]
        assert cfg["mode"] == "upsert"
        assert cfg["key_columns"] == ["id"]

    def test_unknown_mode_rejected(self):
        vr = validate_pipeline(self._pipe(mode="merge"))
        assert not vr.valid
        assert any("mode" in e.message.lower() for e in vr.errors)


class TestValidateMigrateMode:
    """brokoli-sdk#12 follow-up: migrate honors append/overwrite/upsert."""

    def _pipe(self, mode, **kw):
        from brokoli import migrate

        with Pipeline("t", pipeline_id="t") as p:
            migrate(
                "Mig",
                source_conn_id="a",
                target_conn_id="b",
                query="SELECT 1",
                table="t",
                mode=mode,
                **kw,
            )
        return p

    def test_append_and_overwrite_valid(self):
        for mode in ("append", "overwrite"):
            assert validate_pipeline(self._pipe(mode)).valid, mode

    def test_upsert_requires_key_columns(self):
        vr = validate_pipeline(self._pipe("upsert"))
        assert not vr.valid
        assert any("key_columns" in e.message for e in vr.errors)

    def test_upsert_with_key_columns_valid_and_emitted(self):
        p = self._pipe("upsert", key_columns=["id"])
        assert validate_pipeline(p).valid
        cfg = [n for n in p.to_json()["nodes"] if n["name"] == "Mig"][0]["config"]
        assert cfg["mode"] == "upsert"
        assert cfg["key_columns"] == ["id"]

    def test_unknown_mode_rejected(self):
        vr = validate_pipeline(self._pipe("merge"))
        assert not vr.valid
        assert any("mode" in e.message.lower() for e in vr.errors)
