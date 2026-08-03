"""Tests for Phase 0: Protocol Alignment & Pipeline IR V2.

Covers:
- UNSET (falsy-value) handling for retries=0 / timeout=0 across
  sink_db() and the @task / @source / @sink decorators.
- The node capability model and capability-driven checks.
- The pipeline-level ir_version field.
- Decorator-wrapper fan-out deduplication (no duplicate nodes when the
  same wrapper instance is reused across multiple edges).
"""

from brokoli import (
    Pipeline, task, source, sink, sensor,
    source_db, source_api, sink_db, sink_file, sink_api, code,
    dbt, migrate, notify, transform, condition_node,
)
from brokoli.validation import validate_pipeline


# ---------------------------------------------------------------------------
# UNSET handling: explicit 0 must survive serialization
# ---------------------------------------------------------------------------

class TestUnsetSinkDB:
    def test_retries_zero_survives(self):
        with Pipeline("test") as p:
            sink_db("Out", table="t", conn_id="pg", retries=0)
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 0

    def test_uri_omitted_not_leaked_as_empty_string(self):
        with Pipeline("test") as p:
            sink_db("Out", table="t", conn_id="pg")
        config = p.to_json()["nodes"][0]["config"]
        assert "uri" not in config


class TestUnoptedOptionalFieldsAreNotLeaked:
    """``_build_config`` now only filters UNSET/None (not all falsy values),
    so every optional string/dict param that used to default to "" or {}
    must default to UNSET instead -- otherwise it leaks into the
    serialized config as an empty value that was never explicitly set."""

    def test_code_python_path_omitted(self):
        with Pipeline("test") as p:
            code("C", script="x = 1")
        config = p.to_json()["nodes"][0]["config"]
        assert "python_path" not in config

    def test_sink_file_compress_omitted(self):
        with Pipeline("test") as p:
            sink_file("F", path="/out.csv")
        config = p.to_json()["nodes"][0]["config"]
        assert "compress" not in config

    def test_sink_api_body_and_headers_omitted(self):
        with Pipeline("test") as p:
            sink_api("A", url="https://x")
        config = p.to_json()["nodes"][0]["config"]
        assert "body_template" not in config
        assert "headers" not in config

    def test_dbt_optional_fields_omitted(self):
        with Pipeline("test") as p:
            dbt("D", project_dir="/dbt")
        config = p.to_json()["nodes"][0]["config"]
        assert config == {"command": "run", "project_dir": "/dbt"}

    def test_notify_message_and_channel_omitted(self):
        with Pipeline("test") as p:
            notify("N", webhook_url="https://hook")
        config = p.to_json()["nodes"][0]["config"]
        assert "message" not in config
        assert "channel" not in config

    def test_migrate_conn_ids_omitted(self):
        with Pipeline("test") as p:
            migrate("M", source_uri="a", target_uri="b", query="q", table="t")
        config = p.to_json()["nodes"][0]["config"]
        assert "source_conn_id" not in config
        assert "dest_conn_id" not in config

    def test_source_db_retry_backoff_still_defaults_when_retries_set(self):
        """retry_backoff is a plain string default ("exponential"), not
        UNSET -- unlike retries/timeout, an empty/omitted backoff strategy
        isn't a meaningful distinct value from the default, so it should
        keep appearing whenever retries is set, exactly as before."""
        with Pipeline("test") as p:
            source_db("S", query="SELECT 1", conn_id="pg", retries=3)
        config = p.to_json()["nodes"][0]["config"]
        assert config["retry_backoff"] == "exponential"

    def test_retries_omitted_not_in_config(self):
        with Pipeline("test") as p:
            sink_db("Out", table="t", conn_id="pg")
        config = p.to_json()["nodes"][0]["config"]
        assert "max_retries" not in config

    def test_retries_positive_survives(self):
        with Pipeline("test") as p:
            sink_db("Out", table="t", conn_id="pg", retries=5)
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 5


class TestUnsetTaskDecorator:
    def test_retries_zero_survives(self):
        with Pipeline("test") as p:
            @task(retries=0, timeout=0)
            def clean(rows):
                return rows

            clean()
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 0
        assert config["timeout"] == 0

    def test_retries_omitted_not_in_config(self):
        with Pipeline("test") as p:
            @task
            def clean(rows):
                return rows

            clean()
        config = p.to_json()["nodes"][0]["config"]
        assert "max_retries" not in config
        assert "timeout" not in config


class TestUnsetSourceDecorator:
    def test_retries_and_timeout_zero_survive(self):
        with Pipeline("test") as p:
            @source(retries=0, timeout=0)
            def fetch():
                return []

            fetch()
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 0
        assert config["timeout"] == 0

    def test_omitted_not_in_config(self):
        with Pipeline("test") as p:
            @source
            def fetch():
                return []

            fetch()
        config = p.to_json()["nodes"][0]["config"]
        assert "max_retries" not in config
        assert "timeout" not in config


class TestUnsetSinkDecorator:
    def test_retries_and_timeout_zero_survive(self):
        with Pipeline("test") as p:
            @sink(retries=0, timeout=0)
            def push(rows):
                pass

            push()
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 0
        assert config["timeout"] == 0

    def test_omitted_not_in_config(self):
        with Pipeline("test") as p:
            @sink
            def push(rows):
                pass

            push()
        config = p.to_json()["nodes"][0]["config"]
        assert "max_retries" not in config
        assert "timeout" not in config


class TestUnsetSourceDbAndApi:
    """source_db/source_api already handled UNSET correctly -- guard against regression."""

    def test_source_db_retries_zero(self):
        with Pipeline("test") as p:
            source_db("S", query="SELECT 1", conn_id="pg", retries=0)
        config = p.to_json()["nodes"][0]["config"]
        assert config["max_retries"] == 0

    def test_source_api_timeout_zero(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", timeout=0)
        config = p.to_json()["nodes"][0]["config"]
        assert config["timeout"] == 0


class TestSensorTimeout:
    def test_default_timeout_adds_buffer(self):
        with Pipeline("test") as p:
            @sensor(poll_interval=10, timeout=100)
            def ready():
                return True

            ready()
        node = p.to_json()["nodes"][0]
        assert node["config"]["timeout"] == 160  # 100 + 60 buffer
        assert "_timeout = 100" in node["config"]["script"]

    def test_none_timeout_means_no_timeout(self):
        with Pipeline("test") as p:
            @sensor(poll_interval=10, timeout=None)
            def ready():
                return True

            ready()
        node = p.to_json()["nodes"][0]
        assert "timeout" not in node["config"]
        assert "_timeout = None" in node["config"]["script"]
        assert "_timeout is not None and" in node["config"]["script"]


# ---------------------------------------------------------------------------
# Capability model
# ---------------------------------------------------------------------------

class TestCapabilities:
    def test_source_db_has_source_capability(self):
        with Pipeline("test") as p:
            source_db("S", query="SELECT 1", conn_id="pg")
        node = p.to_json()["nodes"][0]
        assert "source" in node["capabilities"]
        assert "dataset-output" in node["capabilities"]

    def test_sink_db_has_sink_capability(self):
        with Pipeline("test") as p:
            sink_db("S", table="t", conn_id="pg")
        node = p.to_json()["nodes"][0]
        assert node["capabilities"] == ["sink"]

    def test_dbt_and_migrate_are_sources(self):
        with Pipeline("test") as p:
            dbt("D", command="run", project_dir="/dbt")
            migrate("M", source_conn_id="a", target_conn_id="b",
                     query="SELECT 1", table="t")
        for node in p.to_json()["nodes"]:
            assert "source" in node["capabilities"], node

    def test_transform_and_condition_are_compute(self):
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")
            transform("T", input=src, rules=["FILTER x > 0"])
            condition_node("C", expression="row_count > 0", input=src)
        nodes = {n["name"]: n for n in p.to_json()["nodes"]}
        assert nodes["T"]["capabilities"] == ["compute"]
        assert nodes["C"]["capabilities"] == ["compute"]

    def test_decorated_source_has_source_capability(self):
        with Pipeline("test") as p:
            @source
            def fetch():
                return []

            fetch()
        node = p.to_json()["nodes"][0]
        assert node["type"] == "code"  # still registered as a code node
        assert "source" in node["capabilities"]

    def test_decorated_sink_has_sink_capability(self):
        with Pipeline("test") as p:
            @sink
            def push(rows):
                pass

            push()
        node = p.to_json()["nodes"][0]
        assert node["type"] == "code"
        assert node["capabilities"] == ["sink"]

    def test_decorated_task_is_compute(self):
        with Pipeline("test") as p:
            @task
            def clean(rows):
                return rows

            clean()
        node = p.to_json()["nodes"][0]
        assert node["capabilities"] == ["compute"]


class TestSchemaHintUsesCapabilities:
    def test_source_db_hint_unaffected(self):
        with Pipeline("test") as p:
            source_db("S", query="SELECT 1", conn_id="pg")
        node = p.to_json()["nodes"][0]
        assert node["config"]["_schema_hint"] == "query_result"

    def test_source_api_hint_unaffected(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x")
        node = p.to_json()["nodes"][0]
        assert node["config"]["_schema_hint"] == "api_response"

    def test_non_source_never_gets_hint(self):
        with Pipeline("test") as p:
            sink_file("S", path="/out.csv")
        node = p.to_json()["nodes"][0]
        assert "_schema_hint" not in node["config"]


class TestValidationUsesCapabilities:
    def test_pipeline_with_no_source_warns(self):
        with Pipeline("test") as p:
            from brokoli.pipeline import _make_id
            nid = _make_id("compute_only")
            p._add_node(nid, "code", "Compute Only", {"language": "python", "script": "x = 1"})
        vr = validate_pipeline(p)
        assert any("no source" in w.message.lower() for w in vr.warnings)

    def test_pipeline_with_dbt_source_does_not_warn(self):
        with Pipeline("test") as p:
            dbt("D", command="run", project_dir="/dbt")
        vr = validate_pipeline(p)
        assert not any("no source" in w.message.lower() for w in vr.warnings)

    def test_pipeline_with_decorated_source_does_not_warn(self):
        with Pipeline("test") as p:
            @source
            def fetch():
                return []

            fetch()
        vr = validate_pipeline(p)
        assert not any("no source" in w.message.lower() for w in vr.warnings)


# ---------------------------------------------------------------------------
# IR version
# ---------------------------------------------------------------------------

class TestIRVersion:
    def test_ir_version_present(self):
        with Pipeline("test") as p:
            source_db("S", query="SELECT 1", conn_id="pg")
        data = p.to_json()
        assert data["ir_version"] == "2.0"


# ---------------------------------------------------------------------------
# Decorator wrapper fan-out deduplication
# ---------------------------------------------------------------------------

class TestWrapperFanOutDedup:
    def test_fan_out_to_same_wrapper_twice_creates_one_node(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1", conn_id="pg")

            @task
            def w(rows):
                return rows

            a >> [w, w]

        data = p.to_json()
        # 1 source + 1 task node, not 3.
        assert len(data["nodes"]) == 2
        code_nodes = [n for n in data["nodes"] if n["type"] == "code"]
        assert len(code_nodes) == 1

    def test_reusing_wrapper_across_separate_rshift_calls_creates_one_node(self):
        with Pipeline("test") as p:
            @task
            def w(rows):
                return rows

            sink_a = sink_db("SinkA", table="a", conn_id="pg")
            sink_b = sink_db("SinkB", table="b", conn_id="pg")

            w >> sink_a
            w >> sink_b

        data = p.to_json()
        code_nodes = [n for n in data["nodes"] if n["type"] == "code"]
        assert len(code_nodes) == 1
        w_id = code_nodes[0]["id"]
        outgoing = [e for e in data["edges"] if e["from"] == w_id]
        assert len(outgoing) == 2
        assert {e["to"] for e in outgoing} == {sink_a.node_id, sink_b.node_id}

    def test_explicit_calls_with_different_inputs_are_not_collapsed(self):
        """Calling the same @task-decorated function twice with different
        upstream inputs must create two distinct nodes -- only the
        zero-arg auto-call path used by ``>>`` chaining is cached."""
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1", conn_id="pg")
            b = source_db("B", query="SELECT 2", conn_id="pg")

            @task
            def clean(rows):
                return rows

            clean(a)
            clean(b)

        data = p.to_json()
        code_nodes = [n for n in data["nodes"] if n["type"] == "code"]
        assert len(code_nodes) == 2

    def test_source_wrapper_reused_across_fan_out(self):
        with Pipeline("test") as p:
            @source
            def fetch():
                return []

            sink_a = sink_db("SinkA", table="a", conn_id="pg")
            sink_b = sink_db("SinkB", table="b", conn_id="pg")

            fetch >> sink_a
            fetch >> sink_b

        data = p.to_json()
        code_nodes = [n for n in data["nodes"] if n["type"] == "code"]
        assert len(code_nodes) == 1
