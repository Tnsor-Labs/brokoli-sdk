"""Tests for code generation and TaskResult."""

from brokoli import Pipeline, task, condition, source_db, sink_db
from brokoli.result import TaskResult


class TestCodeGeneration:
    def test_task_generates_wrapper(self):
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")

            @task("Process")
            def process(df):
                return df

            process(src)

        nodes = p.to_json()["nodes"]
        code_node = next(n for n in nodes if n["type"] == "code")
        script = code_node["config"]["script"]
        assert "def process" in script
        assert "_task_result = process(rows)" in script

    def test_condition_generates_wrapper(self):
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")

            @condition("Check")
            def is_ok(df) -> bool:
                return len(df) > 0

            with is_ok(src) as (good, bad):
                good >> sink_db("OK", table="t", conn_id="pg")

        nodes = p.to_json()["nodes"]
        code_nodes = [n for n in nodes if n["type"] == "code"]
        assert len(code_nodes) >= 1
        assert "def is_ok" in code_nodes[0]["config"]["script"]

    def test_task_handles_none_return(self):
        """Template should handle None return from task."""
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")

            @task("Noop")
            def noop(df):
                return None

            noop(src)

        nodes = p.to_json()["nodes"]
        code_node = next(n for n in nodes if n["type"] == "code")
        assert "_task_result is None" in code_node["config"]["script"]

    def test_task_handles_task_result(self):
        """Template should handle TaskResult objects."""
        with Pipeline("test") as p:
            src = source_db("S", query="SELECT 1", conn_id="pg")

            @task("WithResult")
            def with_result(rows):
                return rows

            with_result(src)

        nodes = p.to_json()["nodes"]
        code_node = next(n for n in nodes if n["type"] == "code")
        script = code_node["config"]["script"]
        assert "to_rows" in script
        assert "#WARNING" in script
        assert "#ERROR" in script


class TestTaskResult:
    def test_basic(self):
        r = TaskResult(data=[{"a": 1}, {"a": 2}])
        assert r.row_count == 2
        assert not r.has_warnings
        assert not r.has_errors

    def test_with_warnings(self):
        r = TaskResult(data=[], warnings=["bad row"])
        assert r.has_warnings
        assert r.warnings == ["bad row"]

    def test_with_errors(self):
        r = TaskResult(data=[], errors=["fatal"])
        assert r.has_errors

    def test_to_rows(self):
        r = TaskResult(data=[{"x": 1}])
        rows = r.to_rows()
        assert rows == [{"x": 1}]

    def test_empty(self):
        r = TaskResult()
        assert r.row_count == 0
        assert r.to_rows() == []

    def test_metadata(self):
        r = TaskResult(data=[], metadata={"duration_ms": 100})
        assert r.metadata["duration_ms"] == 100


class TestSchemaHints:
    def test_source_db_has_hint(self):
        with Pipeline("test") as p:
            source_db("DB", query="SELECT 1", conn_id="pg")

        nodes = p.to_json()["nodes"]
        db_node = next(n for n in nodes if n["type"] == "source_db")
        assert db_node["config"].get("_schema_hint") == "query_result"

    def test_source_api_has_hint(self):
        with Pipeline("test") as p:
            from brokoli import source_api
            source_api("API", url="https://example.com")

        nodes = p.to_json()["nodes"]
        api_node = next(n for n in nodes if n["type"] == "source_api")
        assert api_node["config"].get("_schema_hint") == "api_response"
