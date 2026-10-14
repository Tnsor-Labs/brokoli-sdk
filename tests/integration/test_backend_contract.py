"""Contract tests against the real Brokoli backend.

Run with ``BROKOLI_BACKEND_COMMAND`` set to a command that starts a backend
on ``BROKOLI_BACKEND_URL``. The test intentionally uses the SDK client and
the real backend, not a Python server double.
"""

import os
import shlex

import pytest

from brokoli import (
    Client,
    Pipeline,
    condition_node,
    sink_file,
    source_file,
    task,
    transform,
    code,
    offset_pages,
    source_api,
    bash,
    contract_gate,
    migrate,
    wait,
)
from brokoli.testing import BackendProcess
from brokoli.exceptions import CompatibilityError
from brokoli.result import TaskResult


COMMAND = os.getenv("BROKOLI_BACKEND_COMMAND")
SERVER = os.getenv("BROKOLI_BACKEND_URL", "http://127.0.0.1:8080")


@pytest.fixture(scope="module")
def client():
    if not COMMAND:
        pytest.skip("BROKOLI_BACKEND_COMMAND is not configured")
    with BackendProcess(shlex.split(COMMAND), server=SERVER):
        yield Client.from_env(SERVER)


def _run(client, pipeline):
    client.deploy(pipeline)
    detail = client.run(pipeline.pipeline_id).wait(timeout=120, raise_on_failure=True)
    assert detail["status"] == "success"
    return detail


def test_static_pipeline_deploys_and_executes_against_real_backend(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    input_path.write_text("id,status\n1,active\n2,inactive\n")

    pipeline_id = f"sdk-contract-{os.getpid()}"
    with Pipeline("SDK backend contract", pipeline_id=pipeline_id) as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        cleaned = transform(
            "Shape rows",
            input=source,
            rules=[{"type": "rename", "mapping": {"status": "state"}}],
        )
        cleaned >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    output = output_path.read_text()
    assert "id,state" in output
    assert "1,active" in output


def test_decorated_task_is_packaged_and_executes(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    input_path.write_text("id,status\n1,active\n")

    with Pipeline(
        "SDK decorated task contract", pipeline_id=f"sdk-contract-task-{os.getpid()}"
    ) as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")

        @task("Decorated transform")
        def decorate(rows):
            return [dict(row, source="decorated") for row in rows]

        decorated = decorate(source)
        decorated >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    assert "source" in output_path.read_text()
    assert "decorated" in output_path.read_text()


def test_decorated_task_result_round_trips_through_backend_wrapper(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "task-result.csv"
    input_path.write_text("id,status\n1,active\n")

    with Pipeline(
        "SDK decorator wrapper contract", pipeline_id=f"sdk-contract-wrapper-{os.getpid()}"
    ) as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")

        @task("Structured result")
        def structured(rows):
            return TaskResult(
                data=[dict(row, checked="yes") for row in rows],
                warnings=["one row checked"],
            )

        structured(source) >> sink_file("Write", path=str(output_path), format="csv")

    detail = _run(client, pipeline)
    output = output_path.read_text()
    assert "checked" in output
    logs = client.run_handle(detail["id"]).logs()
    assert any("#WARNING: one row checked" in str(entry) for entry in logs)


def test_artifact_reference_transfers_to_downstream_code(client, tmp_path):
    output_path = tmp_path / "artifact-reference.csv"
    script = (
        "uri = rows[0].get('uri', '')\n"
        "checksum = rows[0].get('checksum', '')\n"
        "if not uri or not checksum:\n"
        "    raise RuntimeError('artifact reference was not transferred')\n"
        "output_data = {'columns': ['uri', 'checksum'], 'rows': [{'uri': uri, 'checksum': checksum}]}\n"
    )

    with Pipeline(
        "SDK artifact reference contract", pipeline_id=f"sdk-contract-artifact-{os.getpid()}"
    ) as pipeline:
        artifact = source_api(
            "Fetch artifact",
            url="/api/samples/data/employees.json",
            headers={"Authorization": "Bearer sdk-contract-test-key"},
            response="artifact",
        )
        code("Read artifact reference", input=artifact, script=script) >> sink_file(
            "Write reference", path=str(output_path), format="csv"
        )

    _run(client, pipeline)
    output = output_path.read_text()
    assert "sha256:" in output
    assert "uri" in output


def test_condition_routes_to_the_true_branch(client, tmp_path):
    input_path = tmp_path / "input.csv"
    true_path = tmp_path / "true.csv"
    false_path = tmp_path / "false.csv"
    input_path.write_text("id,status\n1,active\n")

    with Pipeline(
        "SDK condition contract", pipeline_id=f"sdk-contract-condition-{os.getpid()}"
    ) as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        gate = condition_node("Has rows", expression="row_count > 0", input=source)
        gate.when(sink_file("True branch", path=str(true_path), format="csv"))
        gate.otherwise(sink_file("False branch", path=str(false_path), format="csv"))

    _run(client, pipeline)
    assert true_path.exists()
    assert not false_path.exists()


def test_compile_only_dataset_map_is_rejected_by_backend_capabilities(client, tmp_path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("id,status\n1,active\n")

    with Pipeline(
        "SDK unsupported capability contract",
        pipeline_id=f"sdk-contract-unsupported-{os.getpid()}",
    ) as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        mapped = source.map(lambda row: row, name="Compile-only map")
        mapped >> sink_file("Write", path=str(tmp_path / "output.csv"), format="csv")

    with pytest.raises(CompatibilityError, match="dataset-map"):
        client.deploy(pipeline)


def test_pagination_fetches_all_pages(client, tmp_path):
    output_path = tmp_path / "pages.csv"
    with Pipeline(
        "SDK pagination contract", pipeline_id=f"sdk-contract-pagination-{os.getpid()}"
    ) as pipeline:
        pages = source_api(
            "Pages",
            url="/api/samples/data/employees.json",
            headers={"Authorization": "Bearer sdk-contract-test-key"},
            pagination=offset_pages(page_size=2, max_records=5),
        )
        pages >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    output = output_path.read_text()
    assert output.count("\n") == 6
    assert all(name in output for name in ("Alice Chen", "Bob Martinez", "Carla Singh"))


def test_node_retry_recovers_and_reaches_success(client, tmp_path):
    marker = tmp_path / "attempted"
    output_path = tmp_path / "retried.csv"
    script = (
        "import os, sys\n"
        f"marker = {str(marker)!r}\n"
        "if not os.path.exists(marker):\n"
        "    open(marker, 'w').close()\n"
        "    sys.exit(1)\n"
        "output_data = {'columns': ['status'], 'rows': [{'status': 'recovered'}]}\n"
    )

    with Pipeline(
        "SDK retry contract", pipeline_id=f"sdk-contract-retry-{os.getpid()}"
    ) as pipeline:
        source = source_file("Read", path=str(tmp_path / "input.csv"), format="csv")
        (
            code("Retrying node", input=source, script=script, retries=1)
            >> sink_file("Write", path=str(output_path), format="csv")
        )
    (tmp_path / "input.csv").write_text("id\n1\n")

    detail = _run(client, pipeline)
    assert marker.exists()
    assert "recovered" in output_path.read_text()
    assert any(node.get("attempt", 0) for node in detail.get("node_runs", []))


def test_wait_whose_condition_holds_passes_its_input_through(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    input_path.write_text("id\n1\n2\n")

    with Pipeline("SDK wait contract", pipeline_id=f"sdk-contract-wait-{os.getpid()}") as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        ready = wait(
            "Input landed",
            input=source,
            condition="file_exists",
            path=str(input_path),
            poll_interval=1,
            timeout=60,
        )
        ready >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    assert output_path.read_text().splitlines() == ["id", "1", "2"]


def test_contract_gate_drops_quarantined_records(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    input_path.write_text("id,email\n1,a@example.com\n2,not-an-email\n")
    contract = {
        "ir_version": "1.0",
        "contract": {"id": "people", "version": "1"},
        "input": {"kind": "record-stream"},
        "rules": [
            {
                "id": "email-valid",
                "kind": "record",
                "path": "$.email",
                "predicate": {"op": "format", "format": "email"},
                "on_breach": {"severity": "error", "action": "quarantine"},
            }
        ],
    }

    with Pipeline("SDK contract gate", pipeline_id=f"sdk-contract-gate-{os.getpid()}") as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        clean = contract_gate("People contract", input=source, contract=contract)
        clean >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    output = output_path.read_text()
    assert "a@example.com" in output
    assert "not-an-email" not in output


def test_bash_runs_with_env_and_passes_input_through(client, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    marker = tmp_path / "marker.txt"
    input_path.write_text("id\n7\n")

    with Pipeline("SDK bash contract", pipeline_id=f"sdk-contract-bash-{os.getpid()}") as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        ran = bash(
            "Touch marker",
            input=source,
            command='printf "%s" "$GREETING" > "$MARKER"',
            env={"GREETING": "hello", "MARKER": str(marker)},
        )
        ran >> sink_file("Write", path=str(output_path), format="csv")

    _run(client, pipeline)
    assert marker.read_text() == "hello"
    assert output_path.read_text().splitlines() == ["id", "7"]


def test_dry_run_returns_node_results_without_a_run(client, tmp_path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("id\n1\n2\n")
    with Pipeline("SDK dry run", pipeline_id=f"sdk-contract-dry-{os.getpid()}") as pipeline:
        source = source_file("Read", path=str(input_path), format="csv")
        transform("Shape", input=source, rules=[{"type": "rename", "mapping": {"id": "key"}}])
    client.deploy(pipeline)
    result = client.dry_run(pipeline)
    assert "error" not in result, result
    assert result["results"], result


def test_resume_reruns_from_the_failed_node_as_a_new_run(client, tmp_path):
    marker = tmp_path / "fixed"
    output_path = tmp_path / "out.csv"
    (tmp_path / "input.csv").write_text("id\n1\n")
    script = (
        "import os, sys\n"
        f"if not os.path.exists({str(marker)!r}):\n"
        "    sys.exit(1)\n"
        "output_data = {'columns': ['status'], 'rows': [{'status': 'resumed'}]}\n"
    )
    with Pipeline("SDK resume", pipeline_id=f"sdk-contract-resume-{os.getpid()}") as pipeline:
        source = source_file("Read", path=str(tmp_path / "input.csv"), format="csv")
        code("Flaky", input=source, script=script) >> sink_file(
            "Write", path=str(output_path), format="csv"
        )
    client.deploy(pipeline)
    failed = client.run(pipeline.pipeline_id)
    assert failed.wait(timeout=120)["status"] == "failed"

    marker.touch()
    resumed = failed.resume()
    assert resumed.id != failed.id
    assert resumed.wait(timeout=120, raise_on_failure=True)["status"] == "success"
    assert "resumed" in output_path.read_text()


def test_backfill_plans_one_run_per_interval(client, tmp_path):
    (tmp_path / "input.csv").write_text("id\n1\n")
    with Pipeline(
        "SDK backfill",
        pipeline_id=f"sdk-contract-backfill-{os.getpid()}",
        schedule="0 0 * * *",
    ) as pipeline:
        # One file per interval: the server refuses a backfill whose runs
        # would all do the same work.
        source_file("Read", path=str(tmp_path / "input.csv"), format="csv") >> sink_file(
            "Write", path=str(tmp_path / "out-${interval.start}.csv"), format="csv"
        )
    client.deploy(pipeline)
    plan = client.backfill(pipeline, "2026-10-01", "2026-10-03")
    assert plan["intervals"] == 3, plan


def test_partitioned_migrate_is_accepted_and_copies_every_range(client, tmp_path):
    import sqlite3

    source_db_path, target_db_path = tmp_path / "source.db", tmp_path / "target.db"
    with sqlite3.connect(source_db_path) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO events VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 31)])
    with sqlite3.connect(target_db_path) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, v TEXT)")

    with Pipeline("SDK partitioned migrate", pipeline_id=f"sdk-contract-part-{os.getpid()}") as pipeline:
        migrate(
            "Copy",
            source_uri=f"sqlite://{source_db_path}",
            target_uri=f"sqlite://{target_db_path}",
            query="SELECT id, v FROM events",
            table="events",
            mode="upsert",
            key_columns=["id"],
            # One range at a time: SQLite takes one writer, and parallel
            # ranges into it can fail with SQLITE_BUSY.
            partition={"column": "id", "strategy": "numeric", "boundaries": [0, 11, 21, 31], "max_parallel": 1},
        )
    _run(client, pipeline)
    with sqlite3.connect(target_db_path) as conn:
        assert conn.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM events").fetchone() == (30, 1, 30)
