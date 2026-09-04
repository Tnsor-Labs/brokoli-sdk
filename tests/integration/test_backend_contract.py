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
)
from brokoli.testing import BackendProcess
from brokoli.exceptions import CompatibilityError


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
