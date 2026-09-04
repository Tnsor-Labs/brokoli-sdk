"""Contract tests against the real Brokoli backend.

Run with ``BROKOLI_BACKEND_COMMAND`` set to a command that starts a backend
on ``BROKOLI_BACKEND_URL``. The test intentionally uses the SDK client and
the real backend, not a Python server double.
"""

import os
import shlex

import pytest

from brokoli import Pipeline, Client, sink_file, source_file, transform
from brokoli.testing import BackendProcess


COMMAND = os.getenv("BROKOLI_BACKEND_COMMAND")
SERVER = os.getenv("BROKOLI_BACKEND_URL", "http://127.0.0.1:8080")


@pytest.mark.skipif(not COMMAND, reason="BROKOLI_BACKEND_COMMAND is not configured")
def test_static_pipeline_deploys_and_executes_against_real_backend(tmp_path):
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

    with BackendProcess(shlex.split(COMMAND), server=SERVER):
        client = Client.from_env(SERVER)
        client.deploy(pipeline)
        run = client.run(pipeline_id)
        detail = run.wait(timeout=120, raise_on_failure=True)

    assert detail["status"] == "success"
    output = output_path.read_text()
    assert "id,state" in output
    assert "1,active" in output
