"""Schema parity: everything this SDK emits must validate against core's
canonical IR schema (brokoli-sdk#23 M3 / Tnsor-Labs/brokoli#109 M3).

``tests/fixtures/pipeline-ir-2.1.json`` is a vendored copy of the core
repo's ``docs/schema/pipeline-ir-2.1.json`` — refresh it from core main
whenever the contract changes. The mirror-image check lives in core:
``models/ir_schema_contract_test.go`` validates an SDK-emitted golden
fixture, so drift in either direction fails one of the two suites.

Requires the ``jsonschema`` package (dev dependency); skipped when absent
so the rest of the suite stays runnable on minimal environments.
"""

import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")

from brokoli import (
    Pipeline,
    condition_node,
    notify,
    offset_pages,
    sink_file,
    source_api,
    task,
    transform,
)

SCHEMA = json.loads(
    (Path(__file__).parent / "fixtures" / "pipeline-ir-2.1.json").read_text()
)

MODULE_CONSTANT = 500


def _helper(row):
    return {k.lower(): v for k, v in row.items()}


def _validate(payload):
    jsonschema.validate(payload, SCHEMA)


def test_simple_pipeline_validates():
    with Pipeline("simple", pipeline_id="simple") as p:
        src = source_api("Fetch", url="https://api.example.com/x")
        src >> sink_file("Save", path="/tmp/out.csv", format="csv")
    _validate(p.to_json())


def test_kitchen_sink_pipeline_validates():
    with Pipeline(
        "kitchen-sink",
        pipeline_id="kitchen-sink",
        description="everything the SDK can emit",
        schedule="0 6 * * *",
        sla="07:30 America/New_York",
        tags=["a", "b"],
        depends_on=["upstream"],
        webhook=True,
    ) as p:
        src = source_api(
            "Fetch",
            url="https://api.example.com/x",
            pagination=offset_pages(page_size=100, max_records=MODULE_CONSTANT)
            .with_execution(
                max_concurrency=4,
                page_max_retries=3,
                page_retry_backoff="exponential",
                checkpoint_every=10,
            ),
            node_key="fetch",
        )

        @task("Clean")
        def clean(rows):
            return [_helper(r) for r in rows if len(r) < MODULE_CONSTANT]

        gate = condition_node("Gate", expression="row_count > 0")
        clean(src) >> gate

        keep = transform("Keep", rules=[{"type": "rename", "mapping": {"a": "b"}}])
        alert = notify(
            "Alert", notify_type="webhook", webhook_url="https://hooks.example/x"
        )
        gate.when(keep)
        gate.otherwise(alert)
        keep >> sink_file("Save", path="/tmp/out.csv", format="csv")

    payload = p.to_json()
    assert payload["ir_version"] == "2.1"
    _validate(payload)


def test_normalized_form_also_validates():
    # to_normalized_json() strips server fields and layout but must stay
    # inside the same contract -- it is compared against server documents.
    with Pipeline("norm", pipeline_id="norm") as p:
        src = source_api("Fetch", url="https://api.example.com/x")
        src >> sink_file("Save", path="/tmp/out.csv", format="csv")
    _validate(p.to_normalized_json())


def test_schema_rejects_what_sdk_must_never_emit():
    with Pipeline("bad", pipeline_id="bad") as p:
        src = source_api("Fetch", url="https://api.example.com/x")
        src >> sink_file("Save", path="/tmp/out.csv", format="csv")
    payload = p.to_json()
    payload["made_up_field"] = 1
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload)
