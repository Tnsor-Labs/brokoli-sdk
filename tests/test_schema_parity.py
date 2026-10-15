"""Schema parity: everything this SDK emits must validate against core's
canonical IR schema (brokoli-sdk#23 M3 / Tnsor-Labs/brokoli#109 M3).

``tests/fixtures/pipeline-ir-2.1.json``, ``pipeline-ir-2.2.json`` and
``task-interface-v1.json`` are vendored copies of the core repo's
``docs/schema/`` files, last refreshed from core v0.18.12 — refresh them
whenever the contract changes. 2.1 covers what the SDK emits without task
interfaces; 2.2 adds node ``interface`` and pipeline ``parameters``. The mirror-image check lives in core:
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

_FIXTURES = Path(__file__).parent / "fixtures"
SCHEMA = json.loads((_FIXTURES / "pipeline-ir-2.1.json").read_text())
SCHEMA_2_2 = json.loads((_FIXTURES / "pipeline-ir-2.2.json").read_text())
TASK_INTERFACE = json.loads((_FIXTURES / "task-interface-v1.json").read_text())

MODULE_CONSTANT = 500


def _helper(row):
    return {k.lower(): v for k, v in row.items()}


def _validate(payload):
    jsonschema.validate(payload, SCHEMA)


def _validate_2_2(payload):
    from referencing import Registry, Resource

    # 2.2 refers to task-interface-v1.json relative to its own $id.
    registry = Registry().with_resource(
        TASK_INTERFACE["$id"], Resource.from_contents(TASK_INTERFACE)
    )
    jsonschema.Draft202012Validator(SCHEMA_2_2, registry=registry).validate(payload)


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
            pagination=offset_pages(page_size=100, max_records=MODULE_CONSTANT).with_execution(
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
        alert = notify("Alert", notify_type="webhook", webhook_url="https://hooks.example/x")
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


def test_core_node_types_and_options_validate():
    import datetime

    from brokoli import bash, contract_gate, dbt, migrate, sink_db, source_db, wait

    with Pipeline("core-options", pipeline_id="core-options") as p:
        ready = wait("Ready", condition="http", url="https://x.example/ready", timeout=60)
        src = source_db("Read", query="SELECT 1", conn_id="warehouse", pool="warehouse")
        ready >> src
        gated = contract_gate(
            "Gate",
            input=src,
            contract={
                "ir_version": "1.0",
                "contract": {"id": "c", "version": "1"},
                "input": {"kind": "record-stream"},
                "rules": [],
            },
        )
        ran = bash("Shell", input=gated, command='echo "$X"', env={"X": "1"}, max_memory_mb=64)
        sink_db(
            "Load", input=ran, table="t", conn_id="ch", create_table=True, table_engine="MergeTree"
        )
        migrate(
            "Copy",
            source_conn_id="a",
            target_conn_id="b",
            query="SELECT * FROM t",
            table="t",
            mode="upsert",
            key_columns=["id"],
            partition={"column": "id", "strategy": "numeric", "boundaries": [0, 10]},
        )
        dbt("Models", vars={"day": str(datetime.date(2026, 10, 14))}, conn_id="warehouse")
    _validate(p.to_json())
    _validate_2_2(p.to_json())


def test_task_interface_pipeline_validates_against_2_2():
    from typing import TypedDict

    class Row(TypedDict):
        id: int

    with Pipeline("typed", pipeline_id="typed") as p:

        @task
        def score(rows: list[Row], threshold: float = 0.5) -> list[Row]:
            return rows

        score()

    payload = p.to_json()
    assert payload["ir_version"] == "2.2"
    _validate_2_2(payload)
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload)  # 2.1 has no interface/parameters
