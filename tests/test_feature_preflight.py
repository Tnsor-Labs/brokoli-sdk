"""Execution-feature preflight gating (sdk#15 M3, core#109 M3 counterpart).

The server's ``supported_execution_features`` lists only what it can
actually RUN. The preflight contract here:

  - field absent: for a purely DECLARATIVE feature (conditional-routing,
    data_intervals, ...) gating is skipped -- a genuinely pre-v0.10.11
    server executes several gated features without advertising them, so
    absence must never read as "no features" for those;
  - field absent, but a RUNTIME-EXISTENCE feature is required
    (code-streaming-emit, task-bundles -- ADR-030 §3): gating still
    applies. A server old enough to omit the field cannot have the
    wrapper/mount machinery those names refer to, so absence there means
    "unsupported", not "predates the mechanism" -- see
    RUNTIME_EXISTENCE_FEATURES in brokoli.compatibility;
  - field present: every feature the compiled payload depends on must be
    advertised, or deployment fails naming the missing features, and
    --allow-legacy-server cannot override it;
  - field present but malformed: fail closed, like every capability field.
"""

import json
import urllib.request

import pytest

from brokoli import Pipeline, sink_file, source_api, source_file, union
from brokoli.compatibility import (
    preflight_server_compatibility,
    required_execution_features,
)
from brokoli.exceptions import CompatibilityError
from brokoli.pagination import offset_pages


def _serve_capabilities(monkeypatch, payload):
    class _Resp:
        def read(self):
            return json.dumps(payload).encode()

    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=0: _Resp())


def _conditional_pipeline():
    from brokoli import condition_node, notify, transform

    with Pipeline("cond", pipeline_id="cond") as p:
        src = source_file("Read", path="/tmp/in.csv", format="csv")
        gate = condition_node("Gate", expression="row_count > 0")
        src >> gate
        gate.when(transform("Keep", rules=[{"type": "rename", "mapping": {"a": "b"}}]))
        gate.otherwise(notify("Alert", notify_type="webhook", webhook_url="https://h.example/x"))
    return p


def _catchup_pipeline():
    with Pipeline("cu", pipeline_id="cu", schedule="0 * * * *", catch_up=True) as p:
        src = source_file("Read", path="/tmp/in.csv", format="csv")
        src >> sink_file("Save", path="/tmp/out.csv", format="csv")
    return p


class _StaticPayloadPipeline:
    """A pipeline stub whose to_json() is a hand-built payload -- for
    exercising the feature gate against a config shape (task_bundle) that
    does not need a real project on disk to test the gating logic itself."""

    def __init__(self, name, node_config):
        self.name = name
        self._node_config = node_config

    def to_json(self):
        return {
            "name": self.name,
            "ir_version": "2.0",
            "nodes": [{"id": "n1", "type": "code", "name": "N", "config": self._node_config}],
            "edges": [],
        }


def _task_bundle_pipeline():
    return _StaticPayloadPipeline(
        "bundle",
        {
            "language": "python",
            "task_bundle": {"digest": "sha256:" + "0" * 64, "format": "task-bundle/1"},
        },
    )


def _emit_pipeline():
    return _StaticPayloadPipeline(
        "emit", {"language": "python", "script": "begin_emit(['a'])\nfor r in rows:\n    emit(r)\n"}
    )


def _paginated_pipeline():
    with Pipeline("paged", pipeline_id="paged") as p:
        src = source_api(
            "Fetch",
            url="https://api.example.com/x",
            pagination=offset_pages(page_size=10).with_execution(page_max_retries=2),
        )
        src >> sink_file("Save", path="/tmp/out.csv", format="csv")
    return p


class TestRequiredFeatures:
    def test_conditional_and_pagination_and_union_detected(self):
        p = _conditional_pipeline()
        assert required_execution_features(p.to_json()) == {"conditional-routing"}

        paged = _paginated_pipeline()
        assert required_execution_features(paged.to_json()) == {"pagination-checkpoints"}

        with Pipeline("u", pipeline_id="u") as up:
            a = source_file("A", path="/a.csv", format="csv")
            b = source_file("B", path="/b.csv", format="csv")
            union("Merge", a, b) >> sink_file("S", path="/o.csv", format="csv")
        assert required_execution_features(up.to_json()) == {"union"}

    def test_emit_scripts_require_code_streaming_emit(self):
        # ADR-029: emit()/begin_emit() names don't exist on servers whose
        # wrapper predates the streaming idiom -- refuse at deploy, not
        # at run time.
        from brokoli import code

        with Pipeline("e", pipeline_id="e") as p:
            src = source_file("In", path="/a.csv", format="csv")
            code("Stream", input=src, script="begin_emit(['a'])\nfor r in rows:\n    emit(r)\n")
        assert "code-streaming-emit" in required_execution_features(p.to_json())

        with Pipeline("plain", pipeline_id="plain") as p2:
            src = source_file("In", path="/a.csv", format="csv")
            code("Old", input=src, script="output_data = {'columns': columns, 'rows': rows}")
        assert "code-streaming-emit" not in required_execution_features(p2.to_json())

    def test_catch_up_requires_data_intervals(self):
        p = _catchup_pipeline()
        assert required_execution_features(p.to_json()) == {"data_intervals"}

    def test_plain_pipeline_requires_nothing(self):
        with Pipeline("plain", pipeline_id="plain") as p:
            src = source_file("Read", path="/tmp/in.csv", format="csv")
            src >> sink_file("Save", path="/tmp/out.csv", format="csv")
        assert required_execution_features(p.to_json()) == set()

    def test_task_interface_and_pipeline_parameters_require_task_interface_v1(self):
        # ADR-032 rollout step 3: a node's inferred "interface" and a
        # pipeline's inferred "parameters" both need the same gate a
        # server old enough to predate IR 2.2 cannot advertise.
        from typing import TypedDict

        from brokoli import task

        class Row(TypedDict):
            id: int

        with Pipeline("typed", pipeline_id="typed") as p:

            @task
            def score(rows: list[Row], threshold: float = 0.5) -> list[Row]:
                return rows

            score()
        assert required_execution_features(p.to_json()) == {"task-interface-v1"}


class TestFeatureGating:
    FULL = [
        "conditional-routing",
        "dynamic-expansion",
        "union",
        "pagination-checkpoints",
        "data_intervals",
    ]

    def test_absent_field_skips_feature_gating(self, monkeypatch):
        _serve_capabilities(monkeypatch, {"supported_ir_versions": ["2.0", "2.1"]})
        preflight_server_compatibility([_conditional_pipeline()], "http://s")

    def test_absent_field_still_refuses_task_bundles(self, monkeypatch):
        # A server old enough to omit supported_execution_features entirely
        # cannot have ADR-031's mount machinery either -- absence must not
        # be read as "predates the field but would still run this".
        _serve_capabilities(monkeypatch, {"supported_ir_versions": ["2.0", "2.1"]})
        with pytest.raises(CompatibilityError, match="task-bundles"):
            preflight_server_compatibility([_task_bundle_pipeline()], "http://s")

    def test_absent_field_still_refuses_streaming_emit(self, monkeypatch):
        _serve_capabilities(monkeypatch, {"supported_ir_versions": ["2.0", "2.1"]})
        with pytest.raises(CompatibilityError, match="code-streaming-emit"):
            preflight_server_compatibility([_emit_pipeline()], "http://s")

    def test_absent_field_cannot_be_overridden_for_runtime_features(self, monkeypatch):
        # allow_legacy_server only relaxes the case where the capabilities
        # ENDPOINT itself is unreachable (404/405 or a connection failure).
        # Here the endpoint answers normally -- it simply omits the field --
        # so the flag has nothing to relax, and the runtime-existence gate
        # applies regardless of how the flag is set.
        _serve_capabilities(monkeypatch, {"supported_ir_versions": ["2.0", "2.1"]})
        with pytest.raises(CompatibilityError, match="task-bundles"):
            preflight_server_compatibility(
                [_task_bundle_pipeline()], "http://s", allow_legacy_server=True
            )

    def test_advertised_features_pass(self, monkeypatch):
        _serve_capabilities(
            monkeypatch,
            {"supported_ir_versions": ["2.0", "2.1"], "supported_execution_features": self.FULL},
        )
        preflight_server_compatibility([_conditional_pipeline(), _paginated_pipeline()], "http://s")

    def test_missing_feature_fails_naming_it(self, monkeypatch):
        _serve_capabilities(
            monkeypatch,
            {"supported_ir_versions": ["2.0", "2.1"], "supported_execution_features": ["union"]},
        )
        with pytest.raises(CompatibilityError, match="conditional-routing"):
            preflight_server_compatibility([_conditional_pipeline()], "http://s")

    def test_catch_up_refused_by_pre_interval_server(self, monkeypatch):
        # A server between v0.10.11 (feature advertising) and data
        # intervals: advertises features, not this one. The refusal names
        # the feature, BEFORE the server's fail-closed decoder would 400
        # on the unknown catchup field.
        _serve_capabilities(
            monkeypatch,
            {
                "supported_ir_versions": ["2.0", "2.1"],
                "supported_execution_features": ["union", "conditional-routing"],
            },
        )
        with pytest.raises(CompatibilityError, match="data_intervals"):
            preflight_server_compatibility([_catchup_pipeline()], "http://s")

    def test_catch_up_accepted_by_advertising_server(self, monkeypatch):
        _serve_capabilities(
            monkeypatch,
            {"supported_ir_versions": ["2.0", "2.1"], "supported_execution_features": self.FULL},
        )
        preflight_server_compatibility([_catchup_pipeline()], "http://s")

    def test_legacy_flag_cannot_override_feature_mismatch(self, monkeypatch):
        _serve_capabilities(
            monkeypatch,
            {"supported_ir_versions": ["2.0", "2.1"], "supported_execution_features": []},
        )
        with pytest.raises(CompatibilityError, match="cannot override"):
            preflight_server_compatibility(
                [_paginated_pipeline()], "http://s", allow_legacy_server=True
            )

    def test_malformed_feature_field_fails_closed(self, monkeypatch):
        _serve_capabilities(
            monkeypatch,
            {"supported_ir_versions": ["2.0"], "supported_execution_features": [1, ""]},
        )
        with pytest.raises(CompatibilityError, match="supported_execution_features"):
            preflight_server_compatibility([], "http://s")

    def test_dataset_ops_blocked_on_feature_advertising_servers(self, monkeypatch):
        # The server deliberately does not advertise dataset-map/filter --
        # its runtime rejects SDK-emitted configs -- so a gating client
        # refuses them at deploy instead of failing at run time.
        payload = {
            "name": "ds",
            "ir_version": "2.0",
            "nodes": [
                {
                    "id": "m",
                    "type": "dataset_map",
                    "name": "M",
                    "config": {"function": {"name": "f"}},
                }
            ],
            "edges": [],
        }
        assert required_execution_features(payload) == {"dataset-map"}
