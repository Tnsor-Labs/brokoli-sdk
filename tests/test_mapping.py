"""map=Map(...): running a node once per item of an upstream collection
(ADR-050 phase 1), and the deploy gate that keeps it off a server that
would ignore it."""

import json
import urllib.request

import pytest

from brokoli import Map, Pipeline, code, source_api, source_db, task
from brokoli.compatibility import preflight_server_compatibility, required_execution_features
from brokoli.exceptions import CompatibilityError, PipelineError


def _node(p, node_id):
    return next(n for n in p.to_json()["nodes"] if n["id"] == node_id)


def _edges(p):
    return {(e["from"], e["to"]) for e in p.to_json()["edges"]}


def _score(rows):
    return rows


class TestCompile:
    def test_source_db_maps_over_an_upstream_with_every_setting(self):
        with Pipeline("m", pipeline_id="m") as p:
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            orders = source_db(
                "Orders",
                query="SELECT * FROM o WHERE customer_id = ${item.id}",
                conn_id="shop",
                map=Map(
                    over=customers,
                    key="id",
                    max_items=500,
                    max_parallel=8,
                    on_item_failure="continue",
                ),
            )
        assert _node(p, orders.node_id)["config"]["map"] == {
            "over": customers.node_id,
            "key": "id",
            "max_items": 500,
            "max_parallel": 8,
            "on_item_failure": "continue",
        }
        assert (customers.node_id, orders.node_id) in _edges(p)

    def test_only_the_settings_given_are_written(self):
        with Pipeline("m", pipeline_id="m") as p:
            ids = source_db("Ids", query="SELECT id FROM t", conn_id="db")
            page = source_api("Page", url="https://api.example/items/${item.id}", map=Map(over=ids))
        assert _node(p, page.node_id)["config"]["map"] == {"over": ids.node_id}

    def test_code_zips_collections_and_adds_one_edge_each(self):
        with Pipeline("m", pipeline_id="m") as p:
            files = source_db("Files", query="SELECT f FROM files", conn_id="db")
            schemas = source_db("Schemas", query="SELECT s FROM schemas", conn_id="db")
            parsed = code(
                "Parse",
                input=files,
                script="output_data = {'columns': columns, 'rows': rows}",
                map=Map(over={"file": files, "schema": schemas}),
            )
        assert _node(p, parsed.node_id)["config"]["map"] == {
            "over": {"file": files.node_id, "schema": schemas.node_id}
        }
        to_parse = [e for e in p.to_json()["edges"] if e["to"] == parsed.node_id]
        assert sorted(e["from"] for e in to_parse) == sorted([files.node_id, schemas.node_id])

    def test_a_task_call_maps(self):
        with Pipeline("m", pipeline_id="m") as p:
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            scored = task(_score)(customers, map=Map(over=customers, key="id"))
        node = _node(p, scored.node_id)
        assert node["type"] == "code"
        assert node["config"]["map"] == {"over": customers.node_id, "key": "id"}
        assert [e for e in p.to_json()["edges"] if e["to"] == scored.node_id] == [
            {"from": customers.node_id, "to": scored.node_id}
        ]

    def test_mapped_and_plain_calls_of_one_task_are_separate_nodes(self):
        with Pipeline("m", pipeline_id="m") as p:
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            score = task(_score)
            mapped = score(customers, map=Map(over=customers))
            plain = score(customers)
        assert mapped.node_id != plain.node_id
        assert "map" not in _node(p, plain.node_id)["config"]

    def test_map_alone_is_not_answered_with_the_unmapped_node(self):
        # A task called with no inputs is registered once and reused; a
        # later call with only map= must register the mapped node, not
        # hand back the unmapped one.
        with Pipeline("m", pipeline_id="m") as p:
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            score = task(_score)
            plain = score()
            mapped = score(map=Map(over=customers))
        assert mapped.node_id != plain.node_id
        assert _node(p, mapped.node_id)["config"]["map"] == {"over": customers.node_id}


class TestRefusals:
    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"over": "customers"}, "upstream node or a dict"),
            ({"over": {}}, "at least one collection"),
            ({"key": ""}, "column name"),
            ({"max_items": 0}, "at least 1"),
            ({"max_parallel": True}, "at least 1"),
            ({"max_parallel": 2.5}, "at least 1"),
            ({"on_item_failure": "retry"}, "fail, continue, skip"),
        ],
    )
    def test_a_bad_setting_is_refused_when_written(self, kwargs, message):
        with Pipeline("m", pipeline_id="m"):
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            args = {"over": customers, **kwargs}
            with pytest.raises(PipelineError, match=message):
                Map(**args)

    def test_a_node_of_another_pipeline_is_refused_and_nothing_is_registered(self):
        with Pipeline("a", pipeline_id="a"):
            elsewhere = source_db("Elsewhere", query="SELECT 1", conn_id="db")
        with Pipeline("b", pipeline_id="b") as p:
            with pytest.raises(PipelineError):
                source_db("Orders", query="SELECT 1", conn_id="db", map=Map(over=elsewhere))
        assert p.to_json()["nodes"] == []

    def test_map_must_be_a_map(self):
        with Pipeline("m", pipeline_id="m"):
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            with pytest.raises(PipelineError, match="brokoli.Map"):
                code("C", input=customers, script="pass", map={"over": customers})

    def test_a_bundled_task_cannot_map_yet(self):
        # The server runs a mapped code node from its script; a bundle
        # would deploy and then fail at run time.
        with Pipeline("m", pipeline_id="m"):
            bundled = task(package="bundle")(_score)
            customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
            with pytest.raises(PipelineError, match="package='bundle'"):
                bundled(customers, map=Map(over=customers))


def _serve(monkeypatch, payload):
    class _Resp:
        def read(self):
            return json.dumps(payload).encode()

    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=0: _Resp())


def _mapped_pipeline():
    with Pipeline("mapped", pipeline_id="mapped") as p:
        customers = source_db("Customers", query="SELECT id FROM c", conn_id="shop")
        source_db(
            "Orders",
            query="SELECT * FROM o WHERE id = ${item.id}",
            conn_id="shop",
            map=Map(over=customers),
        )
    return p


_MAPPING_SERVER = {
    "supported_ir_versions": ["2.0", "2.1", "2.2"],
    "supported_execution_features": ["node-map"],
    "map_node_types": ["code", "source_api", "source_db", "task"],
}


class TestDeployGate:
    def test_a_mapped_node_requires_node_map(self):
        assert "node-map" in required_execution_features(_mapped_pipeline().to_json())

    def test_a_server_that_can_map_accepts_it(self, monkeypatch):
        _serve(monkeypatch, _MAPPING_SERVER)
        preflight_server_compatibility([_mapped_pipeline()], "http://x")

    def test_a_server_without_node_map_refuses_it(self, monkeypatch):
        # Such a server would ignore the block and run the node once.
        _serve(
            monkeypatch,
            {**_MAPPING_SERVER, "supported_execution_features": [], "map_node_types": None},
        )
        with pytest.raises(CompatibilityError, match="node-map"):
            preflight_server_compatibility([_mapped_pipeline()], "http://x")

    def test_a_server_too_old_to_list_features_refuses_it(self, monkeypatch):
        _serve(monkeypatch, {"supported_ir_versions": ["2.0", "2.1", "2.2"]})
        with pytest.raises(CompatibilityError, match="node-map"):
            preflight_server_compatibility(
                [_mapped_pipeline()], "http://x", allow_legacy_server=True
            )

    def test_a_type_the_server_cannot_map_is_refused(self, monkeypatch):
        _serve(monkeypatch, {**_MAPPING_SERVER, "map_node_types": ["code", "task"]})
        with pytest.raises(CompatibilityError, match="cannot map source_db nodes"):
            preflight_server_compatibility([_mapped_pipeline()], "http://x")

    def test_a_malformed_map_node_types_fails_closed(self, monkeypatch):
        _serve(monkeypatch, {**_MAPPING_SERVER, "map_node_types": "code"})
        with pytest.raises(CompatibilityError, match="map_node_types"):
            preflight_server_compatibility([_mapped_pipeline()], "http://x")

    def test_a_pipeline_that_does_not_map_needs_nothing_new(self, monkeypatch):
        with Pipeline("plain", pipeline_id="plain") as p:
            source_db("Customers", query="SELECT id FROM c", conn_id="shop")
        assert "node-map" not in required_execution_features(p.to_json())
        _serve(
            monkeypatch,
            {**_MAPPING_SERVER, "supported_execution_features": [], "map_node_types": None},
        )
        preflight_server_compatibility([p], "http://x")
