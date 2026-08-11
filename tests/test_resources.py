"""brokoli-sdk#15 M4: typed resource references.

`Connection` is an explicit, typed alternative to an opaque conn_id string.
It compiles to the bare name on the wire, so it's interchangeable with the
string while being a distinct authoring-time type.
"""

import pytest

from brokoli import (
    Pipeline,
    source_db,
    sink_db,
    source_api,
    migrate,
    Connection,
    ResourceRef,
    DatasetRef,
    ir_digest,
)


def _cfg(p, name):
    return [n for n in p.to_json()["nodes"] if n["name"] == name][0]["config"]


class TestConnectionConstruction:
    def test_valid_name(self):
        assert Connection("warehouse").name == "warehouse"
        assert Connection("dw-prod_1.eu").name == "dw-prod_1.eu"

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="non-empty string"):
            Connection("")

    def test_bad_characters_rejected(self):
        with pytest.raises(ValueError, match="may contain only"):
            Connection("has spaces")
        with pytest.raises(ValueError, match="may contain only"):
            Connection("has/slash")

    def test_repr_and_str(self):
        c = Connection("warehouse")
        assert repr(c) == "Connection('warehouse')"
        assert str(c) == "warehouse"

    def test_is_a_resource_ref_not_a_data_ref(self):
        c = Connection("warehouse")
        assert isinstance(c, ResourceRef)
        assert not isinstance(c, DatasetRef)


class TestConnectionEquality:
    def test_distinct_from_plain_string(self):
        assert Connection("warehouse") != "warehouse"

    def test_equal_to_same_connection(self):
        assert Connection("warehouse") == Connection("warehouse")
        assert hash(Connection("warehouse")) == hash(Connection("warehouse"))

    def test_unequal_to_different_name(self):
        assert Connection("a") != Connection("b")


class TestConnectionCompilesToString:
    def test_source_and_sink_db(self):
        with Pipeline("t", pipeline_id="t") as p:
            raw = source_db("Extract", query="SELECT 1", conn_id=Connection("warehouse"))
            sink_db("Load", input=raw, table="t", conn_id=Connection("warehouse"))
        assert _cfg(p, "Extract")["conn_id"] == "warehouse"
        assert _cfg(p, "Load")["conn_id"] == "warehouse"

    def test_source_api(self):
        with Pipeline("t", pipeline_id="t") as p:
            source_api("Fetch", url="https://x", conn_id=Connection("api-creds"))
        assert _cfg(p, "Fetch")["conn_id"] == "api-creds"

    def test_migrate_both_sides(self):
        with Pipeline("t", pipeline_id="t") as p:
            migrate(
                "Move",
                query="SELECT 1",
                table="dst",
                source_conn_id=Connection("oltp"),
                target_conn_id=Connection("warehouse"),
            )
        cfg = _cfg(p, "Move")
        assert cfg["source_conn_id"] == "oltp"
        assert cfg["dest_conn_id"] == "warehouse"

    def test_wire_equivalent_to_string(self):
        # The whole point: Connection and the string it names produce the
        # exact same IR, so the digest is identical.
        def build(conn):
            with Pipeline("orders", pipeline_id="orders") as p:
                source_db("Extract", query="SELECT 1", conn_id=conn)
            return p

        assert ir_digest(build(Connection("warehouse")).to_json()) == ir_digest(
            build("warehouse").to_json()
        )


class TestInterpolationRefs:
    def test_compile_to_namespaced_tokens(self):
        from brokoli import Secret, Variable, Param, EnvVar
        assert Secret("api_token").ir_value() == "${secret.api_token}"
        assert Variable("region").ir_value() == "${var.region}"
        assert Param("day").ir_value() == "${param.day}"
        assert EnvVar("HOME").ir_value() == "${env.HOME}"

    def test_str_is_the_token_for_fstring_embedding(self):
        from brokoli import Param
        assert f"https://api/{Param('date')}/x" == "https://api/${param.date}/x"

    def test_full_value_in_nested_dict_normalizes(self):
        from brokoli import Pipeline, source_api, Secret
        with Pipeline("t", pipeline_id="t") as p:
            source_api("Fetch", url="https://x",
                       headers={"Authorization": Secret("token")})
        assert _cfg(p, "Fetch")["headers"]["Authorization"] == "${secret.token}"

    def test_embedded_in_string_passes_through(self):
        from brokoli import Pipeline, source_db, Variable
        with Pipeline("t", pipeline_id="t") as p:
            source_db("Q", query=f"SELECT 1 WHERE r='{Variable('region')}'")
        assert _cfg(p, "Q")["query"] == "SELECT 1 WHERE r='${var.region}'"

    def test_distinct_types(self):
        from brokoli import Secret, Variable, Param
        assert Secret("x") != Variable("x")
        assert Secret("x") != Param("x")
        assert Secret("x") == Secret("x")

    def test_name_validated(self):
        from brokoli import Secret
        with pytest.raises(ValueError, match="may contain only"):
            Secret("has space")
