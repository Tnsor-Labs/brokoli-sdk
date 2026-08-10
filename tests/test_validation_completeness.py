"""sdk#23 M1: catch locally what the server rejects (or silently coerces).

- Cycles: the server rejects them at save since v0.10.10; locally we name
  the members instead of surfacing a deploy-time 400.
- Enums: the engine SILENTLY COERCES out-of-set values (unknown join_type
  becomes inner, unknown sink_file format becomes json) -- transcribed
  sets from the engine's own switches, so a typo is an error here rather
  than quietly different behavior there.
- Fan-in pairing: `[a, b] >> [c]` used to zip-truncate, silently dropping
  edges.
"""

import pytest

from brokoli import Pipeline, join, notify, sink_file, source_db, source_file
from brokoli.exceptions import PipelineError
from brokoli.validation import validate_pipeline


def _errors(p):
    return [str(e) for e in validate_pipeline(p).errors]


class TestCycleDetection:
    def test_cycle_is_an_error_naming_members(self):
        with Pipeline("cyclic") as p:
            a = source_file("A", path="/a.csv", format="csv")
            b = sink_file("B", path="/b.csv", format="csv")
            c = sink_file("C", path="/c.csv", format="csv")
            a >> b
            b >> c
            # Close the loop behind the operator API's back, as a legacy
            # or hand-mutated graph could. Internal edges are
            # (from, to, condition) tuples.
            p._edges.append((c.node_id, b.node_id, None))
        errs = " ".join(_errors(p))
        assert "cycle" in errs
        assert b.node_id in errs and c.node_id in errs
        assert a.node_id not in errs.split("cycle")[1]  # acyclic head not blamed

    def test_acyclic_pipeline_clean(self):
        with Pipeline("fine") as p:
            src = source_file("A", path="/a.csv", format="csv")
            src >> sink_file("B", path="/b.csv", format="csv")
        assert not any("cycle" in e for e in _errors(p))


class TestEnumValidation:
    def test_join_type_typo_caught(self):
        with Pipeline("j") as p:
            a = source_db("A", conn_id="c", query="select 1")
            b = source_db("B", conn_id="c", query="select 2")
            join("J", left=a, right=b, on="id", how="lefft")
        errs = " ".join(_errors(p))
        assert "join_type" in errs and "lefft" in errs

    def test_notify_type_email_not_supported(self):
        # The engine's notify switch implements slack and webhook only.
        with Pipeline("n") as p:
            src = source_file("A", path="/a.csv", format="csv")
            src >> notify("N", notify_type="email", webhook_url="https://h.example/x")
        errs = " ".join(_errors(p))
        assert "notify_type" in errs and "email" in errs

    def test_sink_format_parquet_caught(self):
        with Pipeline("f") as p:
            src = source_file("A", path="/a.csv", format="csv")
            src >> sink_file("S", path="/o.parquet", format="parquet")
        errs = " ".join(_errors(p))
        assert "format" in errs and "parquet" in errs

    def test_retry_backoff_typo_caught(self):
        with Pipeline("r") as p:
            source_db("A", conn_id="c", query="select 1",
                      retries=2, retry_backoff="expnential")
        errs = " ".join(_errors(p))
        assert "retry_backoff" in errs

    def test_valid_enums_pass(self):
        with Pipeline("ok") as p:
            a = source_db("A", conn_id="c", query="select 1",
                          retries=2, retry_backoff="linear")
            b = source_db("B", conn_id="c", query="select 2")
            j = join("J", left=a, right=b, on="id", how="full_outer")
            j >> sink_file("S", path="/o.json", format="json")
        assert _errors(p) == []


class TestFanInPairing:
    def test_mismatched_fanin_lists_raise(self):
        from brokoli import parallel, transform

        with Pipeline("fan") as p:
            a = source_file("A", path="/a.csv", format="csv")
            b = source_file("B", path="/b.csv", format="csv")
            t = transform("T", rules=[{"type": "rename", "mapping": {"x": "y"}}])
            with pytest.raises(PipelineError, match="one-to-one"):
                parallel(a, b) >> [t]
