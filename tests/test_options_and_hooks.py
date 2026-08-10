"""sdk#23 M2: serialize-or-reject constructor options, and real hooks.

catch_up/max_retries/concurrency were accepted for years and silently
dropped from the compiled IR; the server has no fields for them and its
strict decoder (v0.10.11) would reject them anyway. They now raise at
construction with the honest state.

Lifecycle hooks used to accept Python callables and emit an empty
webhook placeholder, discarding the callable. The server persists hooks
since v0.10.10, so hooks are real now: a URL string becomes a webhook
hook, a dict carries type/url, and callables are rejected with guidance.
"""

import json
from pathlib import Path

import pytest

from brokoli import Pipeline, sink_file, source_file
from brokoli.exceptions import PipelineError


class TestUnsupportedOptionsRejected:
    @pytest.mark.parametrize("option", ["catch_up", "max_retries", "concurrency"])
    def test_option_raises_naming_itself(self, option):
        with pytest.raises(PipelineError, match=option):
            Pipeline("p", **{option: 1})

    def test_defaults_still_fine(self):
        with Pipeline("p") as pipe:
            src = source_file("A", path="/a.csv", format="csv")
            src >> sink_file("B", path="/b.csv", format="csv")
        payload = pipe.to_json()
        for absent in ("catch_up", "max_retries", "concurrency"):
            assert absent not in payload


class TestRealHooks:
    def _pipeline(self, **kwargs):
        with Pipeline("hooked", **kwargs) as p:
            src = source_file("A", path="/a.csv", format="csv")
            src >> sink_file("B", path="/b.csv", format="csv")
        return p.to_json()

    def test_url_string_becomes_webhook_hook(self):
        payload = self._pipeline(on_failure="https://hooks.example/fail")
        assert payload["hooks"]["on_failure"] == {
            "type": "webhook", "url": "https://hooks.example/fail", "enabled": True,
        }

    def test_dict_hook_passes_through(self):
        payload = self._pipeline(
            on_success={"type": "slack", "url": "https://hooks.slack.example/x",
                        "extra": {"channel": "wins"}}
        )
        hook = payload["hooks"]["on_success"]
        assert hook["type"] == "slack" and hook["extra"] == {"channel": "wins"}

    def test_callable_rejected_with_guidance(self):
        with pytest.raises(PipelineError, match="callable"):
            Pipeline("p", on_failure=lambda ctx: None)

    def test_bad_hook_type_rejected(self):
        with pytest.raises(PipelineError, match="type"):
            Pipeline("p", on_start={"type": "carrier-pigeon", "url": "https://x"})

    def test_emitted_hooks_validate_against_canonical_schema(self):
        jsonschema = pytest.importorskip("jsonschema")
        schema = json.loads(
            (Path(__file__).parent / "fixtures" / "pipeline-ir-2.1.json").read_text()
        )
        jsonschema.validate(self._pipeline(on_failure="https://h.example/f"), schema)


class TestJoinArity:
    def test_single_input_join_is_an_error(self):
        from brokoli import join, source_db
        from brokoli.validation import validate_pipeline

        with Pipeline("j") as p:
            a = source_db("A", conn_id="c", query="select 1")
            b = source_db("B", conn_id="c", query="select 2")
            j = join("J", left=a, right=b, on="id")
            # Sever one input behind the API's back (legacy graph shape).
            p._edges = [e for e in p._edges if e[0] != b.node_id]
        vr = validate_pipeline(p)
        assert any("exactly 2 inputs" in str(e) for e in vr.errors)
