"""Tests for brokoli-sdk#22 M1: constructs that used to deploy green and
fail remotely must now fail at authoring time with a named, actionable
error.

Three families:
  - async def / lambda tasks: source extraction only captures from a
    ``def `` line, so these produced a deployed script with *no function
    in it* -- a remote NameError. Now rejected locally, including for
    async helpers pulled in by auto-packaging.
  - float('inf') / float('nan') constants: pass the default json.dumps
    serializability check but are emitted via repr() as bare ``inf`` /
    ``nan`` -- not valid Python literals. Now routed into the existing
    names-the-symbol packaging error.
  - ParseError: there were two unrelated classes with this name
    (brokoli.exceptions vs brokoli.parsing); catching the exceptions one
    never caught what quality_check actually raised. Now one class.
"""

import pytest

import module_packaging_fixtures as fx
from brokoli import Pipeline, task
from brokoli.exceptions import PipelineError


class TestAsyncAndLambdaRejection:
    def test_async_task_rejected_with_named_error(self):
        with Pipeline("test"):
            async def fetch(rows):
                return rows

            t = task(fetch)
            with pytest.raises(PipelineError, match="async"):
                t()

    def test_lambda_task_rejected(self):
        with Pipeline("test"):
            t = task(lambda rows: rows)
            with pytest.raises(PipelineError, match="[Ll]ambda"):
                t()

    def test_async_helper_referenced_by_sync_task_rejected(self):
        # The helper flows through the same extraction as the task itself;
        # silently emitting a script without it would NameError remotely.
        with Pipeline("test"):
            t = task(fx.clean_with_async_helper)
            with pytest.raises(PipelineError, match="async"):
                t()

    def test_plain_def_still_works(self):
        with Pipeline("test") as p:
            t = task(fx.clean_no_refs)
            t()
        node = p.to_json()["nodes"][0]
        assert "def clean_no_refs" in node["config"]["script"]


class TestNonFiniteConstantRejection:
    def test_infinity_constant_fails_locally_naming_the_symbol(self):
        with Pipeline("test"):
            t = task(fx.clean_with_inf_ref)
            with pytest.raises(PipelineError, match="INFINITY_THRESHOLD"):
                t()

    def test_nested_nan_fails_locally_naming_the_symbol(self):
        with Pipeline("test"):
            t = task(fx.clean_with_nested_nan_ref)
            with pytest.raises(PipelineError, match="NESTED_NAN"):
                t()

    def test_finite_float_constants_still_package(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_constant)
            t()
        node = p.to_json()["nodes"][0]
        assert "package" in node["config"]


class TestParseErrorIsOneClass:
    def test_exceptions_and_parsing_share_the_class(self):
        from brokoli import exceptions, parsing

        assert exceptions.ParseError is parsing.ParseError

    def test_quality_rule_failure_caught_via_exceptions_module(self):
        from brokoli.exceptions import BrokoliError, ParseError
        from brokoli.parsing import parse_quality_rule

        with pytest.raises(ParseError) as exc_info:
            parse_quality_rule("definitely not a rule ###")
        assert isinstance(exc_info.value, BrokoliError)
        assert exc_info.value.rule_string == "definitely not a rule ###"
