"""Tests for source_api's response/record-extraction contract and pagination DSL.

Covers:
- params / response / records / value_path kwargs on source_api, with the
  UNSET-vs-None-vs-omitted discipline established in Phase 0.
- Pagination strategy builders (offset_pages, cursor_pages, numbered_pages,
  next_link_pages, link_header_pages) serializing to a `pagination` config
  block, including meaningful falsy edge cases (e.g. numbered_pages(start=0)).
- `.with_execution(...)` attaching a sibling `execution` config block.
- The RFC's worked GBIF example, end to end.
- New validation.py rules: records/value_path mutual exclusion, pagination
  requiring response="dataset", and unknown pagination strategy names.

Scope reminder: everything here is SDK-side declarative-IR-config
production. None of it executes HTTP requests or expands pagination into
concrete page fetches -- that's backend (physical-planner) work, tracked
separately and not yet started.
"""

from brokoli import (
    Pipeline,
    source_api,
    offset_pages,
    cursor_pages,
    numbered_pages,
    next_link_pages,
    link_header_pages,
)
from brokoli.pagination import PaginationStrategy, ExecutionPolicy
from brokoli.validation import validate_pipeline


# ---------------------------------------------------------------------------
# params
# ---------------------------------------------------------------------------


class TestParams:
    def test_params_static_values_serialize(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://api.example.com/data",
                params={"hasCoordinate": "true", "limit": 50},
            )
        config = p.to_json()["nodes"][0]["config"]
        assert config["params"] == {"hasCoordinate": "true", "limit": 50}

    def test_params_templated_string_passed_through_opaque(self):
        """The SDK does not interpret {{ ds }}-style placeholders itself --
        it passes params through unchanged for the backend/scheduler to
        resolve, same as url/headers/query already do elsewhere."""
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://api.example.com/data",
                params={"date": "{{ ds }}"},
            )
        config = p.to_json()["nodes"][0]["config"]
        assert config["params"] == {"date": "{{ ds }}"}

    def test_params_omitted_not_in_config(self):
        with Pipeline("test") as p:
            source_api("S", url="https://api.example.com/data")
        config = p.to_json()["nodes"][0]["config"]
        assert "params" not in config

    def test_params_mutation_after_call_does_not_affect_config(self):
        """params dict is shallow-copied like headers, guarding against
        caller mutation after the fact."""
        original = {"a": "1"}
        with Pipeline("test") as p:
            source_api("S", url="https://x", params=original)
        original["a"] = "mutated"
        config = p.to_json()["nodes"][0]["config"]
        assert config["params"] == {"a": "1"}


# ---------------------------------------------------------------------------
# response / records / value_path
# ---------------------------------------------------------------------------


class TestResponseShape:
    def test_response_defaults_to_dataset_and_is_always_present(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x")
        config = p.to_json()["nodes"][0]["config"]
        assert config["response"] == "dataset"

    def test_response_scalar_with_value_path(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", response="scalar", value_path="data.count")
        config = p.to_json()["nodes"][0]["config"]
        assert config["response"] == "scalar"
        assert config["value_path"] == "data.count"
        assert "records" not in config

    def test_response_artifact(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", response="artifact")
        config = p.to_json()["nodes"][0]["config"]
        assert config["response"] == "artifact"

    def test_records_dot_path(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", records="data.items")
        config = p.to_json()["nodes"][0]["config"]
        assert config["records"] == "data.items"

    def test_records_and_value_path_omitted_by_default(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x")
        config = p.to_json()["nodes"][0]["config"]
        assert "records" not in config
        assert "value_path" not in config


# ---------------------------------------------------------------------------
# Pagination strategy builders
# ---------------------------------------------------------------------------


class TestOffsetPages:
    def test_full_config(self):
        strat = offset_pages(page_size=300, max_records=30_000, end_flag="endOfRecords")
        assert strat.to_config() == {
            "strategy": "offset",
            "page_size": 300,
            "max_records": 30_000,
            "offset_param": "offset",
            "limit_param": "limit",
            "end_flag": "endOfRecords",
        }

    def test_minimal_config_omits_unset_fields(self):
        strat = offset_pages(page_size=100)
        config = strat.to_config()
        assert config == {
            "strategy": "offset",
            "page_size": 100,
            "offset_param": "offset",
            "limit_param": "limit",
        }
        assert "max_records" not in config
        assert "end_flag" not in config

    def test_custom_param_names(self):
        strat = offset_pages(page_size=50, offset_param="skip", limit_param="take")
        config = strat.to_config()
        assert config["offset_param"] == "skip"
        assert config["limit_param"] == "take"


class TestCursorPages:
    def test_config(self):
        strat = cursor_pages(cursor_path="meta.next_cursor", cursor_param="cursor")
        assert strat.to_config() == {
            "strategy": "cursor",
            "cursor_path": "meta.next_cursor",
            "cursor_param": "cursor",
        }


class TestNumberedPages:
    def test_default_start(self):
        strat = numbered_pages(page_param="page")
        config = strat.to_config()
        assert config["strategy"] == "numbered"
        assert config["page_param"] == "page"
        assert config["start"] == 1
        assert "total_pages_path" not in config

    def test_start_zero_survives(self):
        """start=0 is a meaningful, valid value (zero-indexed pagination) --
        it must survive serialization, not get dropped by a truthy check."""
        strat = numbered_pages(page_param="page", start=0)
        config = strat.to_config()
        assert config["start"] == 0
        assert "start" in config

    def test_total_pages_path(self):
        strat = numbered_pages(page_param="page", total_pages_path="meta.total_pages")
        config = strat.to_config()
        assert config["total_pages_path"] == "meta.total_pages"


class TestNextLinkPages:
    def test_config(self):
        strat = next_link_pages(next_path="paging.next")
        assert strat.to_config() == {"strategy": "next_link", "next_path": "paging.next"}


class TestLinkHeaderPages:
    def test_default_rel(self):
        strat = link_header_pages()
        assert strat.to_config() == {"strategy": "link_header", "rel": "next"}

    def test_custom_rel(self):
        strat = link_header_pages(rel="last")
        assert strat.to_config() == {"strategy": "link_header", "rel": "last"}


# ---------------------------------------------------------------------------
# with_execution
# ---------------------------------------------------------------------------


class TestWithExecution:
    def test_no_execution_by_default(self):
        strat = offset_pages(page_size=100)
        assert strat.execution_config() is None

    def test_with_execution_attaches_policy(self):
        strat = offset_pages(page_size=100).with_execution(
            max_concurrency=4,
            requests_per_second=5,
            retry_scope="page",
            checkpoint_every=10,
        )
        assert strat.execution_config() == {
            "max_concurrency": 4,
            "requests_per_second": 5,
            "retry_scope": "page",
            "checkpoint_every": 10,
        }

    def test_with_execution_partial_fields(self):
        strat = offset_pages(page_size=100).with_execution(max_concurrency=2)
        assert strat.execution_config() == {"max_concurrency": 2}

    def test_with_execution_returns_new_object_original_untouched(self):
        base = offset_pages(page_size=100)
        chained = base.with_execution(max_concurrency=4)
        assert base.execution_config() is None
        assert chained.execution_config() == {"max_concurrency": 4}
        # pagination config itself is unaffected by chaining
        assert base.to_config() == chained.to_config()

    def test_execution_policy_class_directly(self):
        policy = ExecutionPolicy(max_concurrency=3, checkpoint_every=0)
        assert policy.to_config() == {"max_concurrency": 3, "checkpoint_every": 0}

    def test_page_max_retries_and_page_retry_backoff_serialize(self):
        strat = offset_pages(page_size=100).with_execution(
            page_max_retries=5,
            page_retry_backoff="linear",
        )
        assert strat.execution_config() == {
            "page_max_retries": 5,
            "page_retry_backoff": "linear",
        }

    def test_page_max_retries_omitted_by_default(self):
        strat = offset_pages(page_size=100).with_execution(max_concurrency=2)
        config = strat.execution_config()
        assert "page_max_retries" not in config
        assert "page_retry_backoff" not in config


# ---------------------------------------------------------------------------
# pagination= wired into source_api
# ---------------------------------------------------------------------------


class TestSourceApiPagination:
    def test_pagination_strategy_serializes_into_node_config(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                records="results",
                pagination=offset_pages(page_size=300, end_flag="endOfRecords"),
            )
        config = p.to_json()["nodes"][0]["config"]
        assert config["pagination"] == {
            "strategy": "offset",
            "page_size": 300,
            "offset_param": "offset",
            "limit_param": "limit",
            "end_flag": "endOfRecords",
        }
        assert "execution" not in config

    def test_pagination_with_execution_serializes_execution_block(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                records="results",
                pagination=offset_pages(page_size=300).with_execution(max_concurrency=4),
            )
        config = p.to_json()["nodes"][0]["config"]
        assert config["pagination"]["strategy"] == "offset"
        assert config["execution"] == {"max_concurrency": 4}

    def test_pagination_omitted_not_in_config(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x")
        config = p.to_json()["nodes"][0]["config"]
        assert "pagination" not in config
        assert "execution" not in config

    def test_pagination_accepts_raw_dict(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                records="results",
                pagination={"strategy": "cursor", "cursor_path": "next", "cursor_param": "c"},
            )
        config = p.to_json()["nodes"][0]["config"]
        assert config["pagination"] == {
            "strategy": "cursor",
            "cursor_path": "next",
            "cursor_param": "c",
        }

    def test_pagination_rejects_bad_type(self):
        import pytest

        with Pipeline("test") as p:
            with pytest.raises(TypeError):
                source_api("S", url="https://x", pagination="offset")


# ---------------------------------------------------------------------------
# RFC worked example: GBIF
# ---------------------------------------------------------------------------


class TestGBIFWorkedExample:
    def test_gbif_example_single_node_no_page_loop(self):
        with Pipeline("GBIF Biodiversity Monitor") as p:
            source_api(
                "GBIF Occurrences",
                url="https://api.gbif.org/v1/occurrence/search",
                params={"hasCoordinate": "true", "occurrenceStatus": "PRESENT"},
                records="results",
                pagination=offset_pages(
                    page_size=300,
                    max_records=30_000,
                    end_flag="endOfRecords",
                ).with_execution(max_concurrency=4, requests_per_second=5),
            )

        data = p.to_json()
        assert len(data["nodes"]) == 1  # no hand-written page loop -> one logical node

        node = data["nodes"][0]
        config = node["config"]
        assert config["url"] == "https://api.gbif.org/v1/occurrence/search"
        assert config["response"] == "dataset"
        assert config["records"] == "results"
        assert config["params"] == {"hasCoordinate": "true", "occurrenceStatus": "PRESENT"}
        assert config["pagination"] == {
            "strategy": "offset",
            "page_size": 300,
            "max_records": 30_000,
            "offset_param": "offset",
            "limit_param": "limit",
            "end_flag": "endOfRecords",
        }
        assert config["execution"] == {"max_concurrency": 4, "requests_per_second": 5}
        assert "source" in node["capabilities"]
        assert "dataset-output" in node["capabilities"]

        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidateResponseShape:
    def test_invalid_response_kind_rejected(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x")
            p._nodes[p._node_order[0]]["config"]["response"] = "bogus"
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("response" in e.field for e in vr.errors)

    def test_records_and_value_path_together_rejected(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", records="results", value_path="count")
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("records" in e.field and "value_path" in e.message for e in vr.errors)

    def test_records_alone_is_valid(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", records="results")
        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]

    def test_value_path_alone_is_valid(self):
        with Pipeline("test") as p:
            source_api("S", url="https://x", response="scalar", value_path="count")
        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]


class TestValidatePagination:
    def test_pagination_without_dataset_response_rejected(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                response="scalar",
                value_path="count",
                pagination=offset_pages(page_size=10),
            )
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any("pagination" in e.field for e in vr.errors)

    def test_pagination_with_dataset_response_is_valid(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                records="results",
                pagination=offset_pages(page_size=10),
            )
        vr = validate_pipeline(p)
        assert vr.valid, [str(e) for e in vr.errors]

    def test_unknown_pagination_strategy_rejected(self):
        with Pipeline("test") as p:
            source_api(
                "S",
                url="https://x",
                records="results",
                pagination={"strategy": "not_a_real_strategy"},
            )
        vr = validate_pipeline(p)
        assert not vr.valid
        assert any(
            "pagination" in e.field and "not_a_real_strategy" in e.message for e in vr.errors
        )

    def test_known_strategies_all_accepted(self):
        strategies = [
            offset_pages(page_size=10),
            cursor_pages(cursor_path="next", cursor_param="c"),
            numbered_pages(page_param="page"),
            next_link_pages(next_path="paging.next"),
            link_header_pages(),
        ]
        for strat in strategies:
            with Pipeline("test") as p:
                source_api("S", url="https://x", records="results", pagination=strat)
            vr = validate_pipeline(p)
            assert vr.valid, (strat, [str(e) for e in vr.errors])
