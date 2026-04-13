"""Regression tests for the transform rename rule shape.

Background: the SDK's transform() docstring originally showed rename
rules as

    {"type": "rename", "from": "ts", "to": "event_time"}

but the backend's engine.TransformRule expects

    {"type": "rename", "mapping": {"ts": "event_time"}}

So any user who copied the docstring example ended up with a rule
that serialised to JSON with an empty Mapping on the Go side, and
the pipeline failed at run time with

    rule 1 (rename): transform #1 (rename): rename_columns requires mapping

The UI had the same bug in its "Data Quality" template (fixed in
OSS v0.7.5). The SDK side is fixed by:

1. Correcting the docstring to show the canonical shape.
2. _parse_transform_rules() normalising the {from, to} shorthand to
   {mapping: {from: to}} as a backwards-compat convenience.
3. _parse_transform_rules() rejecting rename rules that have neither
   the canonical nor the shorthand form, with a clear ValueError —
   so the bug fails at Pipeline build time instead of run time.

This test file locks in all three behaviours.
"""

import pytest

from brokoli import Pipeline, transform
from brokoli.nodes import _parse_transform_rules


class TestRenameShorthandNormalisation:
    """Accept the {from, to} shorthand and rewrite it to {mapping}."""

    def test_from_to_shorthand_is_normalised(self):
        out = _parse_transform_rules([
            {"type": "rename", "from": "hire_date", "to": "start_date"},
        ])
        assert out == [{"type": "rename", "mapping": {"hire_date": "start_date"}}]

    def test_rename_columns_alias_also_normalised(self):
        out = _parse_transform_rules([
            {"type": "rename_columns", "from": "x", "to": "y"},
        ])
        assert out == [{"type": "rename_columns", "mapping": {"x": "y"}}]

    def test_canonical_shape_is_untouched(self):
        canonical = {"type": "rename", "mapping": {"a": "b", "c": "d"}}
        out = _parse_transform_rules([canonical])
        assert out == [canonical]
        # defensive copy — caller's dict is not the same instance
        assert out[0] is not canonical


class TestRenameValidation:
    """Reject rename shapes that can't be normalised."""

    def test_missing_mapping_and_no_shorthand_raises(self):
        with pytest.raises(ValueError, match="requires non-empty 'mapping'"):
            _parse_transform_rules([{"type": "rename"}])

    def test_empty_mapping_raises(self):
        with pytest.raises(ValueError, match="requires non-empty 'mapping'"):
            _parse_transform_rules([{"type": "rename", "mapping": {}}])

    def test_half_shorthand_from_only_raises(self):
        with pytest.raises(ValueError, match="requires both keys"):
            _parse_transform_rules([{"type": "rename", "from": "x"}])

    def test_half_shorthand_to_only_raises(self):
        with pytest.raises(ValueError, match="requires both keys"):
            _parse_transform_rules([{"type": "rename", "to": "y"}])

    def test_mapping_wrong_type_raises(self):
        with pytest.raises(ValueError, match="requires non-empty 'mapping'"):
            _parse_transform_rules([
                {"type": "rename", "mapping": "not_a_dict"},
            ])


class TestTransformNodeIntegration:
    """The full transform() call should emit canonical rules so the
    pipeline JSON sent to the server is immediately executable."""

    def test_transform_with_rename_shorthand_produces_canonical_json(self):
        # The integration path: user writes the shorthand, the SDK
        # rewrites it before the node lands in the pipeline's _nodes
        # dict, so the payload eventually POSTed to /api/pipelines
        # already has the canonical shape the engine expects.
        with Pipeline("test") as p:
            n = transform("Clean", rules=[
                {"type": "rename", "from": "ts", "to": "event_time"},
            ])

        node = p._nodes[n.node_id]
        assert node["type"] == "transform"
        assert node["config"]["rules"] == [
            {"type": "rename", "mapping": {"ts": "event_time"}}
        ]

    def test_transform_with_wrong_rename_shape_fails_at_build_time(self):
        # If the user builds a rename rule with neither `mapping` nor
        # the {from, to} shorthand, the failure must surface in the
        # transform() call, not later when the pipeline tries to run
        # on a worker pod.
        with pytest.raises(ValueError, match="mapping"):
            with Pipeline("test"):
                transform("Clean", rules=[{"type": "rename"}])
