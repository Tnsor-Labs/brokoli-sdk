# Vendored differential conformance fixtures (ADR-032 section 14)

Copied verbatim from `brokoli-ui-work`'s
`docs/schema/fixtures/task-interface/differential/` (the core repo's own
copy is the source of truth; see its README there for the fixture format
and the format's own scoping notes). `tests/test_differential_fixtures.py`
loads these and asserts real Python declarations matching each vector's
`python` field compile, via `infer_task_interface`, to exactly that
vector's `expected_node_interface`/`expected_pipeline_parameters`.

Update this copy whenever core's fixtures change; a mismatch here means
this SDK's inference and core's schema have drifted apart.
