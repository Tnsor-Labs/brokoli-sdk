"""Tests for @task module-context packaging (brokoli-sdk#3).

Covers:
  - auto-detected module-level constants/helpers get included, and the
    task's node config carries a "package" IR block describing what.
  - transitive helper-of-helper inclusion, and import re-emission.
  - references that can't be safely auto-included fail *locally* with a
    clear error, instead of silently deploying to fail remotely.
  - a task with no external references serializes byte-for-byte the same
    as the pre-#3 isolated-function-only behavior (no regression, no
    "package" key added).
  - the explicit package="module" escape hatch.

Uses ``tests/module_packaging_fixtures.py`` -- a real importable module --
rather than functions defined inline in a test method, because
``inspect.getsource``/``inspect.getmodule`` (which the packaging code
depends on) need a real source file with real module-level names.
"""

import pytest

import module_packaging_fixtures as fx
from brokoli import Pipeline, task
from brokoli.exceptions import PipelineError
from brokoli.pipeline import TASK_WRAPPER_TEMPLATE, _extract_func_source


def _script_of(node_config: dict) -> str:
    return node_config["script"]


class TestNoExternalReferences:
    """Pins the pre-#3 behavior: a task with nothing to include serializes
    exactly as it always has -- no "package" key, same script content."""

    def test_byte_identical_to_isolated_extraction(self):
        with Pipeline("test") as p:
            t = task(fx.clean_no_refs)
            t()

        node = p.to_json()["nodes"][0]
        config = node["config"]

        expected_script = TASK_WRAPPER_TEMPLATE.format(
            func_source=_extract_func_source(fx.clean_no_refs),
            func_name="clean_no_refs",
        )
        assert config["script"] == expected_script
        assert "package" not in config

    def test_builtins_are_not_treated_as_external_refs(self):
        """len/sorted/dict etc. must never trigger inclusion or errors."""
        with Pipeline("test") as p:
            t = task(fx.clean_with_builtin_only)
            t()

        node = p.to_json()["nodes"][0]
        assert "package" not in node["config"]


class TestModuleLevelConstant:
    def test_constant_is_included_in_script(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_constant)
            t()

        node = p.to_json()["nodes"][0]
        config = node["config"]
        script = _script_of(config)

        # The exact assignment line for the referenced constant must be
        # present in the deployed source.
        assert f"API_BASE = {fx.API_BASE!r}" in script
        assert "def clean_with_constant" in script

        pkg = config["package"]
        assert pkg["mode"] == "auto"
        assert pkg["included"] == ["API_BASE"]
        assert pkg["callable"] == "module_packaging_fixtures:clean_with_constant"

    def test_unreferenced_constants_are_not_included(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_constant)
            t()

        script = p.to_json()["nodes"][0]["config"]["script"]
        assert "DEFAULT_LIMIT" not in script
        assert "TAGS" not in script


class TestModuleLevelHelper:
    def test_helper_function_is_included(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_helper)
            t()

        node = p.to_json()["nodes"][0]
        script = _script_of(node["config"])

        assert "def _normalize(row" in script
        assert "def clean_with_helper" in script
        # The helper's own reference to the constant must also be pulled in.
        assert f"API_BASE = {fx.API_BASE!r}" in script

        pkg = node["config"]["package"]
        assert set(pkg["included"]) == {"_normalize", "API_BASE"}

    def test_transitive_helper_of_helper_is_included(self):
        """clean_with_transitive_helper -> _double_normalize -> _normalize
        -> API_BASE: all three must be pulled in, recursively."""
        with Pipeline("test") as p:
            t = task(fx.clean_with_transitive_helper)
            t()

        node = p.to_json()["nodes"][0]
        script = _script_of(node["config"])

        assert "def _double_normalize" in script
        assert "def _normalize(row" in script
        assert f"API_BASE = {fx.API_BASE!r}" in script

        pkg = node["config"]["package"]
        assert set(pkg["included"]) == {"_double_normalize", "_normalize", "API_BASE"}


class TestModuleLevelImport:
    def test_used_import_is_reemitted(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_import)
            t()

        script = p.to_json()["nodes"][0]["config"]["script"]
        assert "import json as _json_alias" in script
        assert "def clean_with_import" in script


class TestUnincludableReference:
    """The core behavior change: local, clear failure instead of a silent
    deploy that only breaks once it runs remotely."""

    def test_fails_locally_with_clear_error(self):
        with Pipeline("test") as p:
            t = task(fx.clean_with_bad_ref)
            with pytest.raises(PipelineError) as exc_info:
                t()

        message = str(exc_info.value)
        assert "UNINCLUDABLE_INSTANCE" in message
        assert "clean_with_bad_ref" in message

    def test_no_node_registered_on_failure(self):
        """A failed packaging attempt must not leave a half-built node."""
        with Pipeline("test") as p:
            t = task(fx.clean_with_bad_ref)
            with pytest.raises(PipelineError):
                t()

        assert p.to_json()["nodes"] == []


class TestPackageModuleMode:
    def test_deploys_whole_module_source(self):
        with Pipeline("test") as p:
            t = task(fx.clean_no_refs, package="module")
            t()

        node = p.to_json()["nodes"][0]
        script = node["config"]["script"]

        # The whole containing module ships verbatim -- including things
        # the task itself never referenced.
        assert "def clean_no_refs" in script
        assert "def clean_with_constant" in script
        assert f'API_BASE = "{fx.API_BASE}"' in script
        assert "class _NotSerializable" in script

        pkg = node["config"]["package"]
        assert pkg["mode"] == "module"
        assert pkg["callable"] == "module_packaging_fixtures:clean_no_refs"
        assert "API_BASE" in pkg["included"]
        assert "clean_with_constant" in pkg["included"]

    def test_module_mode_never_raises_for_unincludable_refs(self):
        """package="module" bypasses auto-detection entirely, so a
        reference that would fail "auto" mode is fine here."""
        with Pipeline("test") as p:
            t = task(fx.clean_with_bad_ref, package="module")
            t()  # must not raise

        script = p.to_json()["nodes"][0]["config"]["script"]
        assert "def clean_with_bad_ref" in script

    def test_expand_supports_package_module(self):
        from brokoli import source_api
        from brokoli.pipeline import CollectionRef

        with Pipeline("test") as p:
            files = CollectionRef(
                source_api("List Files", url="https://x", response="dataset").node_id, p
            )
            t = task(fx.clean_no_refs, package="module")
            t.expand(file=files)

        expand_node = next(n for n in p.to_json()["nodes"] if n["name"] != "List Files")
        assert expand_node["config"]["package"]["mode"] == "module"
        assert "def clean_with_constant" in expand_node["config"]["script"]


class TestInvalidPackageMode:
    def test_unknown_package_mode_raises(self):
        with Pipeline("test") as p:
            t = task(fx.clean_no_refs, package="bogus")
            with pytest.raises(PipelineError, match="bogus"):
                t()
