"""brokoli propose: changes to a pipeline written back into its pipeline.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("libcst")

import brokoli.cli as cli  # noqa: E402
from brokoli.cli import load_pipeline_from_file  # noqa: E402
from brokoli.propose import Change, ProposeError, propose  # noqa: E402

FOLDER = "workspaces/analytics/pipelines/orders"

CONFIG = """version: 1
workspaces:
  analytics: { path: workspaces/analytics, id: ws-a }
"""

# Written the way people write them: a docstring, comments, one argument
# per line, a single-line call, nodes made in a loop and in a helper.
ORDERS = '''"""orders: the shop's orders, every hour."""

from brokoli import Pipeline, sink_db, source_api, transform, wait


def audit(name):
    # A helper that makes a node.
    return transform(name, rules=[{"type": "drop_columns", "columns": ["secret"]}])


with Pipeline(
    "Orders",
    pipeline_id="orders",
    description="Orders from the shop",
    schedule="0 * * * *",  # hourly
    tags=["shop"],
) as p:
    # Where the orders come from.
    orders = source_api(
        "Fetch orders",
        url="https://shop.example.com/orders",
        timeout=15,
        records="orders",
    )
    clean = transform("Clean", rules=[dict(type="drop_columns", columns=["tmp", "café"])])
    checked = audit("Audit")
    copies = [sink_db(f"Copy {i}", conn_id="warehouse", table=f"orders_{i}") for i in (1, 2)]
    ready = wait("Ready", condition="interval_elapsed", timeout="60s")
    orders >> clean >> checked
'''


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "brokoli.yaml").write_text(CONFIG)
    folder = tmp_path / FOLDER
    folder.mkdir(parents=True)
    (folder / "pipeline.py").write_text(ORDERS)
    return tmp_path


def compiled(root: Path, source: str) -> dict:
    path = root / FOLDER / "scratch_compiled.py"
    path.write_text(source)
    try:
        ir = load_pipeline_from_file(str(path))[0].to_json()
    finally:
        path.unlink()
    for node in ir["nodes"]:
        node.pop("position", None)
    return ir


def node(ir: dict, node_id: str) -> dict:
    return next(n for n in ir["nodes"] if n["id"] == node_id)


def test_pipeline_fields_are_written_and_the_rest_kept(root: Path) -> None:
    p = propose(
        root,
        FOLDER,
        [
            Change("pipeline", "schedule", "*/30 * * * *"),
            Change("pipeline", "name", "Shop orders"),
            Change("pipeline", "tags", ["shop", "hourly"]),
            Change("pipeline", "schedule_timezone", "Europe/Lisbon"),
        ],
    )
    assert [c.field for c in p.applied] == ["schedule", "name", "tags", "schedule_timezone"]
    assert p.skipped == []
    assert '    schedule="*/30 * * * *",  # hourly\n' in p.source
    assert '    "Shop orders",\n' in p.source
    assert '    schedule_timezone="Europe/Lisbon",\n) as p:' in p.source
    # Everything else as written, and the file itself untouched.
    for kept in (
        '"""orders: the shop\'s orders, every hour."""',
        "    # Where the orders come from.",
        "def audit(name):",
    ):
        assert kept in p.source
    assert (root / FOLDER / "pipeline.py").read_text() == ORDERS
    ir = compiled(root, p.source)
    assert (ir["name"], ir["schedule"], ir["tags"], ir["schedule_timezone"]) == (
        "Shop orders",
        "*/30 * * * *",
        ["shop", "hourly"],
        "Europe/Lisbon",
    )

    # A timezone cleared is removed from the call.
    (root / FOLDER / "pipeline.py").write_text(p.source)
    again = propose(root, FOLDER, [Change("pipeline", "schedule_timezone", "")])
    assert again.applied and "schedule_timezone" not in again.source


def test_node_values_are_set_added_and_removed(root: Path) -> None:
    fetch = "fetch_orders_1"
    p = propose(
        root,
        FOLDER,
        [
            # Same length as before: a recompile that reused cached bytecode
            # would see the old value and refuse the change.
            Change("node", "config.timeout", 30, node_id=fetch),
            Change("node", "config.method", "POST", node_id=fetch),
            Change("node", "config.records", None, node_id=fetch, remove=True),
            Change(
                "node",
                "config.rules",
                [{"type": "drop_columns", "columns": ["tmp", "x"]}],
                node_id="clean_1",
            ),
        ],
    )
    assert p.skipped == [], p.skipped
    assert "        timeout=30,\n" in p.source
    assert '        method="POST",\n    )' in p.source  # one per line, same indentation
    assert "records=" not in p.source
    # A single-line call stays on one line, and the dict(...) call on the
    # same line is not mistaken for the node's own call.
    assert (
        'clean = transform("Clean", rules=[{"type": "drop_columns", "columns": ["tmp", "x"]}])'
        in p.source
    )
    ir = compiled(root, p.source)
    assert node(ir, fetch)["config"]["timeout"] == 30
    assert node(ir, fetch)["config"]["method"] == "POST"
    assert "records" not in node(ir, fetch)["config"]


def test_a_rename_keeps_the_node_id(root: Path) -> None:
    p = propose(
        root, FOLDER, [Change("node", "name", "Fetch shop orders", node_id="fetch_orders_1")]
    )
    assert p.skipped == [], p.skipped
    assert '        "Fetch shop orders",\n' in p.source
    assert 'node_key="fetch_orders_1"' in p.source
    ir = compiled(root, p.source)
    assert node(ir, "fetch_orders_1")["name"] == "Fetch shop orders"
    assert {"from": "fetch_orders_1", "to": "clean_1"} in ir["edges"]


def test_what_cannot_be_written_safely_is_skipped_with_the_reason(root: Path) -> None:
    changes = [
        Change("node", "config.table", "orders_x", node_id="copy_1_1"),
        Change("node", "config.rules", [], node_id="audit_1"),
        Change("node", "config.url", "https://x", node_id="no_such_node"),
        Change("pipeline", "catch_up", True),
        Change("node", "config._schema_hint", "x", node_id="fetch_orders_1"),
        Change("node", "inputs", [], node_id="fetch_orders_1"),
    ]
    p = propose(root, FOLDER, changes)
    assert p.applied == []
    assert not p.changed and p.source == ORDERS
    reasons = [r for _, r in p.skipped]
    assert "loop or a helper that makes several nodes" in reasons[0]
    assert "inside audit()" in reasons[1]
    assert "not in the pipeline" in reasons[2]
    assert "catch_up is not supported yet" in reasons[3]
    assert "does not compile" in reasons[4]  # source_api has no such argument
    assert "not supported yet" in reasons[5]


def test_an_edit_the_sdk_compiles_differently_is_refused(root: Path) -> None:
    # wait() turns a number of seconds into a duration string: timeout=30
    # compiles to "30s", not to the 30 asked for. The same edit given as the
    # SDK stores it goes through.
    p = propose(
        root,
        FOLDER,
        [
            Change("node", "config.timeout", 30, node_id="ready_1"),
            Change("node", "config.timeout", "90s", node_id="ready_1"),
        ],
    )
    assert [c.value for c in p.applied] == ["90s"]
    assert "compiles this edit to something else" in p.skipped[0][1]
    assert 'timeout="90s"' in p.source


def test_a_file_that_does_not_compile_is_refused(root: Path) -> None:
    (root / FOLDER / "pipeline.py").write_text(ORDERS + "\nraise RuntimeError('boom')\n")
    with pytest.raises(ProposeError) as exc:
        propose(root, FOLDER, [Change("pipeline", "schedule", "")])
    assert "boom" in exc.value.problems[0]
    # A folder outside the project is refused even when it holds a
    # pipeline.py.
    outside = root.parent / f"{root.name}-outside"
    outside.mkdir()
    (outside / "pipeline.py").write_text(ORDERS)
    with pytest.raises(ProposeError) as exc:
        propose(root, f"../{outside.name}", [])
    assert "not inside the project" in exc.value.problems[0]


def test_no_bytecode_is_left_in_the_project(root: Path) -> None:
    propose(root, FOLDER, [Change("pipeline", "schedule", "*/5 * * * *")])
    assert not list(root.rglob("__pycache__")), "propose wrote bytecode into the project"


def test_a_stale_bytecode_file_is_not_trusted(root: Path) -> None:
    # Bytecode left by an earlier import (brokoli compile, say), for the
    # file as it was: a same-length edit within the same second must not
    # be compiled from it.
    import py_compile

    path = root / FOLDER / "pipeline.py"
    py_compile.compile(str(path))
    p = propose(root, FOLDER, [Change("node", "config.timeout", 30, node_id="fetch_orders_1")])
    assert p.skipped == [], p.skipped


def test_literals() -> None:
    from brokoli.propose import _literal

    assert _literal(None) == "None"
    assert _literal(True) == "True" and _literal(False) == "False"
    assert _literal(3) == "3" and _literal(2.5) == "2.5"
    assert _literal('a "b" é') == '"a \\"b\\" é"'
    assert _literal({"k": [1, None, True]}) == '{"k": [1, None, True]}'
    with pytest.raises(ValueError):
        _literal(object())


def test_a_node_made_from_no_frame_of_the_file_has_no_site() -> None:
    from brokoli.pipeline import _node_sites, _record_node_site

    recorder: dict = {"file": "/nowhere/pipeline.py", "sites": {}}
    token = _node_sites.set(recorder)
    try:
        _record_node_site("x")
    finally:
        _node_sites.reset(token)
    assert recorder["sites"] == {"x": None}


def run_cli(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["brokoli", *argv])
    try:
        cli.main()
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


def test_the_cli(
    root: Path,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    changes = tmp_path_factory.mktemp("in") / "changes.json"
    changes.write_text(
        json.dumps(
            [
                {"target": "pipeline", "field": "schedule", "value": "*/5 * * * *"},
                {"target": "pipeline", "field": "catch_up", "value": True},
            ]
        )
    )
    monkeypatch.chdir(root)
    assert run_cli(monkeypatch, "propose", FOLDER, "--changes", str(changes), "--json") == 0
    out = json.loads(capsys.readouterr().out)
    assert out["changed"] and out["applied"] == [
        {"target": "pipeline", "field": "schedule", "value": "*/5 * * * *"}
    ]
    assert out["skipped"][0]["field"] == "catch_up" and out["skipped"][0]["reason"]
    assert out["diff"].startswith(f"--- a/{FOLDER}/pipeline.py")
    assert (root / FOLDER / "pipeline.py").read_text() == ORDERS  # not written without --write

    assert run_cli(monkeypatch, "propose", FOLDER, "--changes", str(changes), "--write") == 0
    text = capsys.readouterr().out
    assert "skipped: catch_up:" in text
    assert 'schedule="*/5 * * * *"' in (root / FOLDER / "pipeline.py").read_text()

    changes.write_text("{}")
    assert run_cli(monkeypatch, "propose", FOLDER, "--changes", str(changes)) == 1


def test_a_node_without_a_site_is_skipped() -> None:
    import libcst

    from brokoli.propose import _edit

    module = libcst.parse_module(ORDERS)
    edited, why = _edit(module, {"x": None}, Change("node", "config.url", "u", node_id="x"))
    assert edited is None and "outside pipeline.py" in why
