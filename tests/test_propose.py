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

    # --output: the result in a file of its own, whatever the file prints.
    noisy = ORDERS.replace(
        "from brokoli import", 'print("hello from the pipeline")\nfrom brokoli import'
    )
    (root / FOLDER / "pipeline.py").write_text(noisy)
    result = tmp_path_factory.mktemp("out") / "result.json"
    code = run_cli(
        monkeypatch, "propose", FOLDER, "--changes", str(changes), "--output", str(result)
    )
    assert code == 0
    assert json.loads(result.read_text())["applied"][0]["field"] == "schedule"

    changes.write_text("{}")
    assert run_cli(monkeypatch, "propose", FOLDER, "--changes", str(changes)) == 1


def test_a_node_without_a_site_is_skipped() -> None:
    import libcst

    from brokoli.propose import _edit

    module = libcst.parse_module(ORDERS)
    edited, why = _edit(module, {"x": None}, Change("node", "config.url", "u", node_id="x"))
    assert edited is None and "outside pipeline.py" in why


def test_without_columns_the_call_is_found_by_line_and_callee(root: Path) -> None:
    # Before Python 3.11 a frame has no column span, only a line. The call
    # is then the one to the function the file called, on that line: not
    # the dict(...) inside it.
    import libcst

    from brokoli.propose import _Locator

    locator = _Locator(libcst.parse_module(ORDERS))
    line = (
        ORDERS.splitlines().index(
            '    clean = transform("Clean", rules=[dict(type="drop_columns", columns=["tmp", "café"])])'
        )
        + 1
    )
    call = locator.node_call(line, line, None, None, "transform")
    assert call is not None and libcst.Module([]).code_for_node(call.func) == "transform"
    assert locator.node_call(line, line, None, None, "sink_db") is None


# ------------------------------------------------------------- the graph

SINK = {
    "type": "sink_file",
    "name": "Save orders",
    "config": {"path": "/out/orders.csv", "format": "csv"},
}


def test_a_node_and_its_connection_are_added(root: Path) -> None:
    p = propose(
        root,
        FOLDER,
        [
            Change("node", "node", SINK, node_id="n_save"),
            Change("edge", "edge", {"from": "clean_1", "to": "n_save"}),
            # The SDK writes format="csv" out at its default; asked for
            # without it, the node is the same.
            Change(
                "node",
                "node",
                {"type": "sink_file", "name": "Save", "config": {"path": "/o.csv"}},
                node_id="n_small",
            ),
        ],
    )
    assert p.skipped == [], p.skipped
    # Imported in order, made after the last node of the block, connected
    # at its end; a call that fits on one line stays on one.
    assert (
        "from brokoli import Pipeline, sink_db, sink_file, source_api, transform, wait\n"
        in p.source
    )
    assert (
        '    ready = wait("Ready", condition="interval_elapsed", timeout="60s")\n'
        "    save_orders = sink_file(\n"
        '        "Save orders",\n'
        '        path="/out/orders.csv",\n'
        '        format="csv",\n'
        '        node_key="n_save",\n'
        "    )\n"
        '    save = sink_file("Save", path="/o.csv", node_key="n_small")\n'
        "    orders >> clean >> checked\n"
        "    clean >> save_orders\n"
    ) in p.source
    ir = compiled(root, p.source)
    assert node(ir, "n_save")["config"] == {"path": "/out/orders.csv", "format": "csv"}
    assert {"from": "clean_1", "to": "n_save"} in ir["edges"]


def test_a_long_call_is_written_one_argument_per_line(root: Path) -> None:
    spec = {
        "type": "source_api",
        "name": "Fetch refunds",
        "config": {
            "url": "https://shop.example.com/refunds",
            "method": "GET",
            "max_retries": 3,
            "retry_backoff": "exponential",
        },
    }
    p = propose(root, FOLDER, [Change("node", "node", spec, node_id="refunds")])
    assert p.skipped == [], p.skipped
    assert (
        "    fetch_refunds = source_api(\n"
        '        "Fetch refunds",\n'
        '        url="https://shop.example.com/refunds",\n'
        '        method="GET",\n'
        "        retries=3,\n"  # max_retries is the retries argument
        '        retry_backoff="exponential",\n'
        '        node_key="refunds",\n'
        "    )\n"
    ) in p.source


def test_a_node_added_the_sdk_cannot_make_as_asked_is_skipped(root: Path) -> None:
    changes = [
        Change("node", "node", {"type": "condition", "name": "If", "config": {}}, node_id="if1"),
        Change(
            "node",
            "node",
            {"type": "wait", "name": "Pause", "config": {"timeout": 30}},
            node_id="pause",
        ),
        Change("node", "node", {**SINK, "config": {"colour": "red"}}, node_id="odd"),
        Change("node", "node", SINK, node_id="Not An Id"),
        Change("node", "node", SINK, node_id="clean_1"),
        Change("edge", "edge", {"from": "clean_1", "to": "nowhere"}),
        Change("edge", "edge", {"from": "orders_x", "to": "clean_1", "condition": True}),
    ]
    p = propose(root, FOLDER, changes)
    assert p.applied == [] and p.source == ORDERS
    reasons = [r for _, r in p.skipped]
    assert "adding a condition node is not supported yet" in reasons[0]
    assert "compiles this edit to something else" in reasons[1]  # 30 becomes "30s"
    assert "does not compile" in reasons[2]  # sink_file has no colour
    assert "not a node id" in reasons[3]
    assert "already in the pipeline" in reasons[4]
    assert "'nowhere' is not made by a statement of its own" in reasons[5]
    assert "If / Else" in reasons[6]


def test_connections_and_nodes_are_removed(root: Path) -> None:
    p = propose(
        root,
        FOLDER,
        [
            Change("edge", "edge", {"from": "fetch_orders_1", "to": "clean_1"}, remove=True),
            Change("node", "node", None, node_id="fetch_orders_1", remove=True),
        ],
    )
    assert p.skipped == [], p.skipped
    assert "    clean >> checked\n" in p.source
    assert "source_api(" not in p.source.split("as p:")[1]
    assert "# Where the orders come from." not in p.source  # its comment went with it
    ir = compiled(root, p.source)
    assert "fetch_orders_1" not in [n["id"] for n in ir["nodes"]]
    assert ir["edges"] == [{"from": "clean_1", "to": "audit_1"}]


def test_what_cannot_be_removed_safely_is_skipped(root: Path) -> None:
    changes = [
        # Still connected: its name is still used.
        Change("node", "node", None, node_id="clean_1", remove=True),
        # Made in a helper, and in a loop.
        Change("node", "node", None, node_id="audit_1", remove=True),
        Change("node", "node", None, node_id="copy_1_1", remove=True),
        Change("node", "node", None, node_id="no_such_node", remove=True),
        # Made somewhere else than a >> of names.
        Change("edge", "edge", {"from": "clean_1", "to": "ready_1"}, remove=True),
    ]
    p = propose(root, FOLDER, changes)
    assert p.applied == [] and p.source == ORDERS
    reasons = [r for _, r in p.skipped]
    assert "clean is still used" in reasons[0]
    assert "not made by a statement of its own" in reasons[1]
    assert "not made by a statement of its own" in reasons[2]
    assert "not in the pipeline" in reasons[3]
    assert "remove it in pipeline.py" in reasons[4]


FANOUT = """from brokoli import Pipeline, sink_file, source_api, transform

with Pipeline("Fan", pipeline_id="fan") as p:
    a = source_api("A", url="https://a.example.com")
    b = transform("B", rules=[])
    c = transform("C", rules=[])
    sink_file("D", path="/out/d.csv")
    a >> [b, c]
"""


def test_fan_outs_and_unnamed_nodes(root: Path) -> None:
    (root / FOLDER / "pipeline.py").write_text(FANOUT)
    p = propose(
        root,
        FOLDER,
        [
            Change("edge", "edge", {"from": "a_1", "to": "b_1"}, remove=True),
            # A node made without a name gets one to be connected by.
            Change("edge", "edge", {"from": "c_1", "to": "d_1"}),
        ],
    )
    assert p.skipped == [], p.skipped
    assert '    d = sink_file("D", path="/out/d.csv")\n' in p.source
    assert p.source.endswith("    a >> c\n    c >> d\n")

    # The last link of a chain.
    (root / FOLDER / "pipeline.py").write_text(FANOUT.replace("a >> [b, c]", "a >> b >> c"))
    p = propose(root, FOLDER, [Change("edge", "edge", {"from": "b_1", "to": "c_1"}, remove=True)])
    assert p.skipped == [] and p.source.endswith("    a >> b\n")

    (root / FOLDER / "pipeline.py").write_text(
        FANOUT.replace("a >> [b, c]", "a >> [b, c] >> d").replace(
            "    sink_file(", "    d = sink_file("
        )
    )
    p = propose(root, FOLDER, [Change("edge", "edge", {"from": "a_1", "to": "b_1"}, remove=True)])
    assert "fan-out" in p.skipped[0][1]


def test_graph_changes_round_trip_as_json() -> None:
    edge = Change("edge", "edge", {"from": "a", "to": "b"}, remove=True)
    assert edge.to_dict() == {
        "target": "edge",
        "field": "edge",
        "remove": True,
        "value": {"from": "a", "to": "b"},
    }
    assert Change.from_dict(edge.to_dict()) == edge
    gone = Change("node", "node", None, node_id="a", remove=True)
    assert gone.to_dict() == {"target": "node", "field": "node", "node_id": "a", "remove": True}


def test_a_graph_change_is_confirmed_only_by_exactly_that_change() -> None:
    from brokoli.propose import _confirms

    a = {"id": "a", "type": "source_api", "name": "A", "config": {"url": "u"}}
    b = {"id": "b", "type": "transform", "name": "B", "config": {}}
    old = {"name": "p", "nodes": [a, b], "edges": [{"from": "a", "to": "b"}]}

    def made(**node: object) -> dict:
        return {"id": "s", "type": "sink_file", "name": "S", "config": {"path": "/o"}, **node}

    def ir(*extra: dict, edges: list | None = None, **fields: object) -> dict:
        return {
            **old,
            **fields,
            "nodes": [*extra, b, a],
            "edges": old["edges"] if edges is None else edges,
        }

    add = Change(
        "node",
        "node",
        {"type": "sink_file", "name": "S", "config": {"path": "/o", "compress": "", "_hint": 1}},
        node_id="s",
    )
    # The order nodes come in is not what the pipeline is.
    assert _confirms(old, ir(made()), add)
    # format="csv" is sink_file's default; written out unasked, it is the same.
    assert _confirms(old, ir(made(config={"path": "/o", "format": "csv"})), add)
    for wrong in (
        ir(made(type="sink_db")),
        ir(made(name="T")),
        ir(made(config={"path": "/x"})),
        ir(made(config={"path": "/o", "format": "json"})),  # not the default
        ir(made(config={"path": "/o", "mode": "append"})),  # no such argument
        ir(made(id="t")),
        ir(made(), name="q"),  # something else changed too
    ):
        assert not _confirms(old, wrong, add), wrong
    asked_more = Change(
        "node", "node", {**add.value, "config": {"path": "/o", "compress": "gzip"}}, node_id="s"
    )
    assert not _confirms(old, ir(made()), asked_more)

    link = Change("edge", "edge", {"from": "b", "to": "a"})
    both = [{"from": "b", "to": "a"}, {"from": "a", "to": "b"}]
    assert _confirms(old, ir(edges=both), link)
    assert not _confirms(old, ir(), link)
    unlink = Change("edge", "edge", {"from": "a", "to": "b"}, remove=True)
    assert _confirms(old, ir(edges=[]), unlink)
    assert not _confirms(old, ir(), unlink)

    gone = Change("node", "node", None, node_id="b", remove=True)
    assert _confirms(old, {**old, "nodes": [a], "edges": []}, gone)
    assert not _confirms(old, {**old, "nodes": [a]}, gone)


def test_names_imports_and_annotations_of_a_node_added(root: Path) -> None:
    spec = {"type": "transform", "name": "Clean", "config": {"rules": [], "_schema_hint": "x"}}
    p = propose(root, FOLDER, [Change("node", "node", spec, node_id="clean_again")])
    assert p.skipped == [], p.skipped
    # "clean" is taken; the SDK's own annotations are not arguments.
    assert '    clean_2 = transform("Clean", rules=[], node_key="clean_again")\n' in p.source

    odd = {"type": "transform", "name": "X", "config": {"not-a-name": 1}}
    p = propose(root, FOLDER, [Change("node", "node", odd, node_id="x")])
    assert "cannot be written as an argument" in p.skipped[0][1]

    # A name that sorts last goes last; a star import needs nothing.
    (root / FOLDER / "pipeline.py").write_text(FANOUT)
    pause = {"type": "wait", "name": "Pause", "config": {"condition": "interval_elapsed"}}
    p = propose(root, FOLDER, [Change("node", "node", pause, node_id="pause")])
    assert (
        p.skipped == []
        and "from brokoli import Pipeline, sink_file, source_api, transform, wait\n" in p.source
    )
    (root / FOLDER / "pipeline.py").write_text(
        FANOUT.replace("Pipeline, sink_file, source_api, transform", "*")
    )
    p = propose(root, FOLDER, [Change("node", "node", pause, node_id="pause")])
    assert p.skipped == [] and "from brokoli import *\n" in p.source

    plain = (
        "import brokoli\n\n"
        'with brokoli.Pipeline("Plain", pipeline_id="plain") as p:\n'
        '    a = brokoli.source_api("A", url="https://a.example.com")\n'
    )
    (root / FOLDER / "pipeline.py").write_text(plain)
    p = propose(root, FOLDER, [Change("node", "node", pause, node_id="pause")])
    assert "import wait by hand" in p.skipped[0][1]
