"""Projects: brokoli.yaml v1, the folder layout, project-wide validate and
compile, init / new pipeline, and plan against a server."""

from __future__ import annotations

import hashlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

import brokoli
import brokoli.cli as cli
from brokoli.projects import (
    ProjectError,
    compile_project,
    find_project_root,
    load_config,
    load_project,
)
from brokoli.scaffold import init_project, new_pipeline
from brokoli.validation import validate_pipeline

PIPELINE = '''from brokoli import Pipeline, source_api
with Pipeline("{name}", pipeline_id="{pid}", description="{desc}") as p:
    source_api("Fetch", url="https://example.com/{pid}")
'''


def write(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


def project(tmp_path: Path, config: str, pipelines: "dict[str, str] | None" = None) -> Path:
    write(tmp_path / "brokoli.yaml", config)
    for rel, body in (pipelines or {}).items():
        write(tmp_path / rel, body)
    return tmp_path


TWO_WS = """version: 1
workspaces:
  analytics: { path: workspaces/analytics, id: ws-a }
  finance:   { path: workspaces/finance, id: ws-f }
"""


def pipe(pid: str, desc: str = "d", name: "str | None" = None) -> str:
    return PIPELINE.format(pid=pid, name=name or pid, desc=desc)


# ------------------------------------------------------------------ config


def test_config_reads_workspaces_and_branches(tmp_path):
    project(
        tmp_path,
        TWO_WS
        + """branches:
  main: { production: true }
  staging: { workspaces: { analytics: ws-a-staging } }
environments:
  prod: { server: https://example.com }
""",
    )
    cfg = load_config(tmp_path)
    assert cfg.workspaces["analytics"].path == "workspaces/analytics"
    assert cfg.workspaces["finance"].id == "ws-f"
    assert cfg.branches["main"].production is True
    assert cfg.branches["staging"].workspaces == {"analytics": "ws-a-staging"}


@pytest.mark.parametrize(
    "body, expected",
    [
        ("workspaces: {a: {path: x}}\n", "version must be 1"),
        ("version: 2\nworkspaces: {a: {path: x}}\n", "version must be 1"),
        ("version: true\nworkspaces: {a: {path: x}}\n", "version must be 1"),
        ("version: 1\n", "at least one workspace"),
        ("version: 1\nworkspaces: {a: {path: ../x}}\n", "plain folder names"),
        ("version: 1\nworkspaces: {a: {path: /abs}}\n", "relative to the project root"),
        ("version: 1\nworkspaces: {a: {path: w/./x}}\n", "plain folder names"),
        ("version: 1\nworkspaces: {Bad: {path: x}}\n", "lowercase letters"),
        ("version: 1\nworkspaces: {a: {path: x, extra: 1}}\n", "unknown key(s) extra"),
        ("version: 1\nworkspaces: {a: {path: w}, b: {path: w/sub}}\n", "overlapping folders"),
        ("version: 1\nworkspaces: {a: {path: w}, b: {path: w}}\n", "overlapping folders"),
        (
            "version: 1\nworkspaces: {a: {path: w}}\nbranches: {staging: {production: true}}\n",
            "only main can deploy to production",
        ),
        (
            "version: 1\nworkspaces: {a: {path: w}}\nbranches: {dev: {workspaces: {zz: x}}}\n",
            "'zz' is not declared",
        ),
    ],
)
def test_config_refuses(tmp_path, body, expected):
    project(tmp_path, body)
    with pytest.raises(ProjectError) as exc:
        load_config(tmp_path)
    assert any(expected in p for p in exc.value.problems), exc.value.problems


def test_folders_that_only_share_a_prefix_do_not_overlap(tmp_path):
    project(tmp_path, "version: 1\nworkspaces: {a: {path: w/a}, b: {path: w/ab}}\n")
    assert set(load_config(tmp_path).workspaces) == {"a", "b"}


def test_an_environments_only_file_is_not_a_project_and_still_works(tmp_path, monkeypatch):
    write(tmp_path / "brokoli.yaml", "environments:\n  dev: { server: http://localhost:8080 }\n")
    (tmp_path / "sub").mkdir()
    assert find_project_root(tmp_path / "sub") is None
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BROKOLI_CONFIG", raising=False)
    assert cli._load_environments() == {"dev": {"server": "http://localhost:8080"}}


def test_the_project_is_found_from_inside_it(tmp_path):
    project(tmp_path, TWO_WS)
    deep = tmp_path / "workspaces" / "analytics" / "pipelines"
    deep.mkdir(parents=True)
    assert find_project_root(deep) == tmp_path.resolve()


# ------------------------------------------------------------------ loading


def test_loads_every_pipeline_with_its_workspace(tmp_path):
    project(
        tmp_path,
        TWO_WS,
        {
            "workspaces/analytics/pipelines/orders/pipeline.py": pipe("orders"),
            "workspaces/finance/pipelines/ledger/pipeline.py": pipe("ledger"),
        },
    )
    p = load_project(tmp_path)
    got = sorted((i.source.workspace, i.source.pipeline_id, i.pipeline.name) for i in p.pipelines)
    assert got == [("analytics", "orders", "orders"), ("finance", "ledger", "ledger")]


def test_every_layout_problem_is_reported_at_once(tmp_path):
    project(
        tmp_path,
        TWO_WS,
        {
            "workspaces/analytics/pipelines/stray.py": pipe("stray"),
            "workspaces/analytics/pipelines/Upper/pipeline.py": pipe("upper"),
            "workspaces/finance/pipelines/ok/pipeline.py": pipe("ok"),
        },
    )
    (tmp_path / "workspaces/finance/pipelines/empty").mkdir()
    with pytest.raises(ProjectError) as exc:
        load_project(tmp_path)
    text = "\n".join(exc.value.problems)
    assert "stray.py: a pipeline lives in its own folder" in text
    assert "Upper: folder names are pipeline ids" in text
    assert "empty: has no pipeline.py" in text


def test_hidden_and_underscored_folders_are_ignored(tmp_path):
    project(tmp_path, TWO_WS, {"workspaces/analytics/pipelines/a/pipeline.py": pipe("a")})
    (tmp_path / "workspaces/analytics/pipelines/__pycache__").mkdir()
    (tmp_path / "workspaces/analytics/pipelines/.cache").mkdir()
    assert [i.source.pipeline_id for i in load_project(tmp_path).pipelines] == ["a"]


def test_the_folder_must_be_the_pipeline_id(tmp_path):
    project(
        tmp_path, TWO_WS, {"workspaces/analytics/pipelines/orders/pipeline.py": pipe("orderz")}
    )
    with pytest.raises(ProjectError) as exc:
        load_project(tmp_path)
    assert "pipeline_id is 'orderz' but the folder is 'orders'" in exc.value.problems[0]


def test_one_pipeline_per_folder(tmp_path):
    body = pipe("two") + 'with Pipeline("other", pipeline_id="other") as q:\n    pass\n'
    project(tmp_path, TWO_WS, {"workspaces/analytics/pipelines/two/pipeline.py": body})
    with pytest.raises(ProjectError) as exc:
        load_project(tmp_path)
    assert "defines 2 pipelines; one per folder" in exc.value.problems[0]


def test_a_failing_file_is_named_and_the_others_still_reported(tmp_path):
    project(
        tmp_path,
        TWO_WS,
        {
            "workspaces/analytics/pipelines/boom/pipeline.py": "raise RuntimeError('kaboom')\n",
            "workspaces/finance/pipelines/bad/pipeline.py": pipe("other"),
        },
    )
    with pytest.raises(ProjectError) as exc:
        load_project(tmp_path)
    text = "\n".join(exc.value.problems)
    assert "workspaces/analytics/pipelines/boom/pipeline.py: RuntimeError: kaboom" in text
    assert "folder is 'bad'" in text


def test_shared_is_importable_and_the_path_is_restored(tmp_path):
    write(tmp_path / "shared/__init__.py", "URL = 'https://example.com/shared'\n")
    body = (
        "from shared import URL\nfrom brokoli import Pipeline, source_api\n"
        'with Pipeline("s", pipeline_id="s") as p:\n    source_api("F", url=URL)\n'
    )
    project(tmp_path, TWO_WS, {"workspaces/analytics/pipelines/s/pipeline.py": body})
    before = list(sys.path)
    sys.modules.pop("shared", None)
    try:
        p = load_project(tmp_path)
    finally:
        sys.modules.pop("shared", None)
    assert "https://example.com/shared" in json.dumps(p.pipelines[0].pipeline.to_json())
    assert sys.path == before


def test_no_project_says_how_to_make_one(tmp_path):
    with pytest.raises(ProjectError) as exc:
        load_project(tmp_path)
    assert "brokoli init" in exc.value.problems[0]


# ------------------------------------------------------------------ compile


def _compiled(tmp_path: Path) -> Path:
    project(
        tmp_path,
        TWO_WS,
        {
            "workspaces/analytics/pipelines/orders/pipeline.py": pipe("orders"),
            "workspaces/finance/pipelines/ledger/pipeline.py": pipe("ledger"),
        },
    )
    return tmp_path


def _tree(path: Path) -> dict[str, bytes]:
    return {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}


def test_compile_is_deterministic_and_each_digest_is_its_files_hash(tmp_path):
    root = _compiled(tmp_path)
    manifest = compile_project(load_project(root), root / "build")
    first = _tree(root / "build")
    compile_project(load_project(root), root / "build")
    assert _tree(root / "build") == first
    assert [(e["workspace"], e["pipeline_id"]) for e in manifest["pipelines"]] == [
        ("analytics", "orders"),
        ("finance", "ledger"),
    ]
    for entry in manifest["pipelines"]:
        data = (root / "build" / entry["ir"]).read_bytes()
        assert entry["digest"] == "sha256:" + hashlib.sha256(data).hexdigest()
        assert entry["digest"] == cli.ir_digest(json.loads(data))


def test_compile_replaces_a_previous_build_but_not_other_folders(tmp_path):
    root = _compiled(tmp_path)
    compile_project(load_project(root), root / "build")
    write(root / "build/pipelines/analytics/stale.json", "{}")
    compile_project(load_project(root), root / "build")
    assert not (root / "build/pipelines/analytics/stale.json").exists()

    write(root / "notabuild/keep.txt", "mine")
    with pytest.raises(ProjectError):
        compile_project(load_project(root), root / "notabuild")
    assert (root / "notabuild/keep.txt").read_text() == "mine"


# ------------------------------------------------------------------ scaffold


def test_init_creates_a_project_whose_example_validates(tmp_path):
    written = init_project(tmp_path / "acme", workspace="analytics")
    names = sorted(str(p.relative_to(tmp_path / "acme")) for p in written)
    assert names == [
        ".github/workflows/brokoli.yml",
        ".gitignore",
        "README.md",
        "brokoli.yaml",
        "shared/__init__.py",
        "workspaces/analytics/pipelines/example/pipeline.py",
    ]
    p = load_project(tmp_path / "acme")
    assert [i.source.pipeline_id for i in p.pipelines] == ["example"]
    assert validate_pipeline(p.pipelines[0].pipeline).valid
    assert p.config.branches["main"].production
    workflow = (tmp_path / "acme/.github/workflows/brokoli.yml").read_text()
    assert f'brokoli=={brokoli.__version__}' in workflow


@pytest.mark.parametrize("template", ["basic", "python"])
def test_each_template_compiles_and_validates(tmp_path, template):
    init_project(tmp_path, template=template, workflow=False)
    item = load_project(tmp_path).pipelines[0]
    assert validate_pipeline(item.pipeline).valid
    assert item.pipeline.pipeline_id == "example"


def test_init_writes_nothing_when_anything_exists(tmp_path):
    write(tmp_path / "README.md", "mine")
    with pytest.raises(ProjectError) as exc:
        init_project(tmp_path)
    assert "README.md: already exists" in exc.value.problems[0]
    assert not (tmp_path / "brokoli.yaml").exists()
    assert (tmp_path / "README.md").read_text() == "mine"


def test_init_refuses_inside_another_project(tmp_path):
    init_project(tmp_path, workflow=False)
    with pytest.raises(ProjectError) as exc:
        init_project(tmp_path / "workspaces" / "nested")
    assert "inside the project" in exc.value.problems[0]


def test_new_pipeline(tmp_path):
    init_project(tmp_path, workflow=False)
    path = new_pipeline("orders", template="python", start=tmp_path)
    assert path == tmp_path / "workspaces/default/pipelines/orders/pipeline.py"
    assert {i.source.pipeline_id for i in load_project(tmp_path).pipelines} == {
        "example",
        "orders",
    }
    with pytest.raises(ProjectError, match="already exists"):
        new_pipeline("orders", start=tmp_path)
    with pytest.raises(ProjectError, match="not a pipeline id"):
        new_pipeline("Orders", start=tmp_path)
    with pytest.raises(ProjectError, match="no workspace 'nope'"):
        new_pipeline("x", workspace="nope", start=tmp_path)


def test_new_pipeline_needs_a_workspace_when_there_are_several(tmp_path):
    project(tmp_path, TWO_WS)
    with pytest.raises(ProjectError, match="choose a workspace"):
        new_pipeline("x", start=tmp_path)
    assert new_pipeline("x", workspace="finance", start=tmp_path).parent.name == "x"


# ------------------------------------------------------------------ CLI


def run_cli(monkeypatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["brokoli", *argv])
    try:
        cli.main()
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


def test_cli_validate_without_a_file_validates_the_project(tmp_path, monkeypatch, capsys):
    _compiled(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert run_cli(monkeypatch, "validate") == 0
    out = capsys.readouterr().out
    assert "Workspace analytics" in out and "All 2 pipeline(s) valid" in out


def test_cli_validate_reports_project_problems(tmp_path, monkeypatch, capsys):
    project(tmp_path, TWO_WS, {"workspaces/analytics/pipelines/x/pipeline.py": pipe("y")})
    monkeypatch.chdir(tmp_path)
    assert run_cli(monkeypatch, "validate") == 1
    assert "The project has problems" in capsys.readouterr().out


def test_cli_compile_project(tmp_path, monkeypatch, capsys):
    _compiled(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert run_cli(monkeypatch, "compile", "--project", "--out", "out") == 0
    assert (tmp_path / "out/manifest.json").is_file()
    assert run_cli(monkeypatch, "compile", "x.py", "--project") == 2
    assert run_cli(monkeypatch, "compile") == 2
    capsys.readouterr()
    assert run_cli(monkeypatch, "compile", "--project", "--digest") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].endswith("workspaces/analytics/pipelines/orders")


def test_cli_init_and_new(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert run_cli(monkeypatch, "init", "p", "--no-workflow") == 0
    monkeypatch.chdir(tmp_path / "p")
    assert run_cli(monkeypatch, "new", "pipeline", "orders") == 0
    assert run_cli(monkeypatch, "new", "pipeline", "orders") == 1
    assert run_cli(monkeypatch, "new") == 2  # argparse: the subcommand is required


# ------------------------------------------------------------------ plan


class FakeServer:
    """A Brokoli API with one pipeline list per workspace, chosen by the
    X-Workspace-ID header, as the real server scopes them."""

    def __init__(self, by_workspace: "dict[str, list[dict]]") -> None:
        self.by_workspace = by_workspace
        self.seen_workspaces: list[str] = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):  # quiet
                pass

            def do_GET(self):
                ws = self.headers.get("X-Workspace-ID", "")
                fake.seen_workspaces.append(ws)
                items = fake.by_workspace.get(ws, [])
                if self.path.startswith("/api/pipelines?"):
                    summary = [
                        {"id": i["id"], "pipeline_id": i["pipeline_id"], "name": i["name"]}
                        for i in items
                    ]
                    body = {"items": summary, "has_next": False}
                else:
                    pid = self.path.rsplit("/", 1)[-1]
                    match = [i for i in items if i["id"] == pid]
                    if not match:
                        self.send_response(404)
                        self.end_headers()
                        return
                    body = match[0]
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


def remote(pipeline, server_id: str) -> dict:
    ir = pipeline.to_json()
    ir["id"] = server_id
    return ir


@pytest.fixture
def planned(tmp_path, monkeypatch):
    project(
        tmp_path,
        TWO_WS,
        {
            "workspaces/analytics/pipelines/same/pipeline.py": pipe("same"),
            "workspaces/analytics/pipelines/edited/pipeline.py": pipe("edited", desc="new"),
            "workspaces/analytics/pipelines/brand-new/pipeline.py": pipe("brand-new"),
            "workspaces/finance/pipelines/ledger/pipeline.py": pipe("ledger"),
        },
    )
    loaded = {i.source.pipeline_id: i.pipeline for i in load_project(tmp_path).pipelines}
    old_edited = remote(loaded["edited"], "r-edited")
    old_edited["description"] = "old"
    server = FakeServer(
        {
            "ws-a": [
                remote(loaded["same"], "r-same"),
                old_edited,
                {"id": "r-ui", "pipeline_id": "made-in-ui", "name": "made-in-ui", "nodes": []},
            ],
            "ws-f": [remote(loaded["ledger"], "r-ledger")],
        }
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BROKOLI_TOKEN", raising=False)
    monkeypatch.delenv("BROKOLI_WORKSPACE", raising=False)
    yield server
    server.close()


def test_plan_reports_each_workspace_against_its_own_server_workspace(planned, monkeypatch, capsys):
    assert run_cli(monkeypatch, "plan", "--server", planned.url, "--json") == 0
    report = json.loads(capsys.readouterr().out)
    by_ws = {w["workspace"]: w for w in report["workspaces"]}
    a = by_ws["analytics"]
    assert [x["pipeline_id"] for x in a["added"]] == ["brand-new"]
    assert [x["pipeline_id"] for x in a["changed"]] == ["edited"]
    assert '"description": "new"' in a["changed"][0]["diff"]
    assert [x["pipeline_id"] for x in a["unchanged"]] == ["same"]
    assert a["server_only"] == ["made-in-ui"]
    f = by_ws["finance"]
    assert [x["pipeline_id"] for x in f["unchanged"]] == ["ledger"] and not f["added"]
    assert set(planned.seen_workspaces) == {"ws-a", "ws-f"}


def test_plan_text_and_exit_codes(planned, monkeypatch, capsys):
    assert run_cli(monkeypatch, "plan", "--server", planned.url) == 0
    out = capsys.readouterr().out
    assert "+ brand-new (added)" in out and "~ edited (changed)" in out
    assert "on the server, not in this folder: made-in-ui" in out
    assert "2 pipeline(s) to add or change" in out
    assert run_cli(monkeypatch, "plan", "--server", planned.url, "--detailed-exitcode") == 2
    assert (
        run_cli(
            monkeypatch, "plan", "--server", planned.url, "--only", "finance", "--detailed-exitcode"
        )
        == 0
    )


def test_plan_needs_a_server_workspace_for_each_folder(tmp_path, monkeypatch, capsys):
    project(
        tmp_path,
        "version: 1\nworkspaces: {a: {path: wa}, b: {path: wb}}\n",
        {"wa/pipelines/x/pipeline.py": pipe("x")},
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BROKOLI_WORKSPACE", raising=False)
    assert run_cli(monkeypatch, "plan", "--server", "http://127.0.0.1:9") == 1
    assert "has no server workspace" in capsys.readouterr().out
    assert run_cli(monkeypatch, "plan", "--server", "http://127.0.0.1:9", "--workspace", "w") == 1
    assert "use it with --only" in capsys.readouterr().out


def test_plan_workspace_flag_targets_one_folder(planned, monkeypatch, capsys):
    code = run_cli(
        monkeypatch, "plan", "--server", planned.url, "--only", "analytics", "--workspace", "ws-f"
    )
    assert code == 0
    assert set(planned.seen_workspaces) == {"ws-f"}


def test_the_project_node_is_not_shadowed_by_the_projects_module(tmp_path, monkeypatch):
    """`brokoli.project` is the native projection node. The projects module
    must not take its name: importing a submodule rebinds the package
    attribute, and `from brokoli import project` would return a module."""
    _compiled(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert run_cli(monkeypatch, "validate") == 0  # imports brokoli.projects
    from brokoli import project as project_node

    assert callable(project_node) and not isinstance(project_node, type(sys))
