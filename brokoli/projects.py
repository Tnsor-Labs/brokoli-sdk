"""Brokoli projects: a repository of pipelines, one folder per pipeline.

A project is a directory with a ``brokoli.yaml`` that declares
``version: 1`` and its workspaces::

    version: 1
    workspaces:
      analytics: { path: workspaces/analytics }
      finance:   { path: workspaces/finance, id: 01a0...  }
    branches:
      main: { production: true }
    environments:                 # named servers, as before
      prod: { server: https://brokoli.example, token_env: BROKOLI_TOKEN }

and this layout::

    brokoli.yaml
    workspaces/<workspace>/pipelines/<pipeline_id>/pipeline.py
    shared/                       # importable from every pipeline.py

The rules, checked by :func:`load_project`:

- ``version`` is 1, and there is at least one workspace.
- A workspace path is a plain relative folder, unique, and not inside
  another workspace's folder.
- Each pipeline folder holds a ``pipeline.py`` that defines exactly one
  ``Pipeline``, and the folder's name is that pipeline's ``pipeline_id``.
- Only ``main`` may be a production branch.

A ``brokoli.yaml`` without ``version`` is the older environments-only
file; it still works for ``--env`` and is simply not a project.

Everything here works offline. Talking to a server (``brokoli plan``)
lives in the CLI.
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from brokoli.ir import canonical_json, ir_digest, render_ir

PROJECT_FILE = "brokoli.yaml"
PROJECT_VERSION = 1
PIPELINE_FILE = "pipeline.py"
PIPELINES_DIR = "pipelines"
BUILD_MANIFEST = "manifest.json"

# A pipeline folder is its pipeline_id, so it follows the SDK's own slug
# shape (what Pipeline("My Name") generates): lowercase letters, digits and
# single hyphens.
_PIPELINE_ID = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$")
# Workspace names are keys people type: the same shape, underscores allowed.
_WORKSPACE_NAME = re.compile(r"^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$")
_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")


class ProjectError(Exception):
    """A project that cannot be used, with every problem found."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        super().__init__("\n".join(self.problems))


@dataclass(frozen=True)
class WorkspaceSpec:
    name: str
    path: str
    # The server workspace this folder deploys to, for commands that talk
    # to a server from a laptop (brokoli plan). Optional: a project
    # connected to Brokoli through GitHub keeps that mapping in Brokoli.
    id: str = ""


@dataclass(frozen=True)
class BranchSpec:
    name: str
    production: bool = False
    # workspace name -> server workspace this branch deploys it to
    workspaces: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectConfig:
    version: int
    workspaces: dict[str, WorkspaceSpec]
    branches: dict[str, BranchSpec]


@dataclass(frozen=True)
class PipelineSource:
    """Where one pipeline lives in the project."""

    workspace: str
    pipeline_id: str
    folder: str  # relative to the project root, posix
    file: Path


@dataclass
class ProjectPipeline:
    source: PipelineSource
    pipeline: Any  # brokoli.pipeline.Pipeline


@dataclass
class Project:
    root: Path
    config: ProjectConfig
    pipelines: list[ProjectPipeline]

    def in_workspace(self, name: str) -> list[ProjectPipeline]:
        return [p for p in self.pipelines if p.source.workspace == name]


# ---------------------------------------------------------------- finding


def find_project_root(start: "str | os.PathLike[str]" = ".") -> "Path | None":
    """The nearest directory at or above *start* whose brokoli.yaml declares
    a ``version``, or None. An environments-only brokoli.yaml is not a
    project, and the search stops there: it belongs to whatever directory
    it is in."""
    here = Path(start).resolve()
    if here.is_file():
        here = here.parent
    for directory in (here, *here.parents):
        candidate = directory / PROJECT_FILE
        if candidate.is_file():
            try:
                data = yaml.safe_load(candidate.read_text()) or {}
            except (OSError, yaml.YAMLError):
                return directory  # load_config reports what is wrong
            return directory if isinstance(data, dict) and "version" in data else None
    return None


# ---------------------------------------------------------------- config


def _clean_path(raw: Any, where: str, problems: list[str]) -> "str | None":
    if not isinstance(raw, str) or not raw.strip():
        problems.append(f"{where}: 'path' must be a folder such as workspaces/analytics")
        return None
    text = raw.strip().strip("/")
    pure = PurePosixPath(text)
    if raw.strip().startswith("/") or "\\" in text:
        problems.append(f"{where}: path {raw!r} must be relative to the project root")
        return None
    # Split the text itself: PurePosixPath drops "." and empty segments.
    if any(seg in (".", "..") or not _PATH_SEGMENT.match(seg) for seg in text.split("/")):
        problems.append(f"{where}: path {raw!r} must be plain folder names, no '.' or '..'")
        return None
    return pure.as_posix()


def load_config(root: "str | os.PathLike[str]") -> ProjectConfig:
    """Read and check a project's brokoli.yaml."""
    path = Path(root) / PROJECT_FILE
    try:
        data = yaml.safe_load(path.read_text())
    except OSError as exc:
        raise ProjectError([f"{PROJECT_FILE}: cannot read: {exc}"]) from exc
    except yaml.YAMLError as exc:
        raise ProjectError([f"{PROJECT_FILE}: not valid YAML: {exc}"]) from exc
    if not isinstance(data, dict):
        raise ProjectError([f"{PROJECT_FILE}: must be a mapping"])

    problems: list[str] = []
    version = data.get("version")
    if version != PROJECT_VERSION or isinstance(version, bool):
        problems.append(f"{PROJECT_FILE}: version must be {PROJECT_VERSION}, got {version!r}")

    workspaces: dict[str, WorkspaceSpec] = {}
    raw_ws = data.get("workspaces")
    if not isinstance(raw_ws, dict) or not raw_ws:
        problems.append(
            f"{PROJECT_FILE}: 'workspaces' must map at least one workspace to its folder"
        )
        raw_ws = {}
    for name, spec in raw_ws.items():
        where = f"{PROJECT_FILE}: workspace {name!r}"
        if not isinstance(name, str) or not _WORKSPACE_NAME.match(name):
            problems.append(f"{where}: names are lowercase letters, digits, '-' and '_'")
            continue
        if not isinstance(spec, dict):
            problems.append(f"{where}: must be a mapping with a 'path'")
            continue
        unknown = sorted(set(spec) - {"path", "id"})
        if unknown:
            problems.append(f"{where}: unknown key(s) {', '.join(unknown)}")
        folder = _clean_path(spec.get("path"), where, problems)
        ws_id = spec.get("id", "")
        if ws_id is None:
            ws_id = ""
        if not isinstance(ws_id, str):
            problems.append(f"{where}: 'id' must be a string")
            ws_id = ""
        if folder is not None:
            workspaces[name] = WorkspaceSpec(name=name, path=folder, id=ws_id.strip())

    specs = sorted(workspaces.values(), key=lambda w: w.path)
    for i, a in enumerate(specs):
        for b in specs[i + 1 :]:
            if a.path == b.path or b.path.startswith(a.path + "/"):
                problems.append(
                    f"{PROJECT_FILE}: workspaces {a.name!r} and {b.name!r} have overlapping "
                    f"folders ({a.path}, {b.path})"
                )

    branches: dict[str, BranchSpec] = {}
    raw_br = data.get("branches") or {}
    if not isinstance(raw_br, dict):
        problems.append(f"{PROJECT_FILE}: 'branches' must be a mapping")
        raw_br = {}
    for name, spec in raw_br.items():
        where = f"{PROJECT_FILE}: branch {name!r}"
        if not isinstance(name, str) or not name.strip():
            problems.append(f"{where}: a branch needs a name")
            continue
        spec = spec or {}
        if not isinstance(spec, dict):
            problems.append(f"{where}: must be a mapping")
            continue
        unknown = sorted(set(spec) - {"production", "workspaces"})
        if unknown:
            problems.append(f"{where}: unknown key(s) {', '.join(unknown)}")
        production = spec.get("production", False)
        if not isinstance(production, bool):
            problems.append(f"{where}: 'production' must be true or false")
            production = False
        if production and name != "main":
            problems.append(f"{where}: only main can deploy to production")
        targets = spec.get("workspaces") or {}
        if not isinstance(targets, dict):
            problems.append(f"{where}: 'workspaces' must map workspace names to server workspaces")
            targets = {}
        for ws in targets:
            if ws not in raw_ws:
                problems.append(f"{where}: workspace {ws!r} is not declared under 'workspaces'")
        branches[name] = BranchSpec(
            name=name,
            production=production,
            workspaces={str(k): str(v) for k, v in targets.items()},
        )

    envs = data.get("environments")
    if envs is not None and not isinstance(envs, dict):
        problems.append(f"{PROJECT_FILE}: 'environments' must be a mapping")

    if problems:
        raise ProjectError(problems)
    return ProjectConfig(version=PROJECT_VERSION, workspaces=workspaces, branches=branches)


# ---------------------------------------------------------------- discovery


def discover(root: "str | os.PathLike[str]", config: ProjectConfig) -> list[PipelineSource]:
    """Every pipeline folder the project declares, without importing any.

    Raises ProjectError listing folders that break the layout.
    """
    root = Path(root)
    problems: list[str] = []
    found: list[PipelineSource] = []
    for ws in sorted(config.workspaces.values(), key=lambda w: w.name):
        base = root / ws.path / PIPELINES_DIR
        if not base.is_dir():
            continue  # a workspace with no pipelines yet
        for entry in sorted(base.iterdir(), key=lambda p: p.name):
            rel = f"{ws.path}/{PIPELINES_DIR}/{entry.name}"
            if entry.name.startswith((".", "_")):
                continue
            if entry.is_file():
                if entry.suffix == ".py":
                    problems.append(
                        f"{rel}: a pipeline lives in its own folder, "
                        f"{ws.path}/{PIPELINES_DIR}/<pipeline_id>/{PIPELINE_FILE}"
                    )
                continue
            if not _PIPELINE_ID.match(entry.name):
                problems.append(
                    f"{rel}: folder names are pipeline ids: lowercase letters, digits and '-'"
                )
                continue
            file = entry / PIPELINE_FILE
            if not file.is_file():
                problems.append(f"{rel}: has no {PIPELINE_FILE}")
                continue
            found.append(PipelineSource(ws.name, entry.name, rel, file))
    if problems:
        raise ProjectError(problems)
    return found


def _load_one(root: Path, source: PipelineSource) -> Any:
    from brokoli.cli import load_pipeline_from_file

    pipelines = load_pipeline_from_file(str(source.file))
    if len(pipelines) != 1:
        raise ProjectError(
            [f"{source.folder}/{PIPELINE_FILE}: defines {len(pipelines)} pipelines; one per folder"]
        )
    pipeline = pipelines[0]
    if pipeline.pipeline_id != source.pipeline_id:
        raise ProjectError(
            [
                f"{source.folder}/{PIPELINE_FILE}: pipeline_id is {pipeline.pipeline_id!r} but "
                f"the folder is {source.pipeline_id!r}; rename the folder or pass "
                f"pipeline_id={source.pipeline_id!r}"
            ]
        )
    return pipeline


def load_project(start: "str | os.PathLike[str]" = ".") -> Project:
    """Find the project at or above *start*, check it, and import every
    pipeline. Problems from every pipeline are collected and raised
    together, so one run shows them all."""
    root = find_project_root(start)
    if root is None:
        raise ProjectError(
            [
                f"no project here: no {PROJECT_FILE} with 'version: {PROJECT_VERSION}' in "
                f"{Path(start).resolve()} or above (create one with: brokoli init)"
            ]
        )
    config = load_config(root)
    sources = discover(root, config)

    problems: list[str] = []
    loaded: list[ProjectPipeline] = []
    # The project root is importable while pipelines load, so every
    # pipeline.py can `import shared`.
    root_str = str(root)
    added = root_str not in sys.path
    if added:
        sys.path.insert(0, root_str)
    try:
        for source in sources:
            try:
                loaded.append(ProjectPipeline(source, _load_one(root, source)))
            except ProjectError as exc:
                problems.extend(exc.problems)
            except Exception as exc:  # the file's own code failed to import
                problems.append(f"{source.folder}/{PIPELINE_FILE}: {type(exc).__name__}: {exc}")
    finally:
        if added and root_str in sys.path:
            sys.path.remove(root_str)
    if problems:
        raise ProjectError(problems)
    return Project(root=root, config=config, pipelines=loaded)


# ---------------------------------------------------------------- compile


def _bundles(pipeline: Any) -> list[Any]:
    return list(getattr(pipeline, "task_bundles", ()) or ())


def compile_project(project: Project, out: "str | os.PathLike[str]") -> dict[str, Any]:
    """Write every pipeline's normalized IR and task bundles under *out*,
    with a manifest naming each one's digest. The output depends only on
    the pipelines, so the same commit compiles to the same bytes anywhere.

    *out* must be empty, missing, or a previous build (it has a
    manifest.json); a previous build is replaced.
    """
    out = Path(out)
    if out.exists():
        if not out.is_dir():
            raise ProjectError([f"{out}: exists and is not a folder"])
        if any(out.iterdir()) and not (out / BUILD_MANIFEST).is_file():
            raise ProjectError([f"{out}: is not empty and is not a previous build; choose another"])
        _remove_tree_contents(out)
    out.mkdir(parents=True, exist_ok=True)

    entries: list[dict[str, Any]] = []
    written_bundles: set[str] = set()
    for item in sorted(project.pipelines, key=lambda p: (p.source.workspace, p.source.pipeline_id)):
        src, pipeline = item.source, item.pipeline
        ir = pipeline.to_json()
        rel_ir = f"pipelines/{src.workspace}/{src.pipeline_id}.json"
        target = out / rel_ir
        target.parent.mkdir(parents=True, exist_ok=True)
        # Exactly the bytes ir_digest hashes: sha256 of this file is its digest.
        target.write_text(render_ir(ir))
        bundle_digests = []
        for bundle in _bundles(pipeline):
            bundle_digests.append(bundle.digest)
            if bundle.digest not in written_bundles:
                written_bundles.add(bundle.digest)
                path = out / "bundles" / f"{bundle.digest.replace(':', '-')}.tar.gz"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(bundle.archive)
        entries.append(
            {
                "workspace": src.workspace,
                "pipeline_id": src.pipeline_id,
                "name": pipeline.name,
                "folder": src.folder,
                "digest": ir_digest(ir),
                "ir": rel_ir,
                "task_bundles": sorted(set(bundle_digests)),
            }
        )
    manifest = {"version": PROJECT_VERSION, "pipelines": entries}
    (out / BUILD_MANIFEST).write_text(canonical_json(manifest))
    return manifest


def _remove_tree_contents(directory: Path) -> None:
    for child in directory.iterdir():
        if child.is_dir() and not child.is_symlink():
            _remove_tree_contents(child)
            child.rmdir()
        else:
            child.unlink()
