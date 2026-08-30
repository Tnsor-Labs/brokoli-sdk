"""Task-bundle packaging (ADR-031): the deterministic, content-addressed
project archive a ``@task(package="bundle")`` compiles to.

The archive is the ADR-016 shareable style of envelope the server already
ships: a gzipped tar of ``manifest.json`` plus the task's project files,
addressed by the SHA-256 of its own bytes (``sha256:<64 hex>``). The
digest is computed here, once, locally -- the server never builds, it only
re-verifies: an uploaded archive must hash to the digest it claims, and
what it mounts must match the manifest's own self-description.

v1 scope is project files only (ADR-031 scope note): the packager walks
the entry module's import graph and bundles exactly those files -- the
entry module, same-repo helper modules, relative imports. A task whose
module imports a third-party dependency fails packaging with a clear,
named error rather than silently succeeding and failing at run time.

Manifest shape is the server's contract (pkg/taskbundle in the core repo):
``{format, language, entry, files, task_name, source_digest, build_digest,
language_runtime}``. ``files`` is authoritative. ``archive_sha256`` is
deliberately NOT written: it is self-referential (the digest of bytes that
contain the field naming it), so core's ``ParseArchive`` refuses a
non-empty value that does not match -- matching is infeasible for any
real archive.``
"""

from __future__ import annotations

import ast
import gzip
import hashlib
import io
import json
import os
import sys
import tarfile
from dataclasses import dataclass
from typing import Any, Callable, Iterable

# Format value the task_bundle IR reference must equal (core taskbundle.Format).
FORMAT = "task-bundle/1"

# Mirror of core taskbundle.MaxArchiveBytes; uploads larger are refused
# server-side, so fail here -- before the network round-trip -- with the same cap.
MAX_BYTES = 64 * 1024 * 1024  # 64 MiB

# The generated entry module every task bundle carries; its source imports
# the author module and runs the task function with the wrapper contract's
# rows/output_data globals.
ENTRY = "entry.py"

_DIGEST_PREFIX = "sha256:"

# Unexecutable top-level roots: stdlib modules are not bundled (the worker
# has them); everything else that cannot be resolved inside the project is
# refused by name instead of deployed to fail remotely.
_STDLIB = frozenset(getattr(sys, "stdlib_module_names", ()))


class BundleError(Exception):
    """Packaging failed; nothing was deployed. The message names exactly
    what could not be packaged and why."""


def digest_bytes(data: bytes) -> str:
    """Content address of *data* (``sha256:<hex>``) -- what a bundle's
    IR reference must equal."""
    return _DIGEST_PREFIX + hashlib.sha256(data).hexdigest()


def _json_manifest(manifest: dict[str, Any]) -> bytes:
    # Sorted keys, no whitespace: deterministic, and the ordering the ADR
    # requires for content addressing to mean anything.
    return json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _empty_dir(names: Iterable[str]) -> list[str]:
    """Every parent directory of the given slash paths, outermost-first,
    deduplicated -- the tar entries a strict reader expects to precede
    their files."""
    dirs: set[str] = set()
    for name in names:
        parts = name.split("/")
        for i in range(1, len(parts)):
            dirs.add("/".join(parts[:i]))
    return sorted(dirs)


def _tar_entry(name: str, body: bytes, *, is_dir: bool = False) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.type = tarfile.DIRTYPE if is_dir else tarfile.REGTYPE
    info.mode = 0o755 if is_dir else 0o644
    info.size = 0 if is_dir else len(body)
    info.mtime = 0  # zeroed timestamps: identical input -> identical archive
    info.uid = 0
    info.gid = 0
    return info


def assemble(files: dict[str, str], manifest: dict[str, Any]) -> bytes:
    """Deterministically tar+gzip *files* (sorted, zeroed timestamps) with
    *manifest* as manifest.json. The digest of the returned bytes is the
    bundle's content address."""
    order = sorted(files)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, filename="") as gz:
        with tarfile.open(fileobj=gz, mode="w", format=tarfile.USTAR_FORMAT) as tf:
            for d in _empty_dir(order + ["manifest.json"]):
                tf.addfile(_tar_entry(d, b"", is_dir=True))
            manifest_bytes = _json_manifest(manifest)
            tf.addfile(_tar_entry("manifest.json", manifest_bytes), io.BytesIO(manifest_bytes))
            for p in order:
                body = files[p].encode("utf-8")
                tf.addfile(_tar_entry(p, body), io.BytesIO(body))
    return buf.getvalue()


@dataclass(frozen=True)
class TaskBundle:
    """One packaged task project: the deterministic archive plus its
    self-description. ``digest`` is the value the code node's
    ``task_bundle.digest`` must carry."""

    digest: str
    archive: bytes
    files: tuple[str, ...]
    entry: str
    task_name: str
    manifest: dict[str, Any]

    def __post_init__(self) -> None:
        if self.digest != digest_bytes(self.archive):
            raise BundleError("internal: bundle digest does not match its archive bytes")


def _build_digest() -> str:
    """Name the build tool + its version so identical source rebuilt by a
    different builder is distinguishable (ADR-031 source_digest/build_digest)."""
    version = "unknown"
    try:
        import brokoli  # noqa: PLC0415

        version = brokoli.__version__
    except Exception:  # pragma: no cover - exotic environment
        pass
    return f"brokoli-python/{version};{FORMAT}"


def _rel_parts(abs_path: str, root: str) -> str | None:
    rel = os.path.relpath(abs_path, root)
    rel = rel.replace(os.sep, "/")
    if rel.startswith("../") or rel == ".." or os.path.isabs(rel):
        return None
    return rel


# ---------------------------------------------------------------------------
# Import-graph walker (ADR-031 Python packaging: entry module, relative
# imports, same-repo helper modules; third-party is refused by name).
# ---------------------------------------------------------------------------


def _module_default(rel: str) -> str:
    """Dotted import name for a bundle-relative module path, e.g.
    ``my_pkg/tasks.py`` -> ``my_pkg.tasks`` (dropping ``/__init__``)."""
    parts = [p for p in rel.split("/") if p not in ("", ".")]
    if parts and parts[-1] == "__init__":  # package itself, no module part
        parts = parts[:-1]
    elif parts and parts[-1].endswith(".py"):
        parts[-1] = parts[-1][:-3]
    return ".".join(parts)


def _file_candidates(rel: str, root: str) -> list[str]:
    """Absolute module-file candidates for a dotted import name resolved
    against the project root: ``a.b`` -> ``root/a/b.py`` or
    ``root/a/b/__init__.py`` (also namespace packages under PEP 420)."""
    path = "/".join(rel.split("."))
    out: list[str] = []
    for suffix in (".py", "/__init__.py"):
        cand = os.path.join(root, path + suffix)
        if os.path.isfile(cand):
            out.append(cand)
    # Namespace package dir: a/b/__init__ absent but a/b/mod.py present.
    dir_cand = os.path.join(root, path)
    if os.path.isdir(dir_cand):
        out.append(dir_cand)
    return out


def _resolve_under_root(rel_module: str, root: str) -> str | None:
    """Resolve dotted *rel_module* to a concrete **absolute** importable
    path under *root*, longest (most specific) first. Handles module files,
    packages (``__init__.py``), and PEP 420 namespace directories. ``None``
    means nothing inside the project."""
    parts = rel_module.split(".")
    for i in range(len(parts), 0, -1):
        base = "/".join(parts[:i])
        for cand in (base + ".py", base + "/__init__.py"):
            abs_path = os.path.join(root, cand)
            if os.path.isfile(abs_path):
                return abs_path
        abs_dir = os.path.join(root, base)
        if os.path.isdir(abs_dir):
            init = os.path.join(abs_dir, "__init__.py")
            if os.path.isfile(init):
                return init
            return abs_dir  # namespace package (PEP 420): no file to carry
    return None


def _refuse(name: str, reason: str) -> None:
    raise BundleError(
        f"cannot package {name}: {reason} (task bundles cover the project's own "
        "files only -- see ADR-031 v1 scope)"
    )


def _classify_unresolved(top_root: str) -> None:
    if top_root in _STDLIB:
        return  # stdlib: present in the worker, never bundled
    _refuse(
        top_root,
        f"{top_root!r} is not part of this project and is not in the standard "
        "library (third-party dependencies are not packaged in v1)",
    )


def _package_import(
    files: dict[str, str],
    root: str,
    current_rel: str,
    node: ast.Import | ast.ImportFrom,
) -> None:
    """Resolve one import statement of the module at *current_rel*, adding
    every same-project module file it can reach to *files* and refusing
    anything outside the project scope."""
    if isinstance(node, ast.Import):
        for alias in node.names:
            _package_dotted(files, root, alias.name.split(".")[0], alias.name)
        return

    # ast.ImportFrom
    if node.level == 0:
        if node.module:
            _package_dotted(files, root, node.module.split(".")[0], node.module)
            # ``from <pkg> import <name>`` may be importing a *submodule*
            # of that package (from pkg import sub == import pkg.sub). A
            # symbol defined inside pkg's own __init__ is skipped: an
            # unresolved name is a plain attribute, not a file.
            for alias in node.names:
                target = f"{node.module}.{alias.name}"
                resolved = _resolve_under_root(target, root)
                if resolved is not None:
                    _package_module(files, root, target)
        return

    # Relative import: climb toward the current module's package, then down.
    current_dir = os.path.dirname(current_rel).replace(os.sep, "/")
    parts = [p for p in current_dir.split("/") if p]
    depth = node.level
    if depth > len(parts):
        _refuse(_module_default(current_rel), "relative import climbs above the project root")
    base_parts = parts[: len(parts) - depth + 1] if depth > 1 else parts
    pkg_rel = "/".join(base_parts) if base_parts else ""
    base_dotted = ".".join(p for p in base_parts if p)
    if node.module:
        target = f"{base_dotted}.{node.module}" if base_dotted else node.module
        _package_module(files, root, target)
    # ``from . import name``: a sibling submodule under the package, if it
    # exists; otherwise the name is a symbol re-exported by __init__ (already
    # or about to be included), which needs no extra file.
    for alias in node.names:
        cand = os.path.join(pkg_rel, alias.name) if pkg_rel else alias.name
        cand_abs = os.path.join(root, cand)
        if os.path.isfile(cand_abs + ".py") or os.path.isfile(
            os.path.join(cand_abs, "__init__.py")
        ):
            _package_module(files, root, cand)


def _package_module(files: dict[str, str], root: str, rel_module: str) -> None:
    """Include module *rel_module* (dotted, relative to *root*) and its
    project-file dependencies in *files*."""
    # Resolve inside the project, prefer exact dotted path.
    resolved = _resolve_under_root(rel_module, root)
    if resolved is None:
        top = rel_module.split(".")[0]
        if top in _STDLIB:
            return
        _refuse(
            top,
            f"import {rel_module!r} cannot be resolved inside this project; "
            "third-party and undeclared dependencies are not packaged",
        )
        return
    _add_module_file(files, root, resolved)


def _package_dotted(files: dict[str, str], root: str, top: str, dotted: str) -> None:
    # Chopping a deep import like a.b.c.d: the deepest resolvable module
    # under the root is included; ancestors get their __init__ too via
    # _ensure_package_chain.
    parts = dotted.split(".")
    for i in range(len(parts), 0, -1):
        prefix = ".".join(parts[:i])
        resolved = _resolve_under_root(prefix, root)
        if resolved is not None:
            _add_module_file(files, root, resolved)
            return
    # Nothing under the project: stdlib is fine, everything else refuses.
    _classify_unresolved(top)


def _add_module_file(files: dict[str, str], root: str, abs_path: str) -> None:
    """Add *abs_path* (a file under *root*) to the bundle, ensure parent
    packages' ``__init__.py`` files are present, then continue the walk
    from its own imports."""
    rel = _rel_parts(abs_path, root)
    if rel is None or not os.path.isfile(abs_path):
        return
    if rel in files:
        return
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            content = f.read()
    except (OSError, UnicodeError) as exc:
        _refuse(rel, f"could not read source file for packaging: {exc}")
    files[rel] = content
    _ensure_package_chain(files, root, rel)

    try:
        tree = ast.parse(content, filename=rel)
    except SyntaxError as exc:
        _refuse(rel, f"source does not parse: {exc}")
    for stmt in tree.body:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            _package_import(files, root, rel, stmt)


def _ensure_package_chain(files: dict[str, str], root: str, rel: str) -> None:
    """Make sure every ancestor package ``__init__.py`` that exists on disk
    is included (an import needs the package, not the leaf, to resolve)."""
    parts = [p for p in rel.split("/") if p]
    for i in range(1, len(parts)):
        pkg_init = os.path.join(root, "/".join(parts[:i]), "__init__.py")
        if os.path.isfile(pkg_init):
            _add_module_file(files, root, pkg_init)


def _entry_source(import_line: str, func_name: str) -> str:
    """The generated entry module: import the author's function, then run
    it under the exact wrapper contract (rows in, ``output_data`` out) a
    bare code node's script obeys -- the same template, with the import
    substituted for an inline function source."""
    from brokoli.pipeline import TASK_WRAPPER_TEMPLATE

    return TASK_WRAPPER_TEMPLATE.format(
        func_source=f"from {import_line} import {func_name}",
        func_name=func_name,
    )


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


def package_task_project(
    func: Callable,
    task_name: str,
    *,
    project_root: str | None = None,
) -> TaskBundle:
    """Package *func*'s project as a task bundle (ADR-031 Python).

    The entry module is *func*'s containing module, resolved together with
    its whole project-file import graph (relative imports and same-repo
    helpers bundled; stdlib imported but not shipped; anything else refused
    by name). Returns a deterministic :class:`TaskBundle`.
    """
    import inspect

    module = inspect.getmodule(func)
    if module is None or getattr(module, "__file__", None) is None:
        raise BundleError(
            f"cannot package {getattr(func, '__qualname__', '<task>')!r}: its "
            "containing module has no source file (defined dynamically, e.g. via "
            "exec()/interactive) -- task bundles need a real project on disk"
        )
    module_file = os.path.realpath(module.__file__)

    root = project_root
    if root is None:
        root = _find_project_root(os.path.dirname(module_file))
    root = os.path.realpath(root)

    entry_rel = _rel_parts(module_file, root)
    if entry_rel is None:
        raise BundleError(
            f"task module {module_file!r} is outside project root {root!r}; "
            "pass project_root= to package its project"
        )

    files: dict[str, str] = {}
    _add_module_file(files, root, module_file)

    dotted = _module_default(entry_rel)
    entry = ENTRY
    entry_source = _entry_source(dotted, func.__name__)
    files[entry] = entry_source

    source_digest = digest_bytes(
        b"\x00".join(f"{p}\x00{files[p]}".encode("utf-8") for p in sorted(files))
    )
    language_runtime = f">={sys.version_info.major}.{sys.version_info.minor}"
    manifest = {
        "format": FORMAT,
        "language": "python",
        "entry": entry,
        "files": sorted(files),
        "task_name": task_name,
        "source_digest": source_digest,
        "build_digest": _build_digest(),
        "language_runtime": language_runtime,
    }
    archive_bytes = assemble(files, manifest)
    total = len(archive_bytes)
    if total > MAX_BYTES:
        raise BundleError(
            f"task bundle would be {total} bytes, over the {MAX_BYTES}-byte cap "
            "(ADR-031 Decision 6)"
        )
    digest = digest_bytes(archive_bytes)
    return TaskBundle(
        digest=digest,
        archive=archive_bytes,
        files=tuple(sorted(files)),
        entry=entry,
        task_name=task_name,
        manifest=manifest,
    )


def _find_project_root(start: str) -> str:
    """The project root for a task module: nearest ancestor with a ``.git``
    directory (a repo), falling back to the module file's own directory."""
    from pathlib import Path

    current = Path(start).resolve()
    while True:
        if (current / ".git").exists():
            return str(current)
        parent = current.parent
        if parent == current:
            return str(Path(start).resolve())
        current = parent
