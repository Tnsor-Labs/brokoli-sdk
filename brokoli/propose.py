"""Write changes to a pipeline's definition back into its pipeline.py.

``propose`` takes changes expressed in the pipeline's IR (a schedule, a
node's name, one of a node's config values, a node added or removed, a
connection made or dropped) and edits the ``pipeline.py`` that compiles to
it, keeping everything else in the file as written: comments, formatting,
imports, helpers.

Every change is verified, not assumed. After each edit the file is compiled
again, and the change is kept only if the new IR is exactly the old IR with
that one change made. Anything else -- a value the SDK writes differently, a
node made in a loop or a helper, a field the codemod does not edit yet -- is
returned as skipped, with the reason, for a person to make by hand. A
proposal never claims a change the file does not make.

Nodes are found by where they were made: the file is compiled with a
recorder that notes the call that created each node, so the codemod edits
that call and never guesses from names.

A value built from a constant is changed where the constant is defined.
``url=f"{SHOP_API}/products"`` asked to become
``"https://shop.example.com/products"`` sets ``SHOP_API``, in pipeline.py or
in the project module it is imported from, rather than writing the URL over
the expression; the constant is where the mistake was, and other code that
reads it stays in step. When that cannot be done exactly (the new value
does not keep the expression's fixed text, or the constant feeds other
values of this pipeline), the value is written in pipeline.py as before.

Needs LibCST: ``pip install 'brokoli[propose]'``.
"""

from __future__ import annotations

import copy
import difflib
import importlib.util
import json
import keyword
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

PIPELINE_FILE = "pipeline.py"

# The Pipeline(...) arguments a change may set, by IR field. "name" is also
# the first positional argument.
PIPELINE_FIELDS = ("name", "description", "schedule", "schedule_timezone", "tags")

# The function that makes a node of each type, for a node a change adds.
# Types without one here (condition, union, task, ...) are added by hand.
NODE_FACTORIES = {
    "source_db": "source_db",
    "source_api": "source_api",
    "source_file": "source_file",
    "transform": "transform",
    "project": "project",
    "filter": "filter_rows",
    "aggregate": "aggregate",
    "join": "join",
    "quality_check": "quality_check",
    "contract_gate": "contract_gate",
    "code": "code",
    "bash": "bash",
    "sink_db": "sink_db",
    "sink_file": "sink_file",
    "sink_api": "sink_api",
    "migrate": "migrate",
    "dbt": "dbt",
    "notify": "notify",
    "wait": "wait",
}

# Config keys whose factory argument has another name.
_CONFIG_ARGUMENTS = {"max_retries": "retries"}

_NODE_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


class ProposeError(Exception):
    """The file cannot be proposed against at all (it does not compile,
    defines no pipeline or several, or LibCST is missing)."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass
class Change:
    """One change, in the IR's terms.

    ``target`` is ``"pipeline"``, ``"node"`` or ``"edge"``. For a node,
    ``node_id`` is the IR node id. ``field`` is a pipeline field
    (``"schedule"``), a node's ``"name"``, or ``"config.<key>"`` for one
    top-level config value; ``remove`` drops a config value instead of
    setting it.

    Field ``"node"`` adds the node, ``value`` being ``{"type", "name",
    "config"}``, or with ``remove`` removes it. An ``"edge"`` change (field
    ``"edge"``) makes the connection ``value`` = ``{"from", "to"}``, or with
    ``remove`` drops it.
    """

    target: str
    field: str
    value: Any = None
    node_id: Optional[str] = None
    remove: bool = False
    # Set by propose when the change was made by setting a constant:
    # {"path", "constant", "value", "also_used_in"}.
    constant: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Change":
        return cls(
            target=str(data.get("target", "")),
            field=str(data.get("field", "")),
            value=data.get("value"),
            node_id=data.get("node_id"),
            remove=bool(data.get("remove", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"target": self.target, "field": self.field}
        if self.node_id is not None:
            out["node_id"] = self.node_id
        if self.remove:
            out["remove"] = True
        if not self.remove or self.target == "edge":
            out["value"] = self.value
        if self.constant is not None:
            out["constant"] = self.constant
        return out


@dataclass
class Proposal:
    """What ``propose`` made of the changes."""

    original: str
    source: str
    applied: list[Change] = field(default_factory=list)
    skipped: list[tuple[Change, str]] = field(default_factory=list)
    # The other files a change was written into (the module a constant is
    # defined in), by path from the project's root: (original, new).
    others: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def changed(self) -> bool:
        return self.source != self.original or any(a != b for a, b in self.others.values())

    def files(self, path: str = PIPELINE_FILE) -> list[tuple[str, str, str]]:
        """Each file the proposal changes: (path, original, new), the
        modules a constant was set in first, pipeline.py (at *path*) last."""
        out = [(p, a, b) for p, (a, b) in sorted(self.others.items()) if a != b]
        if self.source != self.original:
            out.append((path, self.original, self.source))
        return out

    def diff(self, path: str = PIPELINE_FILE) -> str:
        return "".join(
            "".join(
                difflib.unified_diff(
                    old.splitlines(keepends=True),
                    new.splitlines(keepends=True),
                    fromfile=f"a/{name}",
                    tofile=f"b/{name}",
                )
            )
            for name, old, new in self.files(path)
        )

    def to_dict(self, path: str = PIPELINE_FILE) -> dict[str, Any]:
        """``source`` is pipeline.py's text, changed or not; ``files`` is
        every file the proposal changes, pipeline.py among them only when
        it changed."""
        return {
            "changed": self.changed,
            "applied": [c.to_dict() for c in self.applied],
            "skipped": [{**c.to_dict(), "reason": r} for c, r in self.skipped],
            "source": self.source,
            "files": [{"path": name, "source": new} for name, _, new in self.files(path)],
            "diff": self.diff(path),
        }


def _libcst() -> Any:
    try:
        import libcst
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ProposeError(
            ["brokoli propose needs LibCST: pip install 'brokoli[propose]'"]
        ) from exc
    return libcst


# ------------------------------------------------------------- compiling


def _compile(path: Path, text: str, root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compile *text* as the file at *path*, returning its IR and where each
    node was made. The file is written in place for the duration, so the
    recorder sees the real file name; the caller restores it."""
    from brokoli.cli import load_pipeline_from_file
    from brokoli.pipeline import _node_sites

    path.write_text(text)
    # No bytecode, read or written. Python reuses a cached .pyc when the
    # source's size and modification time match, and an edit like
    # timeout=15 -> timeout=30, written within the same second, matches
    # both: the "recompiled" file would be the old one.
    cached = Path(importlib.util.cache_from_source(str(path)))
    if cached.exists():
        cached.unlink()
    prior_dont_write = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    # The project's own modules are imported afresh each time: a constant
    # set in shared/endpoints.py must be read from the file as it is now,
    # not from the module the previous compile left in sys.modules.
    _forget_project_modules(root)
    recorder: dict[str, Any] = {"file": str(path), "sites": {}}
    token = _node_sites.set(recorder)
    root_str = str(root)
    added = root_str not in sys.path
    if added:
        sys.path.insert(0, root_str)
    try:
        pipelines = load_pipeline_from_file(str(path))
    except Exception as exc:
        raise ProposeError([f"does not compile: {type(exc).__name__}: {exc}"]) from exc
    finally:
        sys.dont_write_bytecode = prior_dont_write
        _node_sites.reset(token)
        if added and root_str in sys.path:
            sys.path.remove(root_str)
    if len(pipelines) != 1:
        raise ProposeError([f"defines {len(pipelines)} pipelines; one per file"])
    ir = pipelines[0].to_json()
    for node in ir.get("nodes", []):
        node.pop("position", None)  # layout, not definition
    return ir, recorder["sites"]


def _forget_project_modules(root: Path) -> None:
    """Drop from sys.modules what the project's own files define: modules
    loaded from under *root*, and any module by a name the project
    provides (``shared`` when there is a ``shared/``), which another
    project loaded in this process would otherwise answer for."""
    for name, mod in list(sys.modules.items()):
        top = name.split(".")[0]
        if top == "brokoli":
            continue
        file = getattr(mod, "__file__", None)
        if file and "site-packages" in file:
            continue
        provided = (root / top).is_dir() or (root / f"{top}.py").is_file()
        try:
            inside = file is not None and root in Path(file).resolve().parents
        except (OSError, ValueError):
            inside = False
        if provided or inside:
            del sys.modules[name]


# ------------------------------------------------------------- the edits


def _literal(value: Any) -> str:
    """*value*, a JSON value, as Python source."""
    if value is None:
        return "None"
    if value is True:
        return "True"
    if value is False:
        return "False"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        # A JSON string literal is a valid Python one, in double quotes.
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ", ".join(_literal(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_literal(str(k))}: {_literal(v)}" for k, v in value.items()) + "}"
    raise ValueError(f"{type(value).__name__} is not a JSON value")


def _callee(call: Any) -> str:
    cst = _libcst()
    func = call.func
    if isinstance(func, cst.Name):
        return func.value
    if isinstance(func, cst.Attribute):
        return func.attr.value
    return ""


def _with_argument(call: Any, keyword: str, value: Any, positional: Optional[int] = None) -> Any:
    """*call* with *keyword* set to *value*: replacing the keyword argument,
    or the positional one at *positional* when that is how it is passed, or
    adding a keyword argument."""
    cst = _libcst()
    expr = cst.parse_expression(_literal(value))
    args = list(call.args)
    for i, arg in enumerate(args):
        if arg.keyword is not None and arg.keyword.value == keyword:
            args[i] = arg.with_changes(value=expr)
            return call.with_changes(args=args)
    if positional is not None:
        plain = [i for i, a in enumerate(args) if a.keyword is None and a.star == ""]
        if len(plain) > positional:
            i = plain[positional]
            args[i] = args[i].with_changes(value=expr)
            return call.with_changes(args=args)
    equal = cst.AssignEqual(
        whitespace_before=cst.SimpleWhitespace(""), whitespace_after=cst.SimpleWhitespace("")
    )
    new = cst.Arg(keyword=cst.Name(keyword), value=expr, equal=equal)
    if args:
        # Keep the call's layout. The new argument goes last and takes over
        # the last argument's comma (and so whatever followed it: a newline
        # before the closing parenthesis, or nothing); the last argument
        # gets the separator the arguments before it use, so one argument
        # per line stays one per line, at the same indentation.
        last = args[-1]
        separator = cst.Comma(whitespace_after=cst.SimpleWhitespace(" "))
        if len(args) >= 2 and isinstance(args[-2].comma, cst.Comma):
            separator = args[-2].comma
        elif isinstance(last.comma, cst.Comma) and isinstance(
            last.comma.whitespace_after, cst.ParenthesizedWhitespace
        ):
            separator = last.comma
        new = new.with_changes(comma=last.comma)
        args[-1] = last.with_changes(comma=separator)
    args.append(new)
    return call.with_changes(args=args)


def _without_argument(call: Any, keyword: str) -> Optional[Any]:
    args = [a for a in call.args if not (a.keyword is not None and a.keyword.value == keyword)]
    if len(args) == len(call.args):
        return None
    return call.with_changes(args=args)


class _Locator:
    """Finds the Pipeline(...) call and the call that made each node."""

    def __init__(self, module: Any) -> None:
        cst = _libcst()
        from libcst.metadata import MetadataWrapper, PositionProvider

        self.wrapper = MetadataWrapper(module)
        positions = self.wrapper.resolve(PositionProvider)
        lines = self.wrapper.module.code.splitlines()

        def byte_col(line: int, col: int) -> int:
            # LibCST counts characters; CPython counts UTF-8 bytes.
            text = lines[line - 1] if 0 < line <= len(lines) else ""
            return len(text[:col].encode("utf-8"))

        self.calls: list[tuple[Any, int, int, int, int]] = []
        for node, pos in positions.items():
            if isinstance(node, cst.Call):
                self.calls.append(
                    (
                        node,
                        pos.start.line,
                        pos.end.line,
                        byte_col(pos.start.line, pos.start.column),
                        byte_col(pos.end.line, pos.end.column),
                    )
                )

    def pipeline_call(self) -> Optional[Any]:
        found = [c[0] for c in self.calls if _callee(c[0]) == "Pipeline"]
        return found[0] if len(found) == 1 else None

    def node_call(
        self, start: int, end: int, col: Optional[int], end_col: Optional[int], callee: str
    ) -> Optional[Any]:
        """The call that made a node. With columns (Python 3.11+) the call
        whose span is exactly the recorded one. Without, the innermost call
        to *callee* (the function the file called) spanning those lines,
        when there is exactly one."""
        if col is not None and end_col is not None:
            exact = [c[0] for c in self.calls if c[1:] == (start, end, col, end_col)]
            return exact[0] if len(exact) == 1 else None
        spanning = [
            (c[2] - c[1], c[0])
            for c in self.calls
            if c[1] <= start and c[2] >= end and _callee(c[0]) == callee
        ]
        if not spanning:
            return None
        spanning.sort(key=lambda t: t[0])
        best = spanning[0][0]
        innermost = [c for size, c in spanning if size == best]
        return innermost[0] if len(innermost) == 1 else None


def _replace(module: Any, old: Any, new: Any) -> Any:
    return module.deep_replace(old, new)


def _expected(ir: dict[str, Any], change: Change) -> dict[str, Any]:
    """*ir* with *change* made, as the edited file must compile to."""
    out = copy.deepcopy(ir)
    if change.target == "pipeline":
        if change.field == "schedule_timezone" and not change.value:
            out.pop("schedule_timezone", None)
        else:
            out[change.field] = copy.deepcopy(change.value)
        return out
    for node in out.get("nodes", []):
        if node.get("id") != change.node_id:
            continue
        if change.field == "name":
            node["name"] = change.value
        else:
            key = change.field[len("config.") :]
            if change.remove:
                node.get("config", {}).pop(key, None)
            else:
                node.setdefault("config", {})[key] = copy.deepcopy(change.value)
    return out


def _edit(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    """The module with *change* written into it, or None and why not."""
    locator = _Locator(module)
    module = locator.wrapper.module  # the instance the positions belong to
    if change.target == "pipeline":
        if change.field not in PIPELINE_FIELDS:
            return None, f"changing {change.field} is not supported yet"
        call = locator.pipeline_call()
        if call is None:
            return None, "the file has no single Pipeline(...) call"
        positional = 0 if change.field == "name" else None
        if change.field == "schedule_timezone" and not change.value:
            updated = _without_argument(call, "schedule_timezone")
            if updated is None:
                return None, "schedule_timezone is not set in the file"
        else:
            updated = _with_argument(call, change.field, change.value, positional)
        return _replace(module, call, updated), ""

    if change.target != "node" or not change.node_id:
        return None, "not a pipeline or node change"
    if change.node_id not in sites:
        return None, f"node {change.node_id!r} is not in the pipeline"
    site = sites[change.node_id]
    if site is None:
        return None, "the node is made outside pipeline.py (in another module)"
    if sum(1 for s in sites.values() if s == site) > 1:
        return None, "the node is made by a loop or a helper that makes several nodes"
    start, end, col, end_col, scope, callee = site
    if scope != "<module>":
        return None, f"the node is made inside {scope}(), not where the pipeline is written"
    call = locator.node_call(start, end, col, end_col, callee)
    if call is None:
        return None, "the call that makes the node could not be found"
    if change.field == "name":
        updated = _with_argument(call, "name", change.value, positional=0)
        # A node's id comes from its name unless node_key fixes it. A
        # rename must not change the id -- the platform would see a new node
        # and lose the old one's history -- so fix it at the id it has.
        if not any(a.keyword is not None and a.keyword.value == "node_key" for a in call.args):
            updated = _with_argument(updated, "node_key", change.node_id)
    elif change.field.startswith("config.") and len(change.field) > len("config."):
        key = change.field[len("config.") :]
        if change.remove:
            updated = _without_argument(call, key)
            if updated is None:
                return None, f"{key} is not set in the call"
        else:
            updated = _with_argument(call, key, change.value)
    else:
        return None, f"changing a node's {change.field} is not supported yet"
    return _replace(module, call, updated), ""


# ------------------------------------------------------------- constants
#
# A value written as an expression over one constant -- the constant itself,
# an f-string around it, or strings added to it -- is changed by setting the
# constant, when the new value keeps the expression's fixed text. The
# constant must be a plain string assigned at the top of pipeline.py, or of
# the project module pipeline.py imports it from by name. The compile that
# confirms every change then decides: a constant that is not what the
# pipeline read, or that feeds anything else in this pipeline, changes
# something other than what was asked, and the value is written in
# pipeline.py instead.

# Folders never searched for other readers of a constant.
_NOT_PROJECT = {".git", ".venv", "venv", "node_modules", "__pycache__", "site-packages"}

# Files searched, at most, for other readers of a constant.
_MAX_SCANNED = 2000


def _argument(call: Any, keyword: str, positional: Optional[int] = None) -> Optional[Any]:
    """The expression *call* passes as *keyword*, or None."""
    for arg in call.args:
        if arg.keyword is not None and arg.keyword.value == keyword:
            return arg.value
    if positional is not None:
        plain = [a for a in call.args if a.keyword is None and a.star == ""]
        if len(plain) > positional:
            return plain[positional].value
    return None


def _pieces(expr: Any) -> Optional[list[Any]]:
    """An expression as its fixed text and the names it reads, in order:
    ``f"{BASE}/products"`` is ``[Name("BASE"), "/products"]``. None for
    anything else (a call, an attribute, a subscript, ...)."""
    import ast

    if isinstance(expr, ast.Name):
        return [expr]
    if isinstance(expr, ast.Constant):
        return [expr.value] if isinstance(expr.value, str) else None
    if isinstance(expr, ast.JoinedStr):
        out: list[Any] = []
        for part in expr.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                out.append(part.value)
            elif isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name):
                # A conversion or a format spec is caught by the compile.
                out.append(part.value)
            else:
                return None
        return out
    if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
        left, right = _pieces(expr.left), _pieces(expr.right)
        return None if left is None or right is None else left + right
    return None


def _string_assignment(module: Any, name: str) -> Optional[Any]:
    """The string of the last top-level ``NAME = "text"`` (or ``NAME: str =
    "text"``) in *module*, the value the module ends up with when nothing
    else rebinds it; the compile that follows the edit checks that."""
    cst = _libcst()
    found = None
    for stmt in module.body:
        if not isinstance(stmt, cst.SimpleStatementLine) or len(stmt.body) != 1:
            continue
        small = stmt.body[0]
        if isinstance(small, cst.Assign) and len(small.targets) == 1:
            target, value = small.targets[0].target, small.value
        elif isinstance(small, cst.AnnAssign) and small.value is not None:
            target, value = small.target, small.value
        else:
            continue
        if isinstance(target, cst.Name) and target.value == name:
            found = value if isinstance(value, cst.SimpleString) else None
    return found


def _string_like(original: Any, value: str) -> str:
    """*value* as a literal quoted the way *original* is. A raw string
    escaped this way says something else, which the compile refuses."""
    body = json.dumps(value, ensure_ascii=False)[1:-1]
    if original.quote == "'":
        body = body.replace('\\"', '"').replace("'", "\\'")
    return f"{original.prefix}{original.quote}{body}{original.quote}"


def _imported_from(tree: Any, name: str, root: Path) -> Optional[tuple[Path, str, str]]:
    """Where pipeline.py's *name* comes from, when it is ``from a.b import
    X [as name]`` at the top of the file and a.b is a module of the project:
    (its file, X, "a.b")."""
    import ast

    for stmt in tree.body:
        if not isinstance(stmt, ast.ImportFrom) or stmt.level or not stmt.module:
            continue
        for alias in stmt.names:
            if (alias.asname or alias.name) != name:
                continue
            base = root.joinpath(*stmt.module.split("."))
            for candidate in (base.with_suffix(".py"), base / "__init__.py"):
                try:
                    resolved = candidate.resolve()
                except (OSError, ValueError):
                    continue
                if root in resolved.parents and resolved.is_file():
                    return resolved, alias.name, stmt.module
    return None


def _readers(root: Path, module: str, name: str, skip: set[Path]) -> list[str]:
    """The project's other files that import *name* from *module*: what
    else a change to the constant reaches."""
    import ast

    out: list[str] = []
    scanned = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in _NOT_PROJECT and not d.startswith("."))
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            path = Path(dirpath, filename).resolve()
            if path in skip:
                continue
            scanned += 1
            if scanned > _MAX_SCANNED:
                return out
            try:
                tree = ast.parse(path.read_text())
            except (OSError, SyntaxError, UnicodeDecodeError, ValueError):
                continue
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.ImportFrom)
                    and not node.level
                    and node.module == module
                    and any(a.name in (name, "*") for a in node.names)
                ) or (isinstance(node, ast.Import) and any(a.name == module for a in node.names)):
                    out.append(path.relative_to(root).as_posix())
                    break
    return out


def _by_constant(
    module: Any, sites: dict[str, Any], change: Change, root: Path, path: Path
) -> Optional[tuple[Path, str, dict[str, Any]]]:
    """The change made by setting a constant: (the file, its new text, what
    was set). None when the value is not written over one constant whose
    new value the change decides."""
    import ast

    cst = _libcst()
    if change.remove or not isinstance(change.value, str):
        return None
    locator = _Locator(module)
    module = locator.wrapper.module
    if change.target == "pipeline" and change.field in PIPELINE_FIELDS:
        call = locator.pipeline_call()
        keyword, positional = change.field, 0 if change.field == "name" else None
    elif change.target == "node" and change.field.startswith("config.") and change.node_id in sites:
        site = sites[change.node_id]
        if site is None or site[4] != "<module>" or sum(1 for s in sites.values() if s == site) > 1:
            return None
        call = locator.node_call(site[0], site[1], site[2], site[3], site[5])
        keyword, positional = change.field[len("config.") :], None
    else:
        return None
    expr = _argument(call, keyword, positional) if call is not None else None
    if expr is None:
        return None
    try:
        pieces = _pieces(ast.parse(module.code_for_node(expr).strip(), mode="eval").body)
    except SyntaxError:
        return None
    if pieces is None:
        return None
    names = [i for i, p in enumerate(pieces) if isinstance(p, ast.Name)]
    if len(names) != 1:
        return None
    prefix = "".join(pieces[: names[0]])
    suffix = "".join(pieces[names[0] + 1 :])
    name = pieces[names[0]].id
    new = change.value
    # A new value that does not keep the fixed text gives a constant the
    # compile then refuses.
    value = new[len(prefix) : len(new) - len(suffix)]

    tree = ast.parse(module.code)
    file, defined, dotted = path, name, ""
    origin = _imported_from(tree, name, root)
    if origin is not None:
        file, defined, dotted = origin
        target = cst.parse_module(file.read_text())
    elif _string_assignment(module, name) is not None:
        target = module
    else:
        return None
    literal = _string_assignment(target, defined)
    if literal is None or not isinstance(literal.evaluated_value, str):
        return None
    text = _string_like(literal, value)
    edited = target.deep_replace(literal, cst.parse_expression(text))
    rel = file.relative_to(root).as_posix()
    written: dict[str, Any] = {"path": rel, "constant": defined, "value": value}
    if dotted:
        written["also_used_in"] = _readers(root, dotted, defined, {file, path})
    return file, edited.code, written


# ------------------------------------------------------------- the graph
#
# A node added is a statement of its own, `name = factory("Name", ...,
# node_key="id")`, in the pipeline's `with` block; a connection made is a
# `a >> b` at the end of it. A node removed is its statement, removed when
# nothing else in the file uses it; a connection dropped is taken out of
# the `a >> b >> c` that makes it. Anything less plain is left for a person.


def _pipeline_block(module: Any, locator: "_Locator") -> tuple[Optional[Any], str]:
    """The `with Pipeline(...)` statement, written at the top of the file."""
    cst = _libcst()
    call = locator.pipeline_call()
    if call is None:
        return None, "the file has no single Pipeline(...) call"
    for stmt in module.body:
        if isinstance(stmt, cst.With) and any(item.item is call for item in stmt.items):
            return stmt, ""
    return None, "the pipeline is not a `with Pipeline(...)` block at the top of the file"


def _names_in(module: Any) -> set[str]:
    """Every name the file uses: read, assigned, imported or defined."""
    import ast

    found: set[str] = set()
    for node in ast.walk(ast.parse(module.code)):
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.alias):
            found.add((node.asname or node.name).split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found.add(node.name)
        elif isinstance(node, ast.arg):
            found.add(node.arg)
    return found


def _used(module: Any, name: str) -> bool:
    """Whether the file reads or assigns *name* anywhere."""
    import ast

    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(ast.parse(module.code)))


def _fresh_name(base: str, taken: set[str]) -> str:
    name = re.sub(r"[^0-9a-zA-Z_]", "_", base).strip("_").lower() or "node"
    if name[0].isdigit():
        name = "n_" + name
    candidate, i = name, 1
    while (
        candidate in taken or keyword.iskeyword(candidate) or candidate in NODE_FACTORIES.values()
    ):
        i += 1
        candidate = f"{name}_{i}"
    return candidate


def _node_statements(
    block: Any, locator: "_Locator", sites: dict[str, Any]
) -> dict[str, tuple[int, Optional[str]]]:
    """Each node made by a statement of its own in the block: its index
    there, and the name it is assigned to (None for a bare call)."""
    cst = _libcst()
    out: dict[str, tuple[int, Optional[str]]] = {}
    for node_id, site in sites.items():
        if site is None or site[4] != "<module>":
            continue
        if sum(1 for s in sites.values() if s == site) > 1:
            continue
        call = locator.node_call(site[0], site[1], site[2], site[3], site[5])
        if call is None:
            continue
        for i, stmt in enumerate(block.body.body):
            if not isinstance(stmt, cst.SimpleStatementLine) or len(stmt.body) != 1:
                continue
            small = stmt.body[0]
            if isinstance(small, cst.Expr) and small.value is call:
                out[node_id] = (i, None)
            elif (
                isinstance(small, cst.Assign)
                and small.value is call
                and len(small.targets) == 1
                and isinstance(small.targets[0].target, cst.Name)
            ):
                out[node_id] = (i, small.targets[0].target.value)
    return out


def _with_body(module: Any, block: Any, body: list[Any]) -> Any:
    return module.deep_replace(block, block.with_changes(body=block.body.with_changes(body=body)))


def _statement(text: str) -> Any:
    return _libcst().parse_statement(text)


def _imported(module: Any, name: str) -> tuple[Optional[Any], str]:
    """The module with *name* imported from brokoli, or None and why not."""
    cst = _libcst()
    for stmt in module.body:
        if not isinstance(stmt, cst.SimpleStatementLine):
            continue
        for small in stmt.body:
            if not (
                isinstance(small, cst.ImportFrom)
                and small.relative == ()
                and isinstance(small.module, cst.Name)
                and small.module.value == "brokoli"
            ):
                continue
            if isinstance(small.names, cst.ImportStar):
                return module, ""
            names = list(small.names)
            if any(a.asname is None and getattr(a.name, "value", None) == name for a in names):
                return module, ""
            # Kept in order, as an import sorter would.
            plain = [getattr(a.name, "value", "") for a in names]
            at = len([n for n in plain if n.lower() < name.lower()])
            comma = cst.Comma(whitespace_after=cst.SimpleWhitespace(" "))
            alias = cst.ImportAlias(name=cst.Name(name), comma=comma)
            if at == len(names):
                last = names[-1]
                alias = alias.with_changes(comma=last.comma)
                names[-1] = last.with_changes(comma=comma)
            names.insert(at, alias)
            return module.deep_replace(small, small.with_changes(names=names)), ""
    return (
        None,
        f"pipeline.py does not import from brokoli with `from brokoli import ...`; import {name} by hand",
    )


def _call_text(factory: str, args: list[str], width: int) -> str:
    """The call on one line when it fits in *width*, else one argument per
    line. Continuation lines are relative to the statement: LibCST indents
    them with the block."""
    one_line = f"{factory}({', '.join(args)})"
    if len(one_line) <= width:
        return one_line
    return f"{factory}(\n" + "".join(f"    {a},\n" for a in args) + ")"


def _add_node(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    spec = change.value if isinstance(change.value, dict) else {}
    node_type, name = spec.get("type"), spec.get("name")
    config = spec.get("config") or {}
    if not isinstance(name, str) or not isinstance(config, dict):
        return None, "a node added needs a type, a name and a config"
    factory = NODE_FACTORIES.get(str(node_type))
    if factory is None:
        return None, f"adding a {node_type} node is not supported yet; add it in pipeline.py"
    node_id = change.node_id
    if not node_id or not _NODE_KEY.fullmatch(node_id):
        return None, f"{node_id!r} is not a node id pipeline.py can keep"
    if change.node_id in sites:
        return None, f"node {change.node_id!r} is already in the pipeline"
    args = [_literal(name)]
    for key, value in config.items():
        if key.startswith("_"):
            continue  # the SDK's own annotations, not settings
        argument = _CONFIG_ARGUMENTS.get(str(key), str(key))
        if not argument.isidentifier() or keyword.iskeyword(argument):
            return None, f"the setting {key!r} cannot be written as an argument"
        args.append(f"{argument}={_literal(value)}")
    args.append(f"node_key={_literal(change.node_id)}")

    locator = _Locator(module)
    module = locator.wrapper.module
    block, why = _pipeline_block(module, locator)
    if block is None:
        return None, why
    made = _node_statements(block, locator, sites)
    indent = block.body.indent if block.body.indent is not None else module.default_indent
    var = _fresh_name(name if name.isascii() else node_id, _names_in(module))
    width = 88 - len(indent) - len(var) - 3
    stmt = _statement(f"{var} = {_call_text(factory, args, width)}\n")
    body = list(block.body.body)
    # After the last node made in the block, before the connections.
    at = max((i for i, _ in made.values()), default=len(body) - 1) + 1
    body.insert(at, stmt)
    module = _with_body(module, block, body)
    return _imported(module, factory)


def _remove_node(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    locator = _Locator(module)
    module = locator.wrapper.module
    block, why = _pipeline_block(module, locator)
    if block is None:
        return None, why
    made = _node_statements(block, locator, sites)
    if change.node_id not in made:
        return (
            None,
            "the node is not made by a statement of its own in the pipeline block; remove it in pipeline.py",
        )
    index, var = made[change.node_id]
    body = list(block.body.body)
    # Its comments go with it: a comment above a statement is about it.
    body.pop(index)
    module = _with_body(module, block, body)
    if var is not None and _used(module, var):
        return None, f"{var} is still used elsewhere in pipeline.py; remove the node by hand"
    return module, ""


def _chain(expr: Any) -> Optional[list[Any]]:
    """`a >> b >> [c, d]` as its parts, or None when it is not a chain."""
    cst = _libcst()
    parts: list[Any] = []
    while isinstance(expr, cst.BinaryOperation) and isinstance(expr.operator, cst.RightShift):
        parts.insert(0, expr.right)
        expr = expr.left
    if not parts:
        return None
    parts.insert(0, expr)
    return parts


def _part_names(part: Any) -> Optional[list[str]]:
    cst = _libcst()
    if isinstance(part, cst.Name):
        return [part.value]
    if isinstance(part, (cst.List, cst.Tuple)) and all(
        isinstance(e.value, cst.Name) for e in part.elements
    ):
        return [e.value.value for e in part.elements]
    return None


def _part_text(names: list[str]) -> str:
    return names[0] if len(names) == 1 else "[" + ", ".join(names) + "]"


def _add_edge(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    edge = change.value if isinstance(change.value, dict) else {}
    if edge.get("condition") is not None:
        return (
            None,
            "connecting a branch of an If / Else node is not supported yet; connect it in pipeline.py",
        )
    source, target = edge.get("from"), edge.get("to")
    locator = _Locator(module)
    module = locator.wrapper.module
    block, why = _pipeline_block(module, locator)
    if block is None:
        return None, why
    made = _node_statements(block, locator, sites)
    body = list(block.body.body)
    taken = _names_in(module)
    names = []
    for node_id in (source, target):
        if node_id not in made:
            return (
                None,
                f"node {node_id!r} is not made by a statement of its own in the pipeline block; connect it in pipeline.py",
            )
        index, var = made[node_id]
        if var is None:
            # A bare call: give it a name to connect it by.
            var = _fresh_name(re.sub(r"_\d+$", "", str(node_id)), taken)
            taken.add(var)
            stmt = body[index]
            call = stmt.body[0].value
            assign = _libcst().Assign(
                targets=[_libcst().AssignTarget(target=_libcst().Name(var))], value=call
            )
            body[index] = stmt.with_changes(body=[assign])
            made[node_id] = (index, var)
        names.append(var)
    body.append(_statement(f"{names[0]} >> {names[1]}\n"))
    return _with_body(module, block, body), ""


def _remove_edge(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    cst = _libcst()
    edge = change.value if isinstance(change.value, dict) else {}
    source, target = edge.get("from"), edge.get("to")
    locator = _Locator(module)
    module = locator.wrapper.module
    block, why = _pipeline_block(module, locator)
    if block is None:
        return None, why
    made = _node_statements(block, locator, sites)
    var_of = {node_id: var for node_id, (_, var) in made.items() if var}
    src, dst = var_of.get(str(source)), var_of.get(str(target))
    plain = "the connection is not made by an `a >> b` of named nodes in the pipeline block; remove it in pipeline.py"
    if src is None or dst is None:
        return None, plain
    body = list(block.body.body)
    for i, stmt in enumerate(body):
        if not isinstance(stmt, cst.SimpleStatementLine) or len(stmt.body) != 1:
            continue
        small = stmt.body[0]
        parts = _chain(small.value) if isinstance(small, cst.Expr) else None
        if parts is None:
            continue
        maybe = [_part_names(p) for p in parts]
        names = [n for n in maybe if n is not None]
        if len(names) != len(maybe):
            continue
        link = next(
            (k for k in range(len(names) - 1) if src in names[k] and dst in names[k + 1]), None
        )
        if link is None:
            continue
        left, right = names[link], names[link + 1]
        if len(left) == 1 and len(right) == 1:
            pieces = [names[: link + 1], names[link + 1 :]]
        elif len(left) == 1 and link + 1 == len(names) - 1:
            pieces = [names[:-1] + [[n for n in right if n != dst]]]
        else:
            return (
                None,
                "the connection is part of a fan-out several nodes share; remove it in pipeline.py",
            )
        lines = [" >> ".join(_part_text(p) for p in piece) for piece in pieces if len(piece) > 1]
        new = [_statement(f"{line}\n") for line in lines]
        if new:
            new[0] = new[0].with_changes(leading_lines=stmt.leading_lines)
        body[i : i + 1] = new
        return _with_body(module, block, body), ""
    return None, plain


def _edit_graph(module: Any, sites: dict[str, Any], change: Change) -> tuple[Optional[Any], str]:
    if change.target == "edge":
        return (_remove_edge if change.remove else _add_edge)(module, sites, change)
    if change.remove:
        if change.node_id not in sites:
            return None, f"node {change.node_id!r} is not in the pipeline"
        return _remove_node(module, sites, change)
    return _add_node(module, sites, change)


def _is_graph(change: Change) -> bool:
    return change.target == "edge" or (change.target == "node" and change.field == "node")


def _normal(ir: dict[str, Any]) -> dict[str, Any]:
    """*ir* with its nodes and edges in a fixed order: where a statement
    sits in the file decides their order, not what the pipeline is."""
    out = dict(ir)
    out["nodes"] = sorted(ir.get("nodes", []), key=lambda n: str(n.get("id")))
    out["edges"] = sorted(
        ir.get("edges", []),
        key=lambda e: (str(e.get("from")), str(e.get("to")), str(e.get("condition"))),
    )
    return out


def _settings(config: Any) -> dict[str, Any]:
    return {k: v for k, v in (config or {}).items() if not str(k).startswith("_")}


def _factory_defaults(node_type: str) -> dict[str, Any]:
    """The defaults of the factory's arguments, by config key."""
    import inspect

    from brokoli import nodes

    factory = getattr(nodes, NODE_FACTORIES.get(node_type, ""), None)
    if factory is None:
        return {}
    by_argument = {v: k for k, v in _CONFIG_ARGUMENTS.items()}
    out: dict[str, Any] = {}
    for name, param in inspect.signature(factory).parameters.items():
        if param.default is not inspect.Parameter.empty and param.default is not None:
            out[by_argument.get(name, name)] = param.default
    return out


def _same_settings(compiled: dict[str, Any], asked: dict[str, Any], node_type: str) -> bool:
    """Whether a node added compiles to the settings asked for. Two
    differences say the same thing: a setting the SDK writes out at its
    default (``response="dataset"``) that was not asked for, and an empty
    setting asked for (``rules=[]``) that the SDK leaves out."""
    defaults = _factory_defaults(node_type)
    for key in set(compiled) | set(asked):
        if key in compiled and key in asked:
            if compiled[key] != asked[key]:
                return False
        elif key in asked:
            if asked[key] not in ("", None, [], {}):
                return False
        elif key not in defaults or compiled[key] != defaults[key]:
            return False
    return True


def _confirms(old: dict[str, Any], new: dict[str, Any], change: Change) -> bool:
    """Whether *new* is *old* with the graph *change* made."""
    old, new = _normal(old), _normal(new)
    expected = copy.deepcopy(old)
    if change.target == "edge":
        edge = {"from": change.value.get("from"), "to": change.value.get("to")}
        if change.remove:
            expected["edges"] = [e for e in expected["edges"] if e != edge]
        else:
            expected["edges"] = _normal({"edges": expected["edges"] + [edge]})["edges"]
        return new == expected
    if change.remove:
        expected["nodes"] = [n for n in expected["nodes"] if n.get("id") != change.node_id]
        expected["edges"] = [
            e for e in expected["edges"] if change.node_id not in (e.get("from"), e.get("to"))
        ]
        return new == expected
    added = [n for n in new["nodes"] if n.get("id") == change.node_id]
    if len(added) != 1:
        return False
    node = added[0]
    if (node.get("type"), node.get("name")) != (change.value.get("type"), change.value.get("name")):
        return False
    if not _same_settings(
        _settings(node.get("config")), _settings(change.value.get("config")), node["type"]
    ):
        return False
    rest = dict(new)
    rest["nodes"] = [n for n in new["nodes"] if n.get("id") != change.node_id]
    return rest == expected


# ------------------------------------------------------------- propose


def propose(root: "str | os.PathLike[str]", folder: str, changes: list[Change]) -> Proposal:
    """Write *changes* into ``<root>/<folder>/pipeline.py``, keeping those
    the recompiled file confirms. The file is left as it was; the new text
    is the proposal's ``source``."""
    cst = _libcst()
    root = Path(root).resolve()
    path = (root / folder / PIPELINE_FILE).resolve()
    if root not in path.parents:
        raise ProposeError([f"{folder}: not inside the project"])
    if not path.is_file():
        raise ProposeError([f"{folder}/{PIPELINE_FILE}: no such file"])
    original = path.read_text()
    proposal = Proposal(original=original, source=original)
    # Other files written for a constant: their text before propose ran,
    # put back however it ends.
    touched: dict[Path, str] = {}
    try:
        ir, sites = _compile(path, original, root)
        module = cst.parse_module(original)
        for change in changes:
            if not _is_graph(change):
                try:
                    found = _by_constant(module, sites, change, root, path)
                except (ValueError, OSError, UnicodeDecodeError, cst.ParserSyntaxError):
                    found = None
                compiled = (
                    _set_constant(path, root, module, change, ir, found, touched)
                    if found is not None
                    else None
                )
                if found is not None and compiled is not None:
                    file, text, written = found
                    if file == path:
                        module = cst.parse_module(text)
                    ir, sites = compiled
                    change.constant = written
                    proposal.applied.append(change)
                    continue
            try:
                if _is_graph(change):
                    edited, why = _edit_graph(module, sites, change)
                else:
                    edited, why = _edit(module, sites, change)
            except (ValueError, cst.ParserSyntaxError) as exc:
                edited, why = None, f"the value cannot be written: {exc}"
            if edited is None:
                proposal.skipped.append((change, why))
                continue
            try:
                new_ir, new_sites = _compile(path, edited.code, root)
            except ProposeError as exc:
                proposal.skipped.append((change, "; ".join(exc.problems)))
                continue
            if _is_graph(change):
                confirmed = _confirms(ir, new_ir, change)
            else:
                confirmed = new_ir == _expected(ir, change)
            if not confirmed:
                proposal.skipped.append(
                    (
                        change,
                        "the SDK compiles this edit to something else; make this change by hand",
                    )
                )
                continue
            module, ir, sites = edited, new_ir, new_sites
            proposal.applied.append(change)
        proposal.source = module.code
        for file, before in touched.items():
            now = file.read_text()
            if now != before:  # tried and put back: not part of the proposal
                proposal.others[file.relative_to(root).as_posix()] = (before, now)
    finally:
        path.write_text(original)
        for file, before in touched.items():
            file.write_text(before)
    return proposal


def _set_constant(
    path: Path,
    root: Path,
    module: Any,
    change: Change,
    ir: dict[str, Any],
    found: tuple[Path, str, dict[str, Any]],
    touched: dict[Path, str],
) -> Optional[tuple[dict[str, Any], dict[str, Any]]]:
    """The compiled pipeline when setting the constant makes exactly
    *change*, else None. The file is left set when it does, as it was when
    it does not."""
    file, text, _ = found
    source = text if file == path else module.code
    prior = None
    if file != path:
        prior = file.read_text()
        touched.setdefault(file, prior)
        _write_module(file, text)
    try:
        new_ir, new_sites = _compile(path, source, root)
    except ProposeError:
        new_ir, new_sites = None, {}
    if new_ir is not None and new_ir == _expected(ir, change):
        return new_ir, new_sites
    if prior is not None:
        _write_module(file, prior)
    return None


def _write_module(file: Path, text: str) -> None:
    file.write_text(text)
    cached = Path(importlib.util.cache_from_source(str(file)))
    if cached.exists():
        cached.unlink()
