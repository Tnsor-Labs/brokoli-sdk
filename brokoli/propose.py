"""Write changes to a pipeline's definition back into its pipeline.py.

``propose`` takes changes expressed in the pipeline's IR (a schedule, a
node's name, one of a node's config values) and edits the ``pipeline.py``
that compiles to it, keeping everything else in the file as written:
comments, formatting, imports, helpers.

Every change is verified, not assumed. After each edit the file is compiled
again, and the change is kept only if the new IR is exactly the old IR with
that one change made. Anything else -- a value the SDK writes differently, a
node made in a loop or a helper, a field the codemod does not edit yet -- is
returned as skipped, with the reason, for a person to make by hand. A
proposal never claims a change the file does not make.

Nodes are found by where they were made: the file is compiled with a
recorder that notes the call that created each node, so the codemod edits
that call and never guesses from names.

Needs LibCST: ``pip install 'brokoli[propose]'``.
"""

from __future__ import annotations

import copy
import difflib
import importlib.util
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

PIPELINE_FILE = "pipeline.py"

# The Pipeline(...) arguments a change may set, by IR field. "name" is also
# the first positional argument.
PIPELINE_FIELDS = ("name", "description", "schedule", "schedule_timezone", "tags")


class ProposeError(Exception):
    """The file cannot be proposed against at all (it does not compile,
    defines no pipeline or several, or LibCST is missing)."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass
class Change:
    """One change, in the IR's terms.

    ``target`` is ``"pipeline"`` or ``"node"``. For a node, ``node_id`` is
    the IR node id. ``field`` is a pipeline field (``"schedule"``), a node's
    ``"name"``, or ``"config.<key>"`` for one top-level config value.
    ``remove`` drops a config value instead of setting it.
    """

    target: str
    field: str
    value: Any = None
    node_id: Optional[str] = None
    remove: bool = False

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
        else:
            out["value"] = self.value
        return out


@dataclass
class Proposal:
    """What ``propose`` made of the changes."""

    original: str
    source: str
    applied: list[Change] = field(default_factory=list)
    skipped: list[tuple[Change, str]] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return self.source != self.original

    def diff(self, path: str = PIPELINE_FILE) -> str:
        return "".join(
            difflib.unified_diff(
                self.original.splitlines(keepends=True),
                self.source.splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )

    def to_dict(self, path: str = PIPELINE_FILE) -> dict[str, Any]:
        return {
            "changed": self.changed,
            "applied": [c.to_dict() for c in self.applied],
            "skipped": [{**c.to_dict(), "reason": r} for c, r in self.skipped],
            "source": self.source,
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
        self, start: int, end: int, col: Optional[int], end_col: Optional[int]
    ) -> Optional[Any]:
        """The call that made a node. With columns (Python 3.11+) the call
        whose span is exactly the recorded one; without, the innermost call
        spanning those lines, when there is exactly one."""
        if col is not None and end_col is not None:
            exact = [c[0] for c in self.calls if c[1:] == (start, end, col, end_col)]
            return exact[0] if len(exact) == 1 else None
        spanning = [(c[2] - c[1], c[0]) for c in self.calls if c[1] <= start and c[2] >= end]
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
    start, end, col, end_col, scope = site
    if scope != "<module>":
        return None, f"the node is made inside {scope}(), not where the pipeline is written"
    call = locator.node_call(start, end, col, end_col)
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
    try:
        ir, sites = _compile(path, original, root)
        module = cst.parse_module(original)
        for change in changes:
            try:
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
            if new_ir != _expected(ir, change):
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
    finally:
        path.write_text(original)
    return proposal
