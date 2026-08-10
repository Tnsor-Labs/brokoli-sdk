"""Pipeline definition and DAG builder."""

from __future__ import annotations

import ast
import builtins
import copy
import sys
from contextlib import contextmanager
import inspect
import json
import re
import textwrap
from typing import Any, Callable, Iterable, Optional

from brokoli.exceptions import PipelineError
from brokoli.sentinel import UNSET

# --- Layout constants ---
LAYOUT_X_GAP: int = 280
LAYOUT_Y_GAP: int = 100
LAYOUT_X_ORIGIN: int = 50
LAYOUT_Y_CENTER: int = 200
LAYOUT_Y_MIN: int = 50

# --- IR version ---
# Bumped whenever the shape of the serialized pipeline JSON changes in a
# way consumers (the Go backend, the UI) need to know about -- e.g. the
# addition of the ``capabilities`` field on nodes.
IR_VERSION: str = "2.0"
CONDITIONAL_ROUTING_IR_VERSION: str = "2.1"

# --- Capability model ---
#
# Every node carries a ``capabilities`` list describing the role it plays
# in the DAG, independent of its literal ``type`` string. Code that needs
# to answer a question like "is this a source" should check capability
# membership (``"source" in node["capabilities"]``) rather than hardcoding
# a list of type strings -- that keeps decorator-based nodes (which are
# always registered with the literal type ``"code"``) and any future
# built-in node types correctly included without further edits.
#
# This is the single source of truth for the type -> capabilities mapping;
# nothing else in the SDK should hardcode this list.
NODE_TYPE_CAPABILITIES: dict[str, list[str]] = {
    "source_file": ["source", "dataset-output"],
    "source_api": ["source", "dataset-output"],
    "source_db": ["source", "dataset-output"],
    # dbt/migrate produce a dataset for downstream nodes to consume, same
    # as the Go backend's validator already treats them.
    "dbt": ["source", "dataset-output"],
    "migrate": ["source", "dataset-output"],
    "sink_db": ["sink"],
    "sink_file": ["sink"],
    "sink_api": ["sink"],
    # -- Typed dataset/collection refs (brokoli-sdk#2) --
    #
    # "union" is a dedicated node type (a dataset-manifest-combination
    # node -- see union()/CollectionRef.collect() below) rather than a
    # chain of individual merge edges. "dataset_map"/"dataset_filter" are
    # partition-transform nodes on DatasetRef, deliberately a different
    # node *type* from the "code" type the @map/@filter decorators
    # register, so the two don't look interchangeable in serialized JSON.
    "union": ["compute", "dataset-output"],
    "dataset_map": ["compute", "dataset-output"],
    "dataset_filter": ["compute", "dataset-output"],
}
DEFAULT_CAPABILITIES: tuple[str, ...] = ("compute",)


def _capabilities_for(node_type: str) -> list[str]:
    """Return the default capabilities for *node_type* (a defensive copy).

    Decorator-based nodes (``@source``, ``@sink``, ...) are always
    registered with the literal type ``"code"``, so they pass an explicit
    ``capabilities`` override to :meth:`Pipeline._add_node` instead of
    relying on this lookup.
    """
    return list(NODE_TYPE_CAPABILITIES.get(node_type, DEFAULT_CAPABILITIES))

# --- Code-gen templates ---

TASK_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: call task function and capture output
    _task_result = {func_name}(rows)
    if hasattr(_task_result, 'to_rows'):
        # TaskResult object
        _rows = _task_result.to_rows()
        output_data = {{"columns": list(_rows[0].keys()) if _rows else [], "rows": _rows}}
        # Log warnings/errors via stderr
        import sys
        for w in getattr(_task_result, 'warnings', []):
            print(f"#WARNING: {{w}}", file=sys.stderr)
        for e in getattr(_task_result, 'errors', []):
            print(f"#ERROR: {{e}}", file=sys.stderr)
    elif isinstance(_task_result, list):
        if _task_result and isinstance(_task_result[0], dict):
            output_data = {{"columns": list(_task_result[0].keys()), "rows": _task_result}}
        else:
            output_data = {{"columns": columns, "rows": _task_result if isinstance(_task_result, list) else rows}}
    elif hasattr(_task_result, 'to_dict'):
        output_data = {{"columns": list(_task_result.columns), "rows": _task_result.to_dict("records")}}
    elif _task_result is None:
        output_data = {{"columns": columns, "rows": rows}}
    else:
        output_data = {{"columns": columns, "rows": _task_result if isinstance(_task_result, list) else rows}}
''')


CONDITION_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: evaluate condition function
    _cond_result = {func_name}(rows)
    if _cond_result:
        output_data = {{"columns": columns, "rows": rows}}
    else:
        output_data = {{"columns": columns, "rows": []}}
''')


SOURCE_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: call source function and capture output
    _source_result = {func_name}()
    if isinstance(_source_result, list) and _source_result and isinstance(_source_result[0], dict):
        output_data = {{"columns": list(_source_result[0].keys()), "rows": _source_result}}
    elif hasattr(_source_result, 'to_dict'):
        output_data = {{"columns": list(_source_result.columns), "rows": _source_result.to_dict("records")}}
    elif hasattr(_source_result, 'to_rows'):
        _rows = _source_result.to_rows()
        output_data = {{"columns": list(_rows[0].keys()) if _rows else [], "rows": _rows}}
    else:
        output_data = {{"columns": [], "rows": _source_result if isinstance(_source_result, list) else []}}
''')


SINK_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: call sink function with input rows
    {func_name}(rows)
    # Pass-through: sinks forward data unchanged
    output_data = {{"columns": columns, "rows": rows}}
''')


FILTER_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: apply filter predicate to each row
    output_data = {{"columns": columns, "rows": [r for r in rows if {func_name}(r)]}}
''')


MAP_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: apply map function to each row
    _mapped = [{func_name}(r) for r in rows]
    if _mapped and isinstance(_mapped[0], dict):
        output_data = {{"columns": list(_mapped[0].keys()), "rows": _mapped}}
    else:
        output_data = {{"columns": columns, "rows": _mapped}}
''')


VALIDATE_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: call validate function and check result
    import sys
    _valid_result = {func_name}(rows)
    if isinstance(_valid_result, tuple):
        _passed, _message = _valid_result
    else:
        _passed, _message = bool(_valid_result), ""
    if not _passed:
        print(f"#VALIDATION_FAILED: {{_message}}", file=sys.stderr)
        {on_failure_action}
    else:
        if _message:
            print(f"#VALIDATION_OK: {{_message}}", file=sys.stderr)
    output_data = {{"columns": columns, "rows": rows}}
''')


SENSOR_WRAPPER_TEMPLATE: str = textwrap.dedent('''\
    {func_source}

    # Auto-generated: poll sensor until ready
    import time
    _poll_interval = {poll_interval}
    _timeout = {timeout}
    _start = time.time()
    while True:
        if {func_name}():
            break
        if _timeout is not None and time.time() - _start > _timeout:
            raise TimeoutError("Sensor '{func_name}' timed out after {{_timeout}}s")
        time.sleep(_poll_interval)
    output_data = {{"columns": columns, "rows": rows}}
''')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(name: str) -> str:
    """Return the deterministic canonical ID base for a display name."""
    clean = name.lower().replace(" ", "_")
    clean = "".join(c for c in clean if c.isalnum() or c == "_")
    return clean[:20] or "node"


_NODE_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


def _decorator_refs(func: Callable) -> list[str]:
    """Root names referenced by *func*'s decorator lines (``@functools.
    lru_cache(...)`` -> ``functools``), which live outside the function's
    code object and so never appear in co_names.
    """
    try:
        source = textwrap.dedent(inspect.getsource(func))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return []
    if not tree.body or not isinstance(tree.body[0], (ast.FunctionDef, ast.AsyncFunctionDef)):
        return []
    names: list[str] = []
    for dec in tree.body[0].decorator_list:
        node = dec
        while isinstance(node, ast.Call):
            node = node.func
        while isinstance(node, ast.Attribute):
            node = node.value
        if isinstance(node, ast.Name) and not hasattr(builtins, node.id):
            names.append(node.id)
    return names


def _extract_func_source(
    func: Callable, allow_freevars: bool = False, keep_decorators: bool = False
) -> str:
    """Return the dedented source of *func*, stripping any decorator lines.

    Rejects shapes the extraction below cannot represent, instead of
    emitting a script that silently omits them: the scan captures from the
    first ``def `` line, so an ``async def`` never matches and a lambda has
    no ``def`` line at all -- both used to produce a deployed script with
    no function in it, which only failed remotely. Closure variables are
    likewise rejected unless the caller has already materialized them into
    the deployed scope (``allow_freevars=True`` -- only @task auto
    packaging does this, via ``_capture_closure``).
    """
    if inspect.iscoroutinefunction(func):
        raise PipelineError(
            f"{func.__qualname__!r} is an async function. Deployed task "
            "scripts run under a plain synchronous entrypoint, so async "
            "functions are not supported -- define it with `def` and do any "
            "awaiting inside (e.g. `asyncio.run(...)`)."
        )
    if func.__name__ == "<lambda>":
        raise PipelineError(
            "Lambdas cannot be deployed: their source cannot be extracted "
            "as a standalone function. Define a named function with `def` "
            "instead."
        )
    if func.__code__.co_freevars and not allow_freevars:
        names = ", ".join(sorted(func.__code__.co_freevars))
        raise PipelineError(
            f"{func.__qualname__!r} closes over {names} from an enclosing "
            "scope, which a deployed script can't carry. Move the value to "
            "module level or pass it through the data instead. (@task with "
            "package=\"auto\" captures JSON-serializable closure values "
            "automatically; the other decorators do not.)"
        )

    source = inspect.getsource(func)

    if keep_decorators:
        # Auto-included helpers keep their decorator lines (getsource
        # starts at the first decorator) -- stripping them silently
        # changed helper behavior (@lru_cache stopped caching). The main
        # task function must NOT take this path: its decorator is the SDK
        # decorator itself, which doesn't exist remotely.
        return textwrap.dedent(source)

    lines = source.split("\n")

    # Skip everything before the first `def` line (decorators, blank lines).
    func_lines: list[str] = []
    found_def = False
    for line in lines:
        if not found_def and line.strip().startswith("def "):
            found_def = True
        if found_def:
            func_lines.append(line)

    return textwrap.dedent("\n".join(func_lines))


# ---------------------------------------------------------------------------
# Task module packaging (brokoli-sdk#3)
# ---------------------------------------------------------------------------
#
# ``_extract_func_source`` (above) captures only the isolated function body,
# which silently drops any module-level constant, helper function, or import
# a task's body references -- the task deploys fine locally (since
# ``inspect.getsource`` only needs the function itself) but fails *remotely*
# with a ``NameError``, because the remote execution namespace never saw
# those module-level names.
#
# The functions below fix that for ``@task`` specifically (see
# ``_TaskWrapper``) in two modes, selected via ``@task(package=...)``:
#
# - ``"auto"`` (default): inspect the task function's bytecode for names it
#   references but doesn't define itself, resolve each one against the
#   function's ``__globals__``, and either (a) auto-include it -- inlining
#   JSON-serializable constants and same-module helper functions
#   (recursively), and re-emitting the module's top-level import statement
#   for imported names -- or (b) fail *locally*, at pipeline-build time,
#   naming exactly what couldn't be included. A task with no such
#   references behaves byte-for-byte as before -- this mode is a strict
#   superset of the old isolated-function behavior.
# - ``"module"``: an explicit escape hatch that skips auto-detection
#   entirely and deploys the task's *entire* containing module source
#   verbatim. Heavier than "auto" (the whole file ships, including any
#   unrelated top-level code -- e.g. pipeline-construction code that lives
#   in the same file) but always works, for cases auto-detection can't
#   handle (e.g. a legitimately whole helper module of dependencies).

_PACKAGE_MODES: tuple[str, ...] = ("auto", "module")


def _is_json_serializable(value: Any) -> bool:
    """Best-effort check that *value* round-trips through ``json.dumps``.

    ``allow_nan=False`` matters: by default ``json.dumps`` accepts
    ``float('inf')``/``float('nan')``, but those values are later emitted
    into the deployed script with ``repr()`` -- producing bare ``inf`` /
    ``nan``, which are not valid Python literals and only fail remotely.
    Rejecting them here routes the constant into the existing
    names-the-symbol packaging error instead.
    """
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        return False
    return True


def _referenced_names(code: Any) -> set[str]:
    """All names *code* looks up by name (``LOAD_GLOBAL``/``LOAD_ATTR``/etc,
    i.e. ``co_names``), including names referenced only inside nested code
    objects (nested ``def``s, lambdas, comprehensions).

    Comprehensions get their own nested code object on Python < 3.12 and
    are inlined into the parent's code object from 3.12 on (PEP 709) --
    recursing into ``co_consts`` for nested code objects handles both.
    """
    names: set[str] = set(code.co_names)
    for const in code.co_consts:
        if isinstance(const, type(code)):
            names |= _referenced_names(const)
    return names


def _external_refs(func: Callable) -> list[str]:
    """Names *func* (transitively, including nested scopes) references that
    aren't its own parameters/locals/cellvars and aren't builtins -- i.e.
    names that can only resolve via ``func.__globals__`` at call time.
    """
    code = func.__code__
    local = set(code.co_varnames) | set(code.co_cellvars)
    out = []
    for name in sorted(_referenced_names(code)):
        if name in local or hasattr(builtins, name):
            continue
        out.append(name)
    return out


def _module_top_level_imports(module: Any) -> tuple[dict[str, str], set[str]]:
    """Map each name bound by a top-level ``import``/``from ... import`` in
    *module* to the exact source text of the statement that binds it, so
    only the imports a task actually needs can be selectively re-emitted.

    Returns ``(bindings, relative)`` -- names in *relative* are bound by a
    relative import (``from .helpers import x``), whose statement text
    cannot run in a deployed standalone script (no parent package there).
    They are tracked separately so packaging can fail naming them instead
    of re-emitting a guaranteed remote ``ImportError``.
    """
    if module is None:
        return {}, set()
    try:
        source = inspect.getsource(module)
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return {}, set()
    lines = source.splitlines()
    bindings: dict[str, str] = {}
    relative: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            start = node.lineno
            end = getattr(node, "end_lineno", node.lineno)
            stmt = "\n".join(lines[start - 1:end])
            is_relative = isinstance(node, ast.ImportFrom) and node.level > 0
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                if is_relative:
                    relative.add(bound)
                else:
                    bindings[bound] = stmt
    return bindings, relative


def _module_top_level_names(module: Any) -> list[str]:
    """All names bound at *module*'s top level (defs, classes, assignments,
    imports) -- used to describe what ``package="module"`` mode included.
    """
    if module is None:
        return []
    try:
        source = inspect.getsource(module)
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return []
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
    return sorted(names)


def _as_same_module_function(value: Any, module: Any) -> Optional[Callable]:
    """Resolve *value* to an includable same-module function, or None.

    Decorators like ``functools.lru_cache`` replace the function with a
    wrapper object that fails ``inspect.isfunction`` -- but they set
    ``__wrapped__``, so unwrapping recovers the original ``def``, whose
    source (including its decorator lines) is what packaging re-emits.
    """
    if not callable(value):
        return None
    try:
        target = value if inspect.isfunction(value) else inspect.unwrap(value)
    except ValueError:  # wrapper chain contains a cycle
        return None
    if inspect.isfunction(target) and inspect.getmodule(target) is module:
        return target
    return None


def _capture_closure(
    func: Callable,
    module: Any,
    included_constants: dict[str, Any],
    included_functions: dict[str, Callable],
) -> list[str]:
    """Materialize *func*'s closure cells into the deployed scope.

    A task defined inside a factory (``def make(threshold): @task ...``)
    references enclosing-scope locals via free variables, which a
    re-emitted top-level ``def`` resolves as globals instead -- so any
    JSON-serializable cell value can be captured as a module-level
    constant in the script, frozen at serialization time (the same
    contract module constants already have). Module-level functions
    captured under their own name are included as helpers. Everything
    else is returned as an error name for the caller's names-the-symbol
    failure.
    """
    code = func.__code__
    if not code.co_freevars:
        return []
    cells = func.__closure__ or ()
    errors: list[str] = []
    for name, cell in zip(code.co_freevars, cells):
        try:
            value = cell.cell_contents
        except ValueError:  # cell not yet filled (still being defined)
            errors.append(name)
            continue
        helper = _as_same_module_function(value, module)
        if helper is not None and helper.__name__ == name:
            included_functions[name] = helper
            continue
        if _is_json_serializable(value):
            included_constants[name] = value
            continue
        errors.append(name)
    return errors


def _package_task_auto(func: Callable) -> tuple[str, list[str], list[str]]:
    """Auto-detect and inline module-level names *func* (transitively)
    references. Returns ``(source_text, included_names)``.

    Raises :class:`PipelineError` naming exactly which referenced names
    couldn't be safely auto-included (an imported module/class/instance
    used directly as a value, a bound method, etc.) -- this is a *local*,
    deploy-time failure, replacing what used to be a silent deploy that
    only failed remotely with a ``NameError``.
    """
    module = inspect.getmodule(func)
    module_globals = func.__globals__
    import_bindings, relative_imports = _module_top_level_imports(module)

    included_imports: dict[str, str] = {}
    included_constants: dict[str, Any] = {}
    included_functions: dict[str, Callable] = {}
    errors: list[str] = []

    errors.extend(
        _capture_closure(func, module, included_constants, included_functions)
    )

    relative_errors: list[str] = []

    def visit(fn: Callable, with_decorators: bool = False) -> None:
        names = list(_external_refs(fn))
        if with_decorators:
            names.extend(_decorator_refs(fn))
        for name in names:
            if (
                name in included_imports
                or name in included_constants
                or name in included_functions
            ):
                continue
            if name in relative_imports:
                relative_errors.append(name)
                continue
            if name in import_bindings:
                included_imports[name] = import_bindings[name]
                continue
            if name not in module_globals:
                # Not resolvable at module scope from here (e.g. a plain
                # attribute-access name that happens to share co_names, or
                # a name that's simply undefined -- not this pass's job to
                # invent a value for it).
                continue
            value = module_globals[name]
            if value is func:
                continue  # self-reference; already included as the main function
            helper = _as_same_module_function(value, module)
            if helper is not None:
                included_functions[name] = helper
                # Helpers keep their decorators, so their decorator names
                # need resolving too.
                visit(helper, with_decorators=True)
                continue
            if _is_json_serializable(value):
                included_constants[name] = value
                continue
            errors.append(name)

    # Closure-captured helper functions get the same transitive treatment
    # as helpers found by name reference.
    for captured in list(included_functions.values()):
        visit(captured, with_decorators=True)
    visit(func)

    if relative_errors:
        names = ", ".join(sorted(set(relative_errors)))
        raise PipelineError(
            f"@task {func.__qualname__!r} references {names}, bound by a "
            "relative import -- a deployed script runs standalone, with no "
            "parent package for the import to resolve against, so "
            "re-emitting it would only fail remotely. Use an absolute "
            "import instead."
        )
    if errors:
        names = ", ".join(sorted(set(errors)))
        raise PipelineError(
            f"@task {func.__qualname__!r} references or closes over "
            f"{names}, which can't be safely auto-included in its deployed "
            "package (not JSON-serializable data, a same-module helper "
            "function, or an imported name). Either remove the reference, "
            "move the value somewhere serializable, or use "
            "@task(package=\"module\") to deploy the whole containing "
            "module instead."
        )

    parts: list[str] = []
    for stmt in sorted(set(included_imports.values())):
        parts.append(stmt)
    if included_imports:
        parts.append("")
    for name in sorted(included_constants):
        parts.append(f"{name} = {included_constants[name]!r}")
    if included_constants:
        parts.append("")
    for name in included_functions:  # insertion order == dependency-visit order
        parts.append(_extract_func_source(included_functions[name], keep_decorators=True))
        parts.append("")
    # Free variables were materialized above (or errored); the re-emitted
    # top-level def resolves those names as globals against the captured
    # constants/helpers.
    parts.append(_extract_func_source(func, allow_freevars=True))

    included_names = sorted(
        set(included_imports) | set(included_constants) | set(included_functions)
    )
    requires = _external_import_roots(set(included_imports.values()))
    return "\n".join(parts), included_names, requires


def _package_task_module(func: Callable) -> tuple[str, list[str], list[str]]:
    """Return the entire source of *func*'s containing module, verbatim.

    The explicit ``package="module"`` escape hatch -- broader/heavier than
    auto-detection (the whole file ships, not just what's referenced) but
    always works, for cases "auto" can't handle.
    """
    if func.__code__.co_freevars:
        names = ", ".join(sorted(func.__code__.co_freevars))
        raise PipelineError(
            f"@task(package=\"module\") can't deploy {func.__qualname__!r}: "
            f"it closes over {names} from an enclosing scope, and module "
            "mode ships the file verbatim -- the deployed wrapper would "
            "call a function that only exists inside its factory. Use "
            "package=\"auto\" (which captures JSON-serializable closure "
            "values) or move the task to module level."
        )
    module = inspect.getmodule(func)
    if module is None:
        raise PipelineError(
            f"@task(package=\"module\"): can't locate the containing "
            f"module for {func.__qualname__!r} (it may have been defined "
            "dynamically, e.g. via exec()) -- module packaging needs a "
            "real source file."
        )
    try:
        source = inspect.getsource(module)
    except (OSError, TypeError) as exc:
        raise PipelineError(
            f"@task(package=\"module\"): couldn't read source for module "
            f"{module.__name__!r}: {exc}"
        ) from exc
    try:
        tree = ast.parse(source)
    except SyntaxError:
        tree = None
    if tree is not None:
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.level > 0:
                raise PipelineError(
                    f"@task(package=\"module\") can't deploy "
                    f"{func.__qualname__!r}: module {module.__name__!r} "
                    "uses a relative import, which cannot resolve in a "
                    "deployed standalone script (no parent package "
                    "there). Use absolute imports in modules you deploy."
                )
    bindings, _relative = _module_top_level_imports(module)
    requires = _external_import_roots(set(bindings.values()))
    return textwrap.dedent(source), _module_top_level_names(module), requires


def _external_import_roots(statements: Iterable[str]) -> list[str]:
    """Root module names of *statements* that aren't in the standard
    library -- dependency-declaration groundwork (brokoli-sdk#22 M3): the
    deployed script needs these importable on the host Python, and the
    package block should say so instead of leaving it to a remote
    ImportError. On interpreters without ``sys.stdlib_module_names``
    (< 3.10) the distinction can't be made cheaply, so nothing is listed.
    """
    stdlib = getattr(sys, "stdlib_module_names", None)
    if stdlib is None:
        return []
    roots: set[str] = set()
    for stmt in statements:
        try:
            tree = ast.parse(textwrap.dedent(stmt))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    roots.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                roots.add(node.module.split(".")[0])
    return sorted(r for r in roots if r not in stdlib and r != "brokoli")


def _package_task_source(
    func: Callable, mode: str
) -> tuple[str, Optional[dict[str, Any]]]:
    """Extract *func*'s deployable source per *mode* (``"auto"``/``"module"``).

    Returns ``(source_text, package_meta)``. *package_meta* is ``None`` when
    nothing beyond the isolated function itself was needed (mode "auto"
    with no external references) -- callers should omit the IR "package"
    block entirely in that case, so a task using none of this machinery
    serializes exactly as it did before this feature existed.
    """
    if mode not in _PACKAGE_MODES:
        raise PipelineError(
            f"@task(package={mode!r}): expected one of {_PACKAGE_MODES!r}"
        )

    if mode == "module":
        source_text, included, requires = _package_task_module(func)
        package_meta = {
            "mode": "module",
            "included": included,
            "callable": f"{func.__module__}:{func.__qualname__}",
        }
        if requires:
            package_meta["requires_modules"] = requires
        return source_text, package_meta

    source_text, included, requires = _package_task_auto(func)
    if not included:
        return source_text, None
    package_meta = {
        "mode": "auto",
        "included": included,
        "callable": f"{func.__module__}:{func.__qualname__}",
    }
    if requires:
        package_meta["requires_modules"] = requires
    return source_text, package_meta


# ---------------------------------------------------------------------------
# Node references
# ---------------------------------------------------------------------------

class NodeRef:
    """Reference to a node in the pipeline DAG.  Supports ``>>`` for chaining."""

    def __init__(self, node_id: str, pipeline: "Pipeline") -> None:
        self.node_id = node_id
        self.pipeline = pipeline

    def __repr__(self) -> str:
        return f"NodeRef({self.node_id!r})"

    def __rshift__(self, other: Any) -> "NodeRef | _MultiRef":
        """``a >> b``, ``a >> [b, c]``, ``a >> func``."""
        if isinstance(other, list):
            with self.pipeline._transaction(other):
                refs = [self._resolve(item) for item in other]
                for ref in refs:
                    self.pipeline._add_edge(self.node_id, ref.node_id)
            return _MultiRef(refs, self.pipeline)

        ref = self._resolve(other)
        self.pipeline._add_edge(self.node_id, ref.node_id)
        return ref

    def __or__(self, other: Any) -> "_MultiRef":
        """``a | b`` -- group for parallel fan-out (no edge added)."""
        with self.pipeline._transaction([other]):
            ref = self._resolve(other)
        return _MultiRef([self, ref], self.pipeline)

    def _resolve(self, item: Any) -> "NodeRef":
        """Convert a node-like object to a :class:`NodeRef`."""
        owner = getattr(item, "pipeline", getattr(item, "_pipeline", self.pipeline))
        if owner is not self.pipeline:
            raise PipelineError("Nodes from different pipelines cannot be connected.")
        if isinstance(item, NodeRef):
            self.pipeline._validate_refs([item])
            return item
        if isinstance(item, _MultiRef):
            self.pipeline._validate_refs(item.refs)
            return item.refs[0]
        if isinstance(item, _ConditionBranch):
            self.pipeline._validate_refs([item._ref])
            return item._ref
        # Auto-call wrappers that haven't been invoked yet
        _auto_call_types = (
            _TaskWrapper, _SourceWrapper, _SinkWrapper,
            _FilterWrapper, _MapWrapper, _ValidateWrapper, _SensorWrapper,
        )
        if isinstance(item, _auto_call_types):
            return item()
        raise TypeError(f"Cannot connect to {type(item).__name__}")


class ConditionRef(NodeRef):
    """Reference to a condition node with explicit true/false routing."""

    def when(self, other: Any) -> "NodeRef | _MultiRef":
        """Connect *other* to the true branch."""
        return self._route(other, True)

    def otherwise(self, other: Any) -> "NodeRef | _MultiRef":
        """Connect *other* to the false branch."""
        return self._route(other, False)

    def _route(self, other: Any, selected: bool) -> "NodeRef | _MultiRef":
        incoming = sum(1 for _, to_id, _ in self.pipeline._edges if to_id == self.node_id)
        if incoming != 1:
            raise PipelineError(
                f"Condition node {self.node_id!r} must have exactly one input "
                f"before adding branches; got {incoming}."
            )
        expression = self.pipeline._nodes[self.node_id]["config"].get("expression", "")
        if not isinstance(expression, str) or not expression.strip():
            raise PipelineError(
                f"Condition node {self.node_id!r} requires a non-empty expression."
            )

        ancestors = [
            from_id
            for from_id, to_id, _ in self.pipeline._edges
            if to_id == self.node_id
        ]
        visited: set[str] = set()
        while ancestors:
            node_id = ancestors.pop()
            if node_id in visited:
                continue
            visited.add(node_id)
            if self.pipeline._nodes.get(node_id, {}).get("type") == "condition":
                raise PipelineError(
                    "Nested conditional routing is not supported; condition "
                    f"node {self.node_id!r} is downstream of {node_id!r}."
                )
            ancestors.extend(
                from_id
                for from_id, to_id, _ in self.pipeline._edges
                if to_id == node_id
            )

        items = other if isinstance(other, list) else [other]
        if not items:
            raise PipelineError("Condition branches require at least one destination.")
        for item in items:
            owner = getattr(item, "pipeline", getattr(item, "_pipeline", self.pipeline))
            if owner is not self.pipeline:
                raise PipelineError("Condition branches cannot cross pipeline boundaries.")
        with self.pipeline._transaction(items):
            refs = [self._resolve(item) for item in items]
            for ref in refs:
                if self.pipeline._nodes.get(ref.node_id, {}).get("type") == "condition":
                    raise PipelineError(
                        "Nested conditional routing is not supported; route to a "
                        "non-condition node instead."
                    )
                for existing_from, existing_to, existing_condition in self.pipeline._edges:
                    if existing_from == self.node_id and existing_to == ref.node_id:
                        if existing_condition != selected:
                            raise PipelineError(
                                f"Edge {self.node_id!r} -> {ref.node_id!r} cannot "
                                "belong to multiple branches."
                            )
            for ref in refs:
                self.pipeline._add_edge(self.node_id, ref.node_id, condition=selected)

        if isinstance(other, list):
            return _MultiRef(refs, self.pipeline)
        return refs[0]


# ---------------------------------------------------------------------------
# Typed node references (brokoli-sdk#2)
# ---------------------------------------------------------------------------
#
# NodeRef subclasses that let node-building functions declare what *kind*
# of thing a node produces -- so pipeline code (and IDEs/type checkers)
# can tell "this produces one scalar value" apart from "this produces a
# multi-gigabyte dataset" apart from "this produces a dynamic collection
# of N items whose size isn't known until the pipeline runs" (RFC §19.1).
#
# All four add no fields beyond plain NodeRef, so every existing NodeRef
# consumer (``>>``, ``_resolve``, ``_MultiRef``, chaining, ...) keeps
# working completely unchanged -- this is a purely additive typing layer.
#
# Not every node-building function returns one of these; see
# brokoli/nodes.py and the PR description for the full mapping and the
# reasoning behind it (some node kinds are genuinely ambiguous -- e.g. a
# ``code`` node can produce anything -- and are deliberately left as
# plain NodeRef rather than force-fit into one of these four).
#
# SCOPE: this is SDK API surface and IR compilation only. There is no
# backend support yet for dynamic per-item task instances or partition
# execution -- see brokoli-sdk#2 and the RFC §11-13 physical-planner
# tracking in Tnsor-Labs/brokoli.

class ScalarRef(NodeRef):
    """A :class:`NodeRef` whose node produces a single scalar value.

    E.g. ``source_api(..., response="scalar", value_path="data.count")``.
    """


class ArtifactRef(NodeRef):
    """A :class:`NodeRef` whose node produces an opaque artifact (a file
    or binary blob) rather than a tabular dataset or a single value.

    E.g. ``source_api(..., response="artifact")``.
    """


class DatasetRef(NodeRef):
    """A :class:`NodeRef` whose node produces a tabular dataset (rows).

    Adds ``.map()``/``.filter()``, which compile to *partition-transform*
    IR nodes (node types ``dataset_map``/``dataset_filter``) -- distinct
    from the ``@map``/``@filter`` decorators in ``brokoli.decorators``,
    which register ordinary ``code`` nodes.

    When to use which (RFC §19.7 flags unexplained parallel API styles as
    a real usability problem, so to be explicit):

    - ``@map``/``@filter`` (decorators): write a plain Python function
      that runs once and sees the *whole* node output (a list of row
      dicts) at once. This is the only one of the two styles that
      actually executes today -- the decorator generates a runnable
      script and registers a ``code`` node.
    - ``DatasetRef.map()``/``.filter()`` (these methods): describe a
      transform intended to run *per-partition*, independently and in
      parallel, once the backend physical planner exists (RFC §12.1).
      These compile to ``dataset_map``/``dataset_filter`` IR nodes only
      -- *there is no backend support to execute them yet*; the function
      you pass is recorded as a name reference, not serialized as
      runnable code or invoked locally.
    """

    def map(
        self, fn: Callable, name: str = "", node_key: Optional[str] = None
    ) -> "DatasetRef":
        """Compile a partition-level map transform over this dataset.

        See the class docstring for how this differs from ``@map``.
        """
        return _add_partition_transform_node(
            self, "dataset_map", fn, name, node_key=node_key
        )

    def filter(
        self, fn: Callable, name: str = "", node_key: Optional[str] = None
    ) -> "DatasetRef":
        """Compile a partition-level filter transform over this dataset.

        See the class docstring for how this differs from ``@filter``.
        """
        return _add_partition_transform_node(
            self, "dataset_filter", fn, name, node_key=node_key
        )


class CollectionRef(NodeRef):
    """A :class:`NodeRef` whose node produces a dynamic collection of
    items (e.g. one entry per file/page/record from an upstream source)
    whose exact count isn't known until the pipeline runs.

    Not returned by any built-in source today -- dynamic collections are
    backend (physical-planner) work that doesn't exist yet. Currently
    produced only by ``_TaskWrapper.expand()`` (the dynamic fan-out of a
    ``@task`` over an upstream ``CollectionRef``).
    """

    def collect(
        self,
        mode: str = "union",
        name: str = "",
        node_key: Optional[str] = None,
    ) -> "DatasetRef":
        """Combine this collection's per-item outputs into one dataset.

        Compiles to the same ``union`` IR node shape as the module-level
        ``union()`` function (``brokoli.nodes.union``) -- just with a
        single upstream edge (this collection) instead of one edge per
        explicit ref.
        """
        collect_name = name or f"{_node_display_name(self)} (Collected)"
        return _build_union_node(
            self.pipeline, collect_name, [self], mode=mode, node_key=node_key
        )


def _node_display_name(ref: NodeRef) -> str:
    """Best-effort human-readable name for *ref*'s node (falls back to its id)."""
    node = ref.pipeline._nodes.get(ref.node_id)
    return node["name"] if node else ref.node_id


def _func_ref(fn: Callable) -> dict[str, Any]:
    """Serialize *fn* as a name/description reference, not executable code.

    Used for callables passed to primitives whose actual invocation is
    backend (physical-planner) work that doesn't exist yet -- ``.expand()``'s
    ``key=`` and ``DatasetRef.map()``/``.filter()``'s ``fn`` -- unlike the
    ``@task``/``@map``/``@filter`` decorators, which generate a runnable
    script from the function's source right now.
    """
    ref: dict[str, Any] = {"name": getattr(fn, "__name__", repr(fn))}
    doc = getattr(fn, "__doc__", None)
    if doc:
        ref["doc"] = doc.strip().splitlines()[0]
    return ref


def _build_union_node(
    pipeline: "Pipeline",
    name: str,
    refs: list[NodeRef],
    mode: str = "union",
    node_key: Optional[str] = None,
) -> "DatasetRef":
    """Register a dataset-manifest-combination (``union``) node.

    Combines the manifests of *refs* into a single dataset node -- one
    ``union`` IR node with an edge from each ref -- instead of a chain of
    individual merge edges. Shared by the module-level ``union()``
    node-building function (``brokoli.nodes``) and
    :meth:`CollectionRef.collect`, so both compile to the exact same IR
    node shape (``union(name, a, b, c)`` and
    ``collection.collect(mode="union")`` produce the same node type,
    capabilities, and config -- just a different edge count).
    """
    if mode != "union":
        raise ValueError(
            f"union()/collect() only support mode='union' currently, got {mode!r}"
        )
    if not refs:
        raise ValueError("union()/collect() requires at least one upstream ref")

    pipeline._validate_refs(refs)
    node_id = pipeline._allocate_node_id(name, node_key)
    pipeline._add_node(node_id, "union", name, {"mode": mode})
    for ref in refs:
        pipeline._add_edge(ref.node_id, node_id)
    return DatasetRef(node_id, pipeline)


def _add_partition_transform_node(
    upstream: "DatasetRef",
    node_type: str,
    fn: Callable,
    name: str,
    node_key: Optional[str] = None,
) -> "DatasetRef":
    """Register a ``dataset_map``/``dataset_filter`` partition-transform node."""
    upstream.pipeline._validate_refs([upstream])
    display_name = name or f"{node_type}({getattr(fn, '__name__', 'fn')})"
    config = {"function": _func_ref(fn)}
    node_id = upstream.pipeline._allocate_node_id(display_name, node_key)
    upstream.pipeline._add_node(node_id, node_type, display_name, config)
    upstream.pipeline._add_edge(upstream.node_id, node_id)
    return DatasetRef(node_id, upstream.pipeline)


class _MultiRef:
    """Multiple node refs -- result of ``[a, b]`` or ``a >> [b, c]``."""

    def __init__(self, refs: list[NodeRef], pipeline: "Pipeline") -> None:
        pipeline._validate_refs(refs)
        self.refs = refs
        self.pipeline = pipeline

    def __rshift__(self, other: Any) -> "NodeRef | _MultiRef":
        """``[a, b] >> c`` -- fan-in: all connect to target."""
        if isinstance(other, list):
            if len(other) != len(self.refs):
                raise PipelineError(
                    f"Cannot pair {len(self.refs)} upstream node(s) with "
                    f"{len(other)} target(s): the lists must match one-to-one. "
                    "The extra entries would previously be dropped silently."
                )
            pairs = list(zip(self.refs, other))
            with self.pipeline._transaction(other):
                targets = [ref._resolve(item) for ref, item in pairs]
                for (ref, _), target in zip(pairs, targets):
                    self.pipeline._add_edge(ref.node_id, target.node_id)
            return _MultiRef(targets, self.pipeline)

        with self.pipeline._transaction([other]):
            target = self.refs[0]._resolve(other)
            for ref in self.refs:
                self.pipeline._add_edge(ref.node_id, target.node_id)
        return target


# ---------------------------------------------------------------------------
# Condition helpers
# ---------------------------------------------------------------------------

class _ConditionBranch:
    """Represents the true or false branch of a condition node."""

    def __init__(self, ref: NodeRef, pipeline: "Pipeline") -> None:
        self._ref = ref
        self.pipeline = pipeline

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        return self._ref >> other


class _ConditionContext:
    """Context manager for condition branching.

    Usage::

        with condition(source) as (ok, fail):
            ok >> sink_a
            fail >> sink_b
    """

    def __init__(
        self,
        true_ref: NodeRef,
        false_ref: NodeRef,
        pipeline: "Pipeline",
    ) -> None:
        self._true = _ConditionBranch(true_ref, pipeline)
        self._false = _ConditionBranch(false_ref, pipeline)
        self.pipeline = pipeline

    def __enter__(self) -> tuple[_ConditionBranch, _ConditionBranch]:
        return self._true, self._false

    def __exit__(self, *args: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Decorator wrappers
# ---------------------------------------------------------------------------

def _validate_wrapper_inputs(pipeline: "Pipeline", inputs: tuple[NodeRef, ...]) -> None:
    """Validate wrapper inputs before allocating an ID."""
    pipeline._validate_refs(list(inputs))


class _TaskWrapper:
    """Wraps a ``@task`` decorated function.  Calling it registers the node."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        config: dict[str, Any],
        package: str = "auto",
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self._package = package
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        # Cache of the node created by the zero-arg "auto-call" path (used
        # by ``_resolve``/chaining), so reusing this wrapper instance across
        # multiple fan-out edges doesn't register a duplicate node. Explicit
        # calls with inputs (e.g. calling the same @task twice with
        # different upstream data) always create a fresh node and never
        # populate or consult this cache.
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        """Register this task as a node with edges from *inputs*."""
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)

        func_source, package_meta = _package_task_source(self._func, self._package)

        call_script = TASK_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
        )

        config: dict[str, Any] = {
            "language": "python",
            "script": call_script,
            **self._config,
        }
        if package_meta is not None:
            config["package"] = package_meta

        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config)

        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)

        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        """``extract >> task_func`` -- auto-call with no explicit input."""
        return self() >> other

    def __rrshift__(self, other: Any) -> NodeRef:
        """``source >> task_func``."""
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]

    def expand(
        self,
        key: Optional[Callable] = None,
        node_key: Optional[str | "CollectionRef"] = None,
        **kwargs: "CollectionRef",
    ) -> "CollectionRef":
        """Fan this task out into one dynamic instance per item of an
        upstream collection, compiling to a *single* IR node carrying an
        ``expansion`` policy block -- not N static nodes -- driven by
        data whose size isn't known until the pipeline runs.

        Mirrors, in IR form, what ``a >> [b, c]`` does at authoring time
        for a Python list literal known up front, except the fan-out
        factor here is determined at run time by an upstream
        :class:`CollectionRef`.

        Args:
            key: Optional callable used to derive a stable per-item
                instance identity (so re-runs don't spawn duplicate
                instances for the same item). Not executed locally or
                serialized as runnable code -- only a name/description
                reference is recorded (see :func:`_func_ref`); the actual
                per-item keying happens server-side once backend support
                for dynamic instances exists.
            node_key: Logical node identity when passed a string. For
                backward compatibility, a CollectionRef passed here is
                treated as the expansion input named ``node_key``.
            **kwargs: Maps a task parameter name to the upstream
                :class:`CollectionRef` driving expansion over that
                parameter, e.g. ``parsed = parse.expand(file=files)``.

        Returns:
            A :class:`CollectionRef` representing the dynamic collection
            of per-instance outputs. Chain ``.collect(mode="union")`` to
            merge them back into a single dataset.

        Note:
            SDK API surface and IR compilation only -- there is no
            backend support yet for actually scheduling dynamic per-item
            task instances or executing this expansion (brokoli-sdk#2).
        """
        logical_node_key: Optional[str]
        if isinstance(node_key, CollectionRef):
            kwargs["node_key"] = node_key
            logical_node_key = None
        else:
            logical_node_key = node_key

        if not kwargs:
            raise ValueError(
                "expand() requires at least one keyword mapping a task "
                "parameter to a CollectionRef, e.g. .expand(file=files)"
            )
        for param, ref in kwargs.items():
            if not isinstance(ref, CollectionRef):
                raise TypeError(
                    f"expand({param}=...): expected a CollectionRef (the "
                    f"dynamic collection driving expansion), got "
                    f"{type(ref).__name__}. CollectionRef isn't produced "
                    "by any built-in source yet -- construct one "
                    "directly, e.g. CollectionRef(some_ref.node_id, "
                    "pipeline), when prototyping IR shape ahead of "
                    "backend support."
                )
        self._pipeline._validate_refs(list(kwargs.values()))

        func_source, package_meta = _package_task_source(self._func, self._package)
        call_script = TASK_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
        )

        expansion: dict[str, Any] = {
            "over": {param: ref.node_id for param, ref in kwargs.items()},
        }
        if key is not None:
            expansion["key"] = _func_ref(key)

        config: dict[str, Any] = {
            "language": "python",
            "script": call_script,
            **self._config,
            "expansion": expansion,
        }
        if package_meta is not None:
            config["package"] = package_meta

        node_id = self._pipeline._allocate_node_id(
            self._name,
            self._node_key if logical_node_key is None else logical_node_key,
        )
        self._pipeline._add_node(
            node_id, "code", self._name, config,
            capabilities=["compute", "dynamic-expansion", "collection-output"],
        )
        for ref in kwargs.values():
            self._pipeline._add_edge(ref.node_id, node_id)

        return CollectionRef(node_id, self._pipeline)


class _ConditionWrapper:
    """Wraps a ``@condition`` decorated function."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._node_key = node_key
        self.__wrapped__ = func

    def __call__(
        self, input_ref: NodeRef, node_key: Optional[str] = None
    ) -> _ConditionContext:
        raise PipelineError(
            "@condition predicates are not supported by the runtime input "
            "contract. Use condition_node(..., expression=...) with "
            ".when() and .otherwise() instead."
        )


class _BranchRef(NodeRef):
    """A :class:`NodeRef` that tracks which branch (true/false) it represents."""

    def __init__(self, cond_id: str, branch: str, pipeline: "Pipeline") -> None:
        super().__init__(cond_id, pipeline)
        self._branch = branch

    def __repr__(self) -> str:
        return f"_BranchRef({self.node_id!r}, branch={self._branch!r})"

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        if isinstance(other, list):
            with self.pipeline._transaction(other):
                refs = [self._resolve(item) for item in other]
                for ref in refs:
                    self.pipeline._add_edge(self.node_id, ref.node_id)
                    self.pipeline._branches[self.node_id][self._branch].append(ref.node_id)
            return _MultiRef(refs, self.pipeline)

        ref = self._resolve(other)
        self.pipeline._add_edge(self.node_id, ref.node_id)
        self.pipeline._branches[self.node_id][self._branch].append(ref.node_id)
        return ref


class _SourceWrapper:
    """Wraps a ``@source`` decorated function. Registers as a source code node."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        config: dict[str, Any],
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        # Sources take no input, so every call is an "auto-call" -- cache
        # unconditionally to avoid registering a duplicate node when the
        # same wrapper instance is reused across multiple fan-out edges.
        self._auto_ref: Optional[NodeRef] = None

    def __call__(self, node_key: Optional[str] = None) -> NodeRef:
        if node_key is None and self._auto_ref is not None:
            return self._auto_ref
        func_source, package_meta = _package_task_source(self._func, "auto")
        script = SOURCE_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script, **self._config}
        if package_meta is not None:
            config["package"] = package_meta
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config, capabilities=["source", "dataset-output"])
        ref = NodeRef(node_id, self._pipeline)
        if node_key is None:
            self._auto_ref = ref
        return ref

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        return self() >> other


class _SinkWrapper:
    """Wraps a ``@sink`` decorated function. Registers as a sink code node."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        config: dict[str, Any],
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)
        func_source, package_meta = _package_task_source(self._func, "auto")
        script = SINK_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script, **self._config}
        if package_meta is not None:
            config["package"] = package_meta
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config, capabilities=["sink"])
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _FilterWrapper:
    """Wraps a ``@filter`` decorated function. Registers as a code node that filters rows."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)
        func_source, package_meta = _package_task_source(self._func, "auto")
        script = FILTER_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script}
        if package_meta is not None:
            config["package"] = package_meta
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _MapWrapper:
    """Wraps a ``@map`` decorated function. Registers as a code node that transforms each row."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)
        func_source, package_meta = _package_task_source(self._func, "auto")
        script = MAP_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script}
        if package_meta is not None:
            config["package"] = package_meta
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _ValidateWrapper:
    """Wraps a ``@validate`` decorated function. Registers as a quality-gate code node."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        on_failure: str,
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._on_failure = on_failure
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)
        func_source, package_meta = _package_task_source(self._func, "auto")
        action = 'raise ValueError(f"Validation failed: {_message}")' if self._on_failure == "block" else ""
        script = VALIDATE_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
            on_failure_action=action,
        )
        config: dict[str, Any] = {"language": "python", "script": script}
        if package_meta is not None:
            config["package"] = package_meta
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _SensorWrapper:
    """Wraps a ``@sensor`` decorated function. Polls until the function returns True."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        poll_interval: int,
        timeout: Any,
        node_key: Optional[str] = None,
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._node_key = node_key
        self.__wrapped__ = func
        self.__name__ = func.__name__
        self._auto_ref: Optional[NodeRef] = None

    def __call__(
        self, *inputs: NodeRef, node_key: Optional[str] = None
    ) -> NodeRef:
        if not inputs and node_key is None and self._auto_ref is not None:
            return self._auto_ref
        _validate_wrapper_inputs(self._pipeline, inputs)
        func_source, package_meta = _package_task_source(self._func, "auto")
        # An explicit UNSET/None timeout means "poll forever" -- the
        # generated script skips its timeout check, and the node's
        # orchestrator-level ``timeout`` config key (poll timeout + a 60s
        # buffer for the last poll/teardown) is omitted entirely rather
        # than always adding 60 to whatever value was passed.
        has_timeout = self._timeout is not UNSET and self._timeout is not None
        script = SENSOR_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
            poll_interval=self._poll_interval,
            timeout=repr(self._timeout) if has_timeout else "None",
        )
        config: dict[str, Any] = {"language": "python", "script": script}
        if package_meta is not None:
            config["package"] = package_meta
        if has_timeout:
            config["timeout"] = self._timeout + 60
        node_id = self._pipeline._allocate_node_id(
            self._name, self._node_key if node_key is None else node_key
        )
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        ref = NodeRef(node_id, self._pipeline)
        if not inputs and node_key is None:
            self._auto_ref = ref
        return ref

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        return self() >> other

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

_SLUG_SANITIZE = re.compile(r"[^a-z0-9-]")
_SLUG_COLLAPSE = re.compile(r"-+")

_HOOK_TYPES = ("webhook", "slack", "email")


def _coerce_hook(name: str, value: Any) -> "dict[str, Any] | None":
    """Normalize a lifecycle hook to the server's Hook shape.

    A URL string becomes a webhook hook; a dict must carry a supported
    'type' and a 'url'. Python callables are rejected with guidance --
    they cannot execute server-side, and for years they were silently
    replaced by an empty placeholder. The server persists hooks since
    v0.10.10, so what's emitted here is now real.
    """
    if value is None:
        return None
    if callable(value):
        raise PipelineError(
            f"Pipeline({name}=...) got a Python callable, which cannot run "
            "server-side (and was previously discarded without warning). "
            "Pass a webhook URL string, or a dict like "
            "{'type': 'slack', 'url': ...}."
        )
    if isinstance(value, str):
        return {"type": "webhook", "url": value, "enabled": True}
    if isinstance(value, dict):
        hook_type = value.get("type", "webhook")
        url = value.get("url", "")
        if hook_type not in _HOOK_TYPES or not url:
            raise PipelineError(
                f"Pipeline({name}=...) dict needs a 'url' and a 'type' in "
                f"{_HOOK_TYPES}."
            )
        out = {"type": hook_type, "url": url, "enabled": value.get("enabled", True)}
        if value.get("extra"):
            out["extra"] = value["extra"]
        return out
    raise PipelineError(
        f"Pipeline({name}=...) must be a URL string or a hook dict, "
        f"got {type(value).__name__}."
    )


_HOOK_NAMES: tuple[str, ...] = ("on_start", "on_success", "on_failure")


class Pipeline:
    """Pipeline definition -- use as a context manager."""

    _current: Optional["Pipeline"] = None

    @staticmethod
    def _generate_pipeline_id(name: str) -> str:
        """Generate a stable slug from a pipeline name."""
        pid = name.lower().replace(" ", "-")
        pid = _SLUG_SANITIZE.sub("", pid)
        pid = _SLUG_COLLAPSE.sub("-", pid).strip("-")
        return pid

    _UNSUPPORTED_OPTIONS = ("catch_up", "max_retries", "concurrency")

    def __init__(
        self,
        name: str,
        pipeline_id: str | None = None,
        description: str = "",
        schedule: str = "",
        catch_up: bool | None = None,
        sla: str = "",
        depends_on: list[str] | None = None,
        tags: list[str] | None = None,
        webhook: bool = False,
        max_retries: int | None = None,
        concurrency: int | None = None,
        on_start: "str | dict[str, Any] | None" = None,
        on_success: "str | dict[str, Any] | None" = None,
        on_failure: "str | dict[str, Any] | None" = None,
    ) -> None:
        # Serialize-or-reject (brokoli-sdk#23 M2): these three were
        # accepted for years and silently dropped from the compiled IR.
        # The server has no fields for them, and since v0.10.11 its
        # strict decoder would reject them anyway -- so accepting them
        # locally was pure fiction. Reject with the honest state.
        for option, value in (
            ("catch_up", catch_up),
            ("max_retries", max_retries),
            ("concurrency", concurrency),
        ):
            if value is not None:
                raise PipelineError(
                    f"Pipeline({option}=...) is not supported by the server "
                    "and was previously discarded without warning. Remove it; "
                    "per-node retries= is real, and backfill exists as a "
                    "server-side operation."
                )
        self.name = name
        self.pipeline_id = pipeline_id or self._generate_pipeline_id(name)
        self.description = description
        self.schedule = schedule
        self.tags: list[str] = tags or []
        self.webhook = webhook
        self.on_start = _coerce_hook("on_start", on_start)
        self.on_success = _coerce_hook("on_success", on_success)
        self.on_failure = _coerce_hook("on_failure", on_failure)
        self.depends_on: list[str] = depends_on or []

        # SLA: "07:30 America/New_York" -> deadline + timezone
        self.sla_deadline: str = ""
        self.sla_timezone: str = ""
        if sla:
            parts = sla.split(" ", 1)
            self.sla_deadline = parts[0]
            self.sla_timezone = parts[1] if len(parts) > 1 else "UTC"

        # DAG state
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: list[tuple[str, str, bool | None]] = []
        self._branches: dict[str, dict[str, list[str]]] = {}
        self._node_order: list[str] = []
        self._node_id_counters: dict[str, int] = {}

    def __repr__(self) -> str:
        return (
            f"Pipeline({self.name!r}, nodes={len(self._nodes)}, "
            f"edges={len(self._edges)})"
        )

    # -- Context manager --------------------------------------------------

    def __enter__(self) -> "Pipeline":
        Pipeline._current = self
        return self

    def __exit__(self, *args: object) -> None:
        Pipeline._current = None

    # -- DAG mutation ------------------------------------------------------

    def _validate_refs(self, refs: list[NodeRef]) -> None:
        """Require refs to be real nodes owned by this pipeline."""
        for ref in refs:
            if not isinstance(ref, NodeRef):
                raise PipelineError(
                    f"Expected a NodeRef input, got {type(ref).__name__}."
                )
            if ref.pipeline is not self:
                raise PipelineError("Nodes from different pipelines cannot be connected.")
            if ref.node_id not in self._nodes:
                raise PipelineError(
                    f"Node {ref.node_id!r} does not exist in its owning pipeline."
                )

    @contextmanager
    def _transaction(self, items: list[Any]):
        """Roll back graph, allocator, and lazy-wrapper state on failure."""
        nodes_before = dict(self._nodes)
        edges_before = list(self._edges)
        order_before = list(self._node_order)
        branches_before = {
            node_id: {
                branch: list(destinations)
                for branch, destinations in branch_map.items()
            }
            for node_id, branch_map in self._branches.items()
        }
        counters_before = dict(self._node_id_counters)
        cached_refs: list[tuple[Any, Optional[NodeRef]]] = []

        def capture(item: Any) -> None:
            if isinstance(item, (list, tuple)):
                for child in item:
                    capture(child)
            elif hasattr(item, "_auto_ref"):
                cached_refs.append((item, item._auto_ref))

        capture(items)
        try:
            yield
        except Exception:
            self._nodes = nodes_before
            self._edges = edges_before
            self._node_order = order_before
            self._branches = branches_before
            self._node_id_counters = counters_before
            for item, cached_ref in cached_refs:
                item._auto_ref = cached_ref
            raise

    def _allocate_node_id(
        self, name: str, node_key: Optional[str] = None
    ) -> str:
        """Allocate a deterministic logical node ID owned by this pipeline."""
        if node_key is not None:
            if not isinstance(node_key, str) or not _NODE_KEY_PATTERN.fullmatch(node_key):
                raise PipelineError(
                    "node_key must match ^[a-z][a-z0-9_-]{0,63}$ exactly"
                )
            if node_key in self._nodes:
                raise PipelineError(f"Duplicate node id {node_key!r}.")
            return node_key

        base = _make_id(name)
        counter = self._node_id_counters.get(base, 0)
        while True:
            counter += 1
            candidate = f"{base}_{counter}"
            if candidate not in self._nodes:
                self._node_id_counters[base] = counter
                return candidate

    def _add_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        config: dict[str, Any],
        capabilities: list[str] | None = None,
    ) -> None:
        """Register a node.

        *capabilities* defaults to the lookup in ``NODE_TYPE_CAPABILITIES``
        for *node_type*. Callers that register decorator-based nodes (which
        always use the literal type ``"code"``) pass an explicit override
        so the capability reflects the decorator (``@source``, ``@sink``,
        ...) rather than the generic code-node default.
        """
        if node_id in self._nodes:
            raise PipelineError(
                f"Duplicate node id {node_id!r} (name={name!r}). "
                "Each node must have a unique id."
            )
        self._nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "name": name,
            "config": config,
            "capabilities": list(capabilities) if capabilities is not None else _capabilities_for(node_type),
        }
        self._node_order.append(node_id)

    def _add_edge(self, from_id: str, to_id: str, condition: bool | None = None) -> None:
        if from_id not in self._nodes or to_id not in self._nodes:
            raise PipelineError("Nodes from different pipelines cannot be connected.")
        if condition is not None and type(condition) is not bool:
            raise PipelineError("Edge condition must be true, false, or omitted.")
        for existing_from, existing_to, existing_condition in self._edges:
            if existing_from != from_id or existing_to != to_id:
                continue
            if existing_condition == condition:
                return
            raise PipelineError(
                f"Edge {from_id!r} -> {to_id!r} cannot belong to multiple branches."
            )
        self._edges.append((from_id, to_id, condition))

    # -- Serialization -----------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        """Convert pipeline to Brokoli API JSON format with auto-layout."""
        self._validate_conditional_routing()
        positions = self._auto_layout()

        nodes: list[dict[str, Any]] = []
        for nid in self._node_order:
            node = self._nodes[nid]
            node_data: dict[str, Any] = {
                "id": node["id"],
                "type": node["type"],
                "name": node["name"],
                "config": copy.deepcopy(node["config"]),
                "capabilities": list(node.get("capabilities") or _capabilities_for(node["type"])),
                "position": positions.get(nid, {"x": 0, "y": 0}),
            }
            self._annotate_schema_hint(node, node_data)
            nodes.append(node_data)

        result: dict[str, Any] = {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "enabled": True,
            "ir_version": (
                CONDITIONAL_ROUTING_IR_VERSION
                if any(condition is not None for _, _, condition in self._edges)
                else IR_VERSION
            ),
            "nodes": nodes,
            "edges": [
                {
                    "from": from_id,
                    "to": to_id,
                    **({} if condition is None else {"condition": condition}),
                }
                for from_id, to_id, condition in self._edges
            ],
            "tags": self.tags,
            "depends_on": self.depends_on,
        }

        if self.sla_deadline:
            result["sla_deadline"] = self.sla_deadline
            result["sla_timezone"] = self.sla_timezone

        hooks = self._build_hooks()
        if hooks:
            result["hooks"] = hooks

        if self.webhook:
            result["webhook_token"] = ""  # server will generate

        return result

    def to_normalized_json(self) -> dict[str, Any]:
        """Return normalized IR for snapshots and semantic comparisons."""
        from brokoli.ir import normalize_ir

        return normalize_ir(self.to_json())

    def to_yaml(self) -> str:
        """Convert pipeline to YAML format.

        Produces clean, human-readable YAML without escape sequences.
        Scripts in code nodes are rendered as YAML block scalars (``|``).

        Example::

            with Pipeline("my-pipeline") as p:
                source_db("Load", query="SELECT 1", conn_id="pg")

            print(p.to_yaml())
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML output. Install it: pip install pyyaml"
            )

        data = self.to_json()

        # Custom representer: render multi-line strings as block scalars (|)
        # so scripts, queries, etc. appear clean without \\n escapes.
        def _str_representer(dumper: yaml.Dumper, value: str) -> yaml.ScalarNode:
            if "\n" in value:
                return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="|")
            return dumper.represent_scalar("tag:yaml.org,2002:str", value)

        class _CleanDumper(yaml.Dumper):
            pass

        _CleanDumper.add_representer(str, _str_representer)

        return yaml.dump(
            data,
            Dumper=_CleanDumper,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )

    # -- Private helpers ---------------------------------------------------

    def _validate_conditional_routing(self) -> None:
        conditional_sources = {
            from_id
            for from_id, _, condition in self._edges
            if condition is not None
        }
        if not conditional_sources:
            return

        adjacency: dict[str, list[str]] = {node_id: [] for node_id in self._nodes}
        reverse_adjacency: dict[str, list[str]] = {node_id: [] for node_id in self._nodes}
        for from_id, to_id, _ in self._edges:
            adjacency.setdefault(from_id, []).append(to_id)
            reverse_adjacency.setdefault(to_id, []).append(from_id)

        for source_id in conditional_sources:
            node = self._nodes.get(source_id)
            if node is None or node.get("type") != "condition":
                raise PipelineError(
                    f"Conditional edge source {source_id!r} must be a condition node."
                )
            incoming = sum(1 for _, to_id, _ in self._edges if to_id == source_id)
            if incoming != 1:
                raise PipelineError(
                    f"Condition node {source_id!r} must have exactly one input; got {incoming}."
                )
            expression = node.get("config", {}).get("expression", "")
            if not isinstance(expression, str) or not expression.strip():
                raise PipelineError(
                    f"Condition node {source_id!r} requires a non-empty expression."
                )

            ancestors = list(reverse_adjacency.get(source_id, ()))
            visited_ancestors: set[str] = set()
            while ancestors:
                node_id = ancestors.pop()
                if node_id in visited_ancestors:
                    continue
                visited_ancestors.add(node_id)
                if self._nodes.get(node_id, {}).get("type") == "condition":
                    raise PipelineError(
                        "Nested conditional routing is not supported; condition "
                        f"node {source_id!r} is downstream of {node_id!r}."
                    )
                ancestors.extend(reverse_adjacency.get(node_id, ()))

            for from_id, to_id, condition in self._edges:
                if from_id == source_id and condition is None:
                    raise PipelineError(
                        f"Condition node edge {source_id!r} -> {to_id!r} "
                        "must use .when() or .otherwise()."
                    )

            stack = list(adjacency.get(source_id, ()))
            visited: set[str] = set()
            while stack:
                node_id = stack.pop()
                if node_id in visited:
                    continue
                visited.add(node_id)
                if self._nodes.get(node_id, {}).get("type") == "condition":
                    raise PipelineError(
                        "Nested conditional routing is not supported; condition "
                        f"node {node_id!r} is downstream of {source_id!r}."
                    )
                stack.extend(adjacency.get(node_id, ()))

    @staticmethod
    def _annotate_schema_hint(
        node: dict[str, Any],
        node_data: dict[str, Any],
    ) -> None:
        """Add ``_schema_hint`` to source nodes for downstream inference."""
        if "source" not in node.get("capabilities", ()):
            return
        if node["type"] == "source_db" and "query" in node["config"]:
            node_data["config"]["_schema_hint"] = "query_result"
            return
        if node["type"] == "source_api":
            node_data["config"]["_schema_hint"] = "api_response"

    def _build_hooks(self) -> dict[str, dict[str, Any]]:
        # Hooks are pre-normalized by _coerce_hook at construction; emit
        # them as-is. (They used to be replaced by an empty webhook
        # placeholder here, discarding whatever the user passed.)
        hooks: dict[str, dict[str, Any]] = {}
        for hook_name in _HOOK_NAMES:
            value = getattr(self, hook_name, None)
            if value is not None:
                hooks[hook_name] = dict(value)
        return hooks

    def _auto_layout(self) -> dict[str, dict[str, int]]:
        """Compute positions via topological sort (left-to-right layers)."""
        if not self._nodes:
            return {}

        # Build adjacency and in-degree maps.
        in_degree: dict[str, int] = {nid: 0 for nid in self._nodes}
        adj: dict[str, list[str]] = {nid: [] for nid in self._nodes}
        for from_id, to_id, _ in self._edges:
            if from_id not in adj or to_id not in in_degree:
                continue
            adj[from_id].append(to_id)
            in_degree[to_id] += 1

        # Kahn's algorithm -> layers.
        layers: list[list[str]] = []
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        visited: set[str] = set()

        while queue:
            layers.append(list(queue))
            visited.update(queue)
            next_queue: list[str] = []
            for nid in queue:
                for dep in adj[nid]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0 and dep not in visited:
                        next_queue.append(dep)
            queue = next_queue

        # Assign positions: x by layer, y centered vertically.
        positions: dict[str, dict[str, int]] = {}
        for layer_idx, layer in enumerate(layers):
            x = LAYOUT_X_ORIGIN + layer_idx * LAYOUT_X_GAP
            total_height = (len(layer) - 1) * LAYOUT_Y_GAP
            start_y = max(LAYOUT_Y_MIN, LAYOUT_Y_CENTER - total_height // 2)
            for i, nid in enumerate(layer):
                positions[nid] = {"x": x, "y": start_y + i * LAYOUT_Y_GAP}

        # Place any disconnected nodes that Kahn's didn't visit.
        for nid in self._nodes:
            if nid not in positions:
                positions[nid] = {
                    "x": LAYOUT_X_ORIGIN,
                    "y": LAYOUT_Y_MIN + len(positions) * LAYOUT_Y_GAP,
                }

        return positions
