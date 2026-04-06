"""Pipeline definition and DAG builder."""

from __future__ import annotations

import inspect
import re
import secrets
import textwrap
from typing import Any, Callable, Optional

from brokoli.exceptions import PipelineError

# --- Layout constants ---
LAYOUT_X_GAP: int = 280
LAYOUT_Y_GAP: int = 100
LAYOUT_X_ORIGIN: int = 50
LAYOUT_Y_CENTER: int = 200
LAYOUT_Y_MIN: int = 50

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
        if time.time() - _start > _timeout:
            raise TimeoutError("Sensor '{func_name}' timed out after {{_timeout}}s")
        time.sleep(_poll_interval)
    output_data = {{"columns": columns, "rows": rows}}
''')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(name: str) -> str:
    """Generate a short unique ID from a name: slugified prefix + random hex."""
    clean = name.lower().replace(" ", "_")
    clean = "".join(c for c in clean if c.isalnum() or c == "_")
    return clean[:20] + "_" + secrets.token_hex(3)


def _extract_func_source(func: Callable) -> str:
    """Return the dedented source of *func*, stripping any decorator lines."""
    source = inspect.getsource(func)
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
            refs = [self._resolve(item) for item in other]
            for ref in refs:
                self.pipeline._add_edge(self.node_id, ref.node_id)
            return _MultiRef(refs, self.pipeline)

        ref = self._resolve(other)
        self.pipeline._add_edge(self.node_id, ref.node_id)
        return ref

    def __or__(self, other: Any) -> "_MultiRef":
        """``a | b`` -- group for parallel fan-out (no edge added)."""
        ref = self._resolve(other)
        return _MultiRef([self, ref], self.pipeline)

    def _resolve(self, item: Any) -> "NodeRef":
        """Convert a node-like object to a :class:`NodeRef`."""
        if isinstance(item, NodeRef):
            return item
        if isinstance(item, _MultiRef):
            return item.refs[0]
        if isinstance(item, _ConditionBranch):
            return item._ref
        # Auto-call wrappers that haven't been invoked yet
        _auto_call_types = (
            _TaskWrapper, _SourceWrapper, _SinkWrapper,
            _FilterWrapper, _MapWrapper, _ValidateWrapper, _SensorWrapper,
        )
        if isinstance(item, _auto_call_types):
            return item()
        raise TypeError(f"Cannot connect to {type(item).__name__}")


class _MultiRef:
    """Multiple node refs -- result of ``[a, b]`` or ``a >> [b, c]``."""

    def __init__(self, refs: list[NodeRef], pipeline: "Pipeline") -> None:
        self.refs = refs
        self.pipeline = pipeline

    def __rshift__(self, other: Any) -> "NodeRef | _MultiRef":
        """``[a, b] >> c`` -- fan-in: all connect to target."""
        if isinstance(other, list):
            pairs = list(zip(self.refs, other))
            for ref, item in pairs:
                target = ref._resolve(item)
                self.pipeline._add_edge(ref.node_id, target.node_id)
            return _MultiRef(
                [ref._resolve(item) for ref, item in pairs],
                self.pipeline,
            )

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

class _TaskWrapper:
    """Wraps a ``@task`` decorated function.  Calling it registers the node."""

    def __init__(
        self,
        func: Callable,
        name: str,
        pipeline: "Pipeline",
        config: dict[str, Any],
    ) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        """Register this task as a node with edges from *inputs*."""
        func_source = _extract_func_source(self._func)

        call_script = TASK_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
        )

        config: dict[str, Any] = {
            "language": "python",
            "script": call_script,
            **self._config,
        }

        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)

        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)

        return NodeRef(node_id, self._pipeline)

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        """``extract >> task_func`` -- auto-call with no explicit input."""
        return self() >> other

    def __rrshift__(self, other: Any) -> NodeRef:
        """``source >> task_func``."""
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _ConditionWrapper:
    """Wraps a ``@condition`` decorated function."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline") -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self.__wrapped__ = func

    def __call__(self, input_ref: NodeRef) -> _ConditionContext:
        func_source = _extract_func_source(self._func)

        cond_script = CONDITION_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
        )

        # --- Build a three-node sub-graph for the condition ---
        #
        # 1. eval node   -- runs the user's Python predicate, outputs rows
        #                    (empty list = false, non-empty = true).
        # 2. condition   -- a lightweight "row_count > 0" gate that splits
        #                    the DAG into true / false branches.
        # 3. branch refs -- virtual true/false handles so downstream ``>>``
        #                    knows which side of the gate to attach to.
        #
        # Edges: input -> eval -> condition
        #         input -------> condition  (condition also sees raw rows)

        eval_id = _make_id(self._name + "_eval")
        self._pipeline._add_node(eval_id, "code", self._name + " (eval)", {
            "language": "python",
            "script": cond_script,
        })

        cond_id = _make_id(self._name)
        self._pipeline._add_node(cond_id, "condition", self._name, {
            "expression": "row_count > 0",
        })

        # Wire edges from input -> eval -> condition, and input -> condition.
        if isinstance(input_ref, NodeRef):
            self._pipeline._add_edge(input_ref.node_id, eval_id)
            self._pipeline._add_edge(input_ref.node_id, cond_id)
        self._pipeline._add_edge(eval_id, cond_id)

        # Register branch metadata so deploy can generate proper routing.
        self._pipeline._branches[cond_id] = {"true": [], "false": []}
        true_ref = _BranchRef(cond_id, "true", self._pipeline)
        false_ref = _BranchRef(cond_id, "false", self._pipeline)

        return _ConditionContext(true_ref, false_ref, self._pipeline)


class _BranchRef(NodeRef):
    """A :class:`NodeRef` that tracks which branch (true/false) it represents."""

    def __init__(self, cond_id: str, branch: str, pipeline: "Pipeline") -> None:
        super().__init__(cond_id, pipeline)
        self._branch = branch

    def __repr__(self) -> str:
        return f"_BranchRef({self.node_id!r}, branch={self._branch!r})"

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        if isinstance(other, list):
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

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline", config: dict[str, Any]) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self) -> NodeRef:
        func_source = _extract_func_source(self._func)
        script = SOURCE_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script, **self._config}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        return NodeRef(node_id, self._pipeline)

    def __rshift__(self, other: Any) -> NodeRef | _MultiRef:
        return self() >> other


class _SinkWrapper:
    """Wraps a ``@sink`` decorated function. Registers as a sink code node."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline", config: dict[str, Any]) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._config = config
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        func_source = _extract_func_source(self._func)
        script = SINK_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script, **self._config}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        return NodeRef(node_id, self._pipeline)

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _FilterWrapper:
    """Wraps a ``@filter`` decorated function. Registers as a code node that filters rows."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline") -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        func_source = _extract_func_source(self._func)
        script = FILTER_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        return NodeRef(node_id, self._pipeline)

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _MapWrapper:
    """Wraps a ``@map`` decorated function. Registers as a code node that transforms each row."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline") -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        func_source = _extract_func_source(self._func)
        script = MAP_WRAPPER_TEMPLATE.format(func_source=func_source, func_name=self._func.__name__)
        config: dict[str, Any] = {"language": "python", "script": script}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        return NodeRef(node_id, self._pipeline)

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _ValidateWrapper:
    """Wraps a ``@validate`` decorated function. Registers as a quality-gate code node."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline", on_failure: str) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._on_failure = on_failure
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        func_source = _extract_func_source(self._func)
        action = 'raise ValueError(f"Validation failed: {_message}")' if self._on_failure == "block" else ""
        script = VALIDATE_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
            on_failure_action=action,
        )
        config: dict[str, Any] = {"language": "python", "script": script}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        return NodeRef(node_id, self._pipeline)

    def __rrshift__(self, other: Any) -> NodeRef:
        if isinstance(other, NodeRef):
            return self(other)
        return NotImplemented  # type: ignore[return-value]


class _SensorWrapper:
    """Wraps a ``@sensor`` decorated function. Polls until the function returns True."""

    def __init__(self, func: Callable, name: str, pipeline: "Pipeline", poll_interval: int, timeout: int) -> None:
        self._func = func
        self._name = name
        self._pipeline = pipeline
        self._poll_interval = poll_interval
        self._timeout = timeout
        self.__wrapped__ = func
        self.__name__ = func.__name__

    def __call__(self, *inputs: NodeRef) -> NodeRef:
        func_source = _extract_func_source(self._func)
        script = SENSOR_WRAPPER_TEMPLATE.format(
            func_source=func_source,
            func_name=self._func.__name__,
            poll_interval=self._poll_interval,
            timeout=self._timeout,
        )
        config: dict[str, Any] = {"language": "python", "script": script, "timeout": self._timeout + 60}
        node_id = _make_id(self._name)
        self._pipeline._add_node(node_id, "code", self._name, config)
        for inp in inputs:
            if isinstance(inp, NodeRef):
                self._pipeline._add_edge(inp.node_id, node_id)
        return NodeRef(node_id, self._pipeline)

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

    def __init__(
        self,
        name: str,
        pipeline_id: str | None = None,
        description: str = "",
        schedule: str = "",
        catch_up: bool = False,
        sla: str = "",
        depends_on: list[str] | None = None,
        tags: list[str] | None = None,
        webhook: bool = False,
        max_retries: int = 0,
        concurrency: int = 4,
        on_start: Callable | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
    ) -> None:
        self.name = name
        self.pipeline_id = pipeline_id or self._generate_pipeline_id(name)
        self.description = description
        self.schedule = schedule
        self.catch_up = catch_up
        self.tags: list[str] = tags or []
        self.webhook = webhook
        self.max_retries = max_retries
        self.concurrency = concurrency
        self.on_start = on_start
        self.on_success = on_success
        self.on_failure = on_failure
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
        self._edges: list[tuple[str, str]] = []
        self._branches: dict[str, dict[str, list[str]]] = {}
        self._node_order: list[str] = []

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

    def _add_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        config: dict[str, Any],
    ) -> None:
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
        }
        self._node_order.append(node_id)

    def _add_edge(self, from_id: str, to_id: str) -> None:
        edge = (from_id, to_id)
        if edge not in self._edges:
            self._edges.append(edge)

    # -- Serialization -----------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        """Convert pipeline to Brokoli API JSON format with auto-layout."""
        positions = self._auto_layout()

        nodes: list[dict[str, Any]] = []
        for nid in self._node_order:
            node = self._nodes[nid]
            node_data: dict[str, Any] = {
                "id": node["id"],
                "type": node["type"],
                "name": node["name"],
                "config": node["config"],
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
            "nodes": nodes,
            "edges": [{"from": f, "to": t} for f, t in self._edges],
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

    # -- Private helpers ---------------------------------------------------

    @staticmethod
    def _annotate_schema_hint(
        node: dict[str, Any],
        node_data: dict[str, Any],
    ) -> None:
        """Add ``_schema_hint`` to source nodes for downstream inference."""
        if node["type"] == "source_db" and "query" in node["config"]:
            node_data["config"]["_schema_hint"] = "query_result"
            return
        if node["type"] == "source_api":
            node_data["config"]["_schema_hint"] = "api_response"

    def _build_hooks(self) -> dict[str, dict[str, Any]]:
        hooks: dict[str, dict[str, Any]] = {}
        for hook_name in _HOOK_NAMES:
            if getattr(self, hook_name, None) is not None:
                hooks[hook_name] = {"type": "webhook", "url": "", "enabled": True}
        return hooks

    def _auto_layout(self) -> dict[str, dict[str, int]]:
        """Compute positions via topological sort (left-to-right layers)."""
        if not self._nodes:
            return {}

        # Build adjacency and in-degree maps.
        in_degree: dict[str, int] = {nid: 0 for nid in self._nodes}
        adj: dict[str, list[str]] = {nid: [] for nid in self._nodes}
        for from_id, to_id in self._edges:
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
