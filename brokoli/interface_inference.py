"""ADR-032 rollout step 3: infer a BPTD node interface and pipeline
parameters from a ``@task``-decorated function's type hints and defaults.

SDK inference is an adapter, not authority (ADR-032 section 8): this
module emits the LEAST contract it can prove. No annotation means
unknown, never a guessed shape (section 8 rule 1); an unresolvable
forward reference or unsupported native type falls back to unknown with
a visible warning rather than raising (section 8 rules 2/7).

Deliberately scoped per the ADR-032 rollout step-3 decision recorded in
docs/adr/032-portable-task-interfaces-and-data-contracts.md: row-schema
inference targets only the implicit single ``input``/``result`` ports
(section 6's default -- this never touches ``task-ports-v1`` or named
multi-port edges), and an inferred keyword parameter becomes a
PIPELINE-level parameter (the mechanism ADR-032 rollout step 4 already
wired end to end), never a task-interface-level ``parameters`` block --
section 7's ``parameter_bindings`` has no execution consumer anywhere in
the core engine yet.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import inspect
import typing
import warnings
from typing import Any, Callable, Optional


class InferredInterface(typing.NamedTuple):
    """Result of inferring a task's row schema and parameters.

    ``node_interface`` is ``None`` when neither the row parameter nor the
    return annotation yielded anything provable -- an unannotated
    ``@task`` never gets a vacuous ``{"row": {"kind": "unknown"}}``
    interface on both sides; the base SDK stays boilerplate-free (ADR-032
    section 13) for the common case.
    """

    node_interface: Optional[dict]
    parameters: dict


_SCALAR_KINDS = {
    int: "int64",
    float: "float64",
    str: "string",
    bool: "boolean",
    bytes: "bytes",
    decimal.Decimal: "decimal",
    datetime.datetime: "timestamp",
    datetime.date: "date",
    datetime.timedelta: "duration",
}


def _warn_unmappable(context: str, annotation: Any) -> None:
    warnings.warn(
        f"brokoli: could not infer a portable type for {context} "
        f"(annotation={annotation!r}); leaving it unknown. "
        "Pass an explicit interface=... to @task to describe it.",
        stacklevel=3,
    )


def _is_typeddict(cls: Any) -> bool:
    # No typing_extensions dependency, and typing.is_typeddict() is 3.10+
    # (this SDK supports 3.9+) -- __total__ is a reliable marker the
    # TypedDict metaclass machinery has set since its introduction.
    return isinstance(cls, type) and hasattr(cls, "__annotations__") and hasattr(cls, "__total__")


def _map_type(annotation: Any, context: str) -> Optional[dict]:
    """Map one Python type annotation to a BPTD type descriptor, or
    ``None`` if it cannot be honestly proven (section 8 rule 2)."""
    if annotation is inspect.Signature.empty or annotation is None or annotation is type(None):
        return None

    # These are a deliberate "I'm not describing this" signal, not a
    # failed attempt at a richer type -- the extremely common
    # `rows: list[dict]` shape must stay silent (ADR-032 section 13: the
    # base SDK doesn't turn every small pipeline into schema boilerplate)
    # rather than warn on every ordinary untyped task.
    if annotation in (dict, list, object) or annotation is Any:
        return None

    origin = typing.get_origin(annotation)

    if origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        nullable = type(None) in typing.get_args(annotation)
        if len(args) != 1:
            # A real (non-Optional) Union has no BPTD mapping in v1 --
            # discriminated unions need per-variant tags this inference
            # layer cannot invent from a bare typing.Union.
            _warn_unmappable(context, annotation)
            return None
        mapped = _map_type(args[0], context)
        if mapped is None:
            return None
        if nullable:
            mapped = dict(mapped, nullable=True)
        return mapped

    if origin is typing.Literal:
        values = typing.get_args(annotation)
        if values and all(isinstance(v, str) for v in values):
            return {"kind": "enum", "values": list(values)}
        _warn_unmappable(context, annotation)
        return None

    if origin in (list, typing.List):
        (item_annotation,) = typing.get_args(annotation) or (inspect.Signature.empty,)
        item_type = _map_type(item_annotation, f"{context} item")
        if item_type is None:
            _warn_unmappable(context, annotation)
            return None
        return {"kind": "array", "items": item_type}

    if origin in (dict, typing.Dict):
        dict_args = typing.get_args(annotation)
        if len(dict_args) == 2 and dict_args[0] is str:
            value_type = _map_type(dict_args[1], f"{context} value")
            if value_type is not None:
                return {"kind": "map", "keys": "string", "values": value_type}
        _warn_unmappable(context, annotation)
        return None

    if annotation in _SCALAR_KINDS:
        return {"kind": _SCALAR_KINDS[annotation]}

    if dataclasses.is_dataclass(annotation):
        return _map_record(annotation, context)

    if _is_typeddict(annotation):
        return _map_record(annotation, context)

    _warn_unmappable(context, annotation)
    return None


def _map_record(cls: Any, context: str) -> Optional[dict]:
    """Map a TypedDict or dataclass to a BPTD record, or ``None`` if any
    field is unmappable -- a partially-honest closed record (missing a
    real field while claiming ``additional_fields: false``) is worse than
    admitting the whole row is unknown (ADR-032 section 8 rule 2)."""
    fields: list[dict] = []

    if dataclasses.is_dataclass(cls):
        try:
            hints = typing.get_type_hints(cls)
        except NameError:
            _warn_unmappable(context, cls)
            return None
        for f in dataclasses.fields(cls):
            field_type = _map_type(hints.get(f.name, f.type), f"{context}.{f.name}")
            if field_type is None:
                return None
            required = f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING  # type: ignore[misc]
            fields.append({"name": f.name, "type": field_type, "required": required})
    else:
        try:
            hints = typing.get_type_hints(cls)
        except NameError:
            _warn_unmappable(context, cls)
            return None
        required_keys = getattr(cls, "__required_keys__", frozenset(hints))
        for field_name, field_annotation in hints.items():
            field_type = _map_type(field_annotation, f"{context}.{field_name}")
            if field_type is None:
                return None
            fields.append(
                {
                    "name": field_name,
                    "type": field_type,
                    "required": field_name in required_keys,
                }
            )

    return {"kind": "record", "fields": fields, "additional_fields": False}


def _dataset_port(annotation: Any, context: str) -> Optional[dict]:
    """Map a row-collection annotation (``list[Row]``) to a dataset value
    contract, or ``None`` if the row type itself isn't provable."""
    origin = typing.get_origin(annotation)
    if origin not in (list, typing.List):
        return None
    (row_annotation,) = typing.get_args(annotation) or (inspect.Signature.empty,)
    row_type = _map_type(row_annotation, context)
    if row_type is None or row_type.get("kind") != "record":
        return None
    return {"kind": "dataset", "row": row_type}


def infer_task_interface(func: Callable) -> InferredInterface:
    """Infer a node interface and pipeline parameters from *func*'s
    signature -- the conventional ``@task`` shape ``def f(rows, **kwargs)
    -> ...``. Resolves annotations via ``typing.get_type_hints`` without
    importing anything beyond what *func*'s own module already imported
    (ADR-032 section 8 rule 7); an unresolvable forward reference is
    treated the same as no annotation, not an error.
    """
    try:
        hints = typing.get_type_hints(func)
    except NameError:
        warnings.warn(
            f"brokoli: could not resolve type hints for {func.__qualname__} "
            "(unresolvable forward reference); inferring nothing.",
            stacklevel=2,
        )
        hints = {}

    sig = inspect.signature(func)
    params = [
        p
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ]

    input_port = None
    if params:
        rows_annotation = hints.get(params[0].name, inspect.Signature.empty)
        input_port = _dataset_port(rows_annotation, f"{func.__qualname__}({params[0].name})")

    output_port = None
    return_annotation = hints.get("return", inspect.Signature.empty)
    output_port = _dataset_port(return_annotation, f"{func.__qualname__} return")

    node_interface = None
    if input_port is not None or output_port is not None:
        node_interface = {
            "contract": "brokoli.task-interface/v1",
            "inputs": {
                "input": {
                    "value": input_port or {"kind": "dataset", "row": {"kind": "unknown"}},
                }
            },
            "outputs": {
                "result": {
                    "value": output_port or {"kind": "dataset", "row": {"kind": "unknown"}},
                }
            },
        }

    parameters: dict = {}
    for param in params[1:]:
        annotation = hints.get(param.name, inspect.Signature.empty)
        if annotation is inspect.Signature.empty:
            continue
        param_type = _map_type(annotation, f"{func.__qualname__}({param.name})")
        if param_type is None:
            continue
        if param.default is inspect.Parameter.empty:
            parameters[param.name] = {"type": param_type, "required": True}
        else:
            parameters[param.name] = {
                "type": param_type,
                "required": False,
                "default": param.default,
            }

    return InferredInterface(node_interface=node_interface, parameters=parameters)
