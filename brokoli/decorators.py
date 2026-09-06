"""Decorators for defining pipeline nodes from Python functions.

Available decorators::

    @task       -- general-purpose data processing
    @condition  -- reserved predicate API; invocation currently fails closed
    @source     -- custom data fetcher (no input, returns rows)
    @sink       -- custom data writer (takes rows, pass-through)
    @filter     -- row-level predicate (keep rows where func returns True)
    @map        -- row-level transform (apply func to each row)
    @validate   -- custom quality check (returns bool + message)
    @sensor     -- poll until ready (returns True to proceed)
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from brokoli.pipeline import (
    Pipeline,
    _ConditionWrapper,
    _FilterWrapper,
    _MapWrapper,
    _SensorWrapper,
    _SinkWrapper,
    _SourceWrapper,
    _TaskWrapper,
    _ValidateWrapper,
)
from brokoli.sentinel import UNSET


# ---------------------------------------------------------------------------
# @task
# ---------------------------------------------------------------------------


def task(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    retries: Any = UNSET,
    retry_backoff: str = "exponential",
    timeout: Any = UNSET,
    on_success: Optional[Callable] = None,
    on_failure: Optional[str | Callable] = None,
    package: str = "auto",
    node_key: Optional[str] = None,
    interface: Optional[dict] = None,
) -> _TaskWrapper | Callable:
    """Wrap a Python function as a code node for general data processing.

    The decorated function receives ``rows`` (list of dicts) and should return
    a list of dicts, a :class:`TaskResult`, or a pandas DataFrame.

    Example::

        @task
        def clean(rows):
            return [r for r in rows if r.get("amount", 0) > 0]

        @task("Enrich Data", retries=3, timeout=120)
        def enrich(rows):
            for r in rows:
                r["domain"] = r["email"].split("@")[1]
            return rows

    By default (``package="auto"``), a task's deployed source is more than
    just the isolated function body: any module-level constant or
    same-module helper function it references is detected (by inspecting
    the function's bytecode) and automatically included, and any module
    import it needs is re-emitted too::

        API_BASE = "https://api.example.com"   # module-level constant

        def _normalize(row, base):             # module-level helper
            row["base"] = base
            return row

        @task
        def clean(rows):
            return [_normalize(r, API_BASE) for r in rows]

    ``clean``'s deployed package includes ``API_BASE``, ``_normalize``, and
    ``clean`` itself -- so it doesn't fail remotely with a ``NameError`` the
    way isolated-function extraction used to. If a task references
    something that can't be safely auto-included this way (an imported
    class instance, a bound method, an arbitrary object that isn't
    JSON-serializable, ...), pipeline construction raises a
    :class:`~brokoli.exceptions.PipelineError` *locally*, naming exactly
    what's missing, rather than deploying something that would only fail
    once it runs remotely.

    Pass ``package="module"`` to skip auto-detection and deploy the task's
    *entire* containing module verbatim instead -- broader/heavier (the
    whole file ships, including unrelated top-level code) but an escape
    hatch for cases auto-detection can't handle, e.g. a task that
    legitimately needs a whole helper module of dependencies::

        @task(package="module")
        def clean(rows):
            return heavy_helpers.transform(rows)

    Pass ``package="bundle"`` (ADR-031) to package the task's *project* —
    its containing module, its relative imports, and its same-repo helper
    modules — into a versioned, content-addressed task bundle uploaded to
    the server before deployment. The node config then carries a
    ``task_bundle`` reference instead of a ``script``, and the engine
    mounts the bundle and runs it in its own import namespace. This is the
    mode for tasks that live in a real project tree (where module-verbatim
    or function-inlining can't reach the files they need). v1 scope is
    project files only: a task whose module imports a third-party package
    fails packaging with a clear, named error rather than deploying
    something that would fail at run time. Requires a server that
    advertises the ``task-bundles`` execution feature.

    ADR-032 (rollout step 3): by default, ``@task`` infers a portable
    node interface from ``rows``' and the return value's type hints
    (``TypedDict``/dataclass rows compile to a BPTD record; anything
    unannotated or unrecognized stays honestly unknown, never a guessed
    shape), and promotes every other annotated keyword parameter to a
    typed **pipeline** parameter -- a keyword with a default becomes
    optional with that default; one without becomes required. Pass
    ``interface={...}`` to skip inference and declare the node's
    interface directly (parameter inference still runs independently).
    Requires a server that advertises the ``task-interface-v1`` execution
    feature; older servers simply never see the field.
    """
    config: dict = {}
    if retries is not UNSET and retries is not None:
        config["max_retries"] = retries
        config["retry_backoff"] = retry_backoff
    if timeout is not UNSET and timeout is not None:
        config["timeout"] = timeout
    if on_failure == "skip":
        config["on_failure"] = "skip"

    def decorator(func: Callable) -> _TaskWrapper:
        task_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@task")
        return _TaskWrapper(
            func,
            task_name,
            pipeline,
            config,
            package=package,
            node_key=node_key,
            interface=interface,
        )

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @condition
# ---------------------------------------------------------------------------


def condition(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    node_key: Optional[str] = None,
) -> _ConditionWrapper | Callable:
    """Declare a Python condition predicate.

    Invoking the wrapper currently raises :class:`PipelineError` because the
    runtime IR cannot distinguish predicate input from the unchanged branch
    payload. Use ``condition_node(...).when()/.otherwise()`` instead.
    """

    def decorator(func: Callable) -> _ConditionWrapper:
        cond_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@condition")
        return _ConditionWrapper(func, cond_name, pipeline, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @source
# ---------------------------------------------------------------------------


def source(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    retries: Any = UNSET,
    timeout: Any = UNSET,
    node_key: Optional[str] = None,
) -> _SourceWrapper | Callable:
    """Wrap a function as a custom data source (no input, returns rows).

    The function takes no arguments and returns a list of dicts or a DataFrame.

    Example::

        @source
        def fetch_stripe_charges():
            import stripe
            return [c.to_dict() for c in stripe.Charge.list(limit=100)]

        @source("GitHub Events", timeout=60)
        def fetch_events():
            import requests
            return requests.get("https://api.github.com/events").json()
    """
    config: dict = {}
    if retries is not UNSET and retries is not None:
        config["max_retries"] = retries
    if timeout is not UNSET and timeout is not None:
        config["timeout"] = timeout

    def decorator(func: Callable) -> _SourceWrapper:
        src_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@source")
        return _SourceWrapper(func, src_name, pipeline, config, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @sink
# ---------------------------------------------------------------------------


def sink(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    retries: Any = UNSET,
    timeout: Any = UNSET,
    node_key: Optional[str] = None,
) -> _SinkWrapper | Callable:
    """Wrap a function as a custom data sink (takes rows, writes somewhere).

    The function receives ``rows`` (list of dicts). Return value is ignored.
    Data passes through unchanged to downstream nodes.

    Example::

        @sink
        def push_to_hubspot(rows):
            import hubspot
            client = hubspot.Client.create(access_token="...")
            for row in rows:
                client.crm.contacts.basic_api.create(properties=row)

        @sink("S3 Upload")
        def upload_to_s3(rows):
            import boto3, json
            s3 = boto3.client("s3")
            s3.put_object(Bucket="lake", Key="data.json", Body=json.dumps(rows))
    """
    config: dict = {}
    if retries is not UNSET and retries is not None:
        config["max_retries"] = retries
    if timeout is not UNSET and timeout is not None:
        config["timeout"] = timeout

    def decorator(func: Callable) -> _SinkWrapper:
        sink_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@sink")
        return _SinkWrapper(func, sink_name, pipeline, config, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @filter
# ---------------------------------------------------------------------------


def filter(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    node_key: Optional[str] = None,
) -> _FilterWrapper | Callable:
    """Wrap a row-level predicate as a filter node.

    The function receives a single row (dict) and returns ``True`` to keep it.

    Example::

        @filter
        def active_users(row):
            return row["status"] == "active" and row["last_login_days"] < 90

        source >> active_users >> sink
    """

    def decorator(func: Callable) -> _FilterWrapper:
        filt_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@filter")
        return _FilterWrapper(func, filt_name, pipeline, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @map
# ---------------------------------------------------------------------------


def map(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    node_key: Optional[str] = None,
) -> _MapWrapper | Callable:
    """Wrap a row-level transform as a map node.

    The function receives a single row (dict) and returns the transformed row.

    Example::

        @map
        def enrich(row):
            row["full_name"] = f"{row['first']} {row['last']}"
            row["domain"] = row["email"].split("@")[1]
            return row

        source >> enrich >> sink
    """

    def decorator(func: Callable) -> _MapWrapper:
        map_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@map")
        return _MapWrapper(func, map_name, pipeline, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @validate
# ---------------------------------------------------------------------------


def validate(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    on_failure: str = "block",
    node_key: Optional[str] = None,
) -> _ValidateWrapper | Callable:
    """Wrap a function as a custom quality check.

    The function receives ``rows`` and returns either:
    - ``bool`` -- True = passed, False = failed
    - ``(bool, str)`` -- (passed, message)

    Args:
        on_failure: ``"block"`` raises an error; ``"warn"`` logs and continues.

    Example::

        @validate("Revenue sanity check")
        def revenue_positive(rows):
            total = sum(r.get("amount", 0) for r in rows)
            return total > 0, f"Total revenue: {total}"

        @validate(on_failure="warn")
        def row_count_check(rows):
            return len(rows) > 100, f"Only {len(rows)} rows"
    """

    def decorator(func: Callable) -> _ValidateWrapper:
        val_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@validate")
        return _ValidateWrapper(func, val_name, pipeline, on_failure, node_key=node_key)

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# @sensor
# ---------------------------------------------------------------------------


def sensor(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    poll_interval: int = 60,
    timeout: Any = 3600,
    node_key: Optional[str] = None,
) -> _SensorWrapper | Callable:
    """Wrap a function as a sensor that polls until ready.

    The function takes no arguments and returns ``True`` when the condition is met.
    The node will poll at ``poll_interval`` seconds and fail after ``timeout`` seconds.
    Pass ``timeout=None`` for a sensor that polls indefinitely (no timeout).

    Example::

        @sensor(poll_interval=30, timeout=1800)
        def wait_for_export():
            import os
            return os.path.exists("/data/daily_export.csv")

        @sensor("Wait for API", poll_interval=10, timeout=300)
        def api_ready():
            import urllib.request
            try:
                urllib.request.urlopen("https://api.example.com/health", timeout=5)
                return True
            except Exception:
                return False

        wait_for_export >> process >> sink
    """

    def decorator(func: Callable) -> _SensorWrapper:
        sensor_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@sensor")
        return _SensorWrapper(
            func, sensor_name, pipeline, poll_interval, timeout, node_key=node_key
        )

    if callable(name_or_func):
        func = name_or_func
        name_or_func = None
        return decorator(func)
    return decorator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_pipeline(decorator_name: str) -> Pipeline:
    """Return the current pipeline or raise a clear error."""
    pipeline = Pipeline.current()
    if pipeline is None:
        raise RuntimeError(f"{decorator_name} must be used inside a `with Pipeline(...):` block")
    return pipeline


def _resolve_name(
    name_or_func: str | Callable | None,
    explicit_name: str,
    func: Callable,
) -> str:
    """Derive a human-readable node name from decorator arguments."""
    if explicit_name:
        return explicit_name
    if isinstance(name_or_func, str) and name_or_func:
        return name_or_func
    return func.__name__.replace("_", " ").title()
