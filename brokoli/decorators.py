"""Decorators for defining pipeline nodes from Python functions.

Available decorators::

    @task       -- general-purpose data processing
    @condition  -- boolean branching (true/false split)
    @source     -- custom data fetcher (no input, returns rows)
    @sink       -- custom data writer (takes rows, pass-through)
    @filter     -- row-level predicate (keep rows where func returns True)
    @map        -- row-level transform (apply func to each row)
    @validate   -- custom quality check (returns bool + message)
    @sensor     -- poll until ready (returns True to proceed)
"""

from __future__ import annotations

from typing import Callable, Optional

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


# ---------------------------------------------------------------------------
# @task
# ---------------------------------------------------------------------------

def task(
    name_or_func: str | Callable | None = None,
    *,
    name: str = "",
    retries: int = 0,
    retry_backoff: str = "exponential",
    timeout: int = 0,
    on_success: Optional[Callable] = None,
    on_failure: Optional[str | Callable] = None,
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
    """
    config: dict = {}
    if retries > 0:
        config["max_retries"] = retries
        config["retry_backoff"] = retry_backoff
    if timeout > 0:
        config["timeout"] = timeout
    if on_failure == "skip":
        config["on_failure"] = "skip"

    def decorator(func: Callable) -> _TaskWrapper:
        task_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@task")
        return _TaskWrapper(func, task_name, pipeline, config)

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
) -> _ConditionWrapper | Callable:
    """Wrap a function returning ``bool`` as a condition node for DAG branching.

    Use with a ``with`` statement to get true/false branch handles.

    Example::

        @condition
        def has_enough_data(rows):
            return len(rows) >= 10

        with has_enough_data(source_node) as (ok, fail):
            ok >> process >> sink
            fail >> alert
    """
    def decorator(func: Callable) -> _ConditionWrapper:
        cond_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@condition")
        return _ConditionWrapper(func, cond_name, pipeline)

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
    retries: int = 0,
    timeout: int = 0,
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
    if retries > 0:
        config["max_retries"] = retries
    if timeout > 0:
        config["timeout"] = timeout

    def decorator(func: Callable) -> _SourceWrapper:
        src_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@source")
        return _SourceWrapper(func, src_name, pipeline, config)

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
    retries: int = 0,
    timeout: int = 0,
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
    if retries > 0:
        config["max_retries"] = retries
    if timeout > 0:
        config["timeout"] = timeout

    def decorator(func: Callable) -> _SinkWrapper:
        sink_name = _resolve_name(name_or_func, name, func)
        pipeline = _require_pipeline("@sink")
        return _SinkWrapper(func, sink_name, pipeline, config)

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
        return _FilterWrapper(func, filt_name, pipeline)

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
        return _MapWrapper(func, map_name, pipeline)

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
        return _ValidateWrapper(func, val_name, pipeline, on_failure)

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
    timeout: int = 3600,
) -> _SensorWrapper | Callable:
    """Wrap a function as a sensor that polls until ready.

    The function takes no arguments and returns ``True`` when the condition is met.
    The node will poll at ``poll_interval`` seconds and fail after ``timeout`` seconds.

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
        return _SensorWrapper(func, sensor_name, pipeline, poll_interval, timeout)

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
    pipeline = Pipeline._current
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
