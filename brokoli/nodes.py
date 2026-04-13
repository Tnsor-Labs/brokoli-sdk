"""Built-in node types -- source, transform, sink, flow control."""

from __future__ import annotations

from typing import Optional

from brokoli.exceptions import ContextError
from brokoli.parsing import ParseError, parse_quality_rule
from brokoli.pipeline import Pipeline, NodeRef, _make_id, _MultiRef


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _current_pipeline() -> Pipeline:
    """Return the active pipeline or raise ContextError."""
    pipeline = Pipeline._current
    if pipeline is not None:
        return pipeline
    raise ContextError("node registration")


def _build_config(base: dict, optional: dict) -> dict:
    """Build a node config dict from required fields and optional overrides.

    Only keys whose values are truthy (non-empty, non-zero) are included
    from *optional*.  Dict values are shallow-copied to prevent caller
    mutation.

    Args:
        base: Always-included key/value pairs.
        optional: Conditionally included key/value pairs.

    Returns:
        Merged config dict.
    """
    config: dict = dict(base)
    for key, value in optional.items():
        if not value:
            continue
        if isinstance(value, dict):
            config[key] = dict(value)
        elif isinstance(value, list):
            config[key] = list(value)
        else:
            config[key] = value
    return config


def _register_node(
    node_type: str,
    name: str,
    config: dict,
    *inputs: NodeRef,
) -> NodeRef:
    """Register a node in the current pipeline and connect inputs.

    Args:
        node_type: The node type string (e.g. ``"source_db"``).
        name: Human-readable node name.
        config: Node configuration dict.
        *inputs: Upstream ``NodeRef`` objects to connect as edges.

    Returns:
        A ``NodeRef`` pointing to the newly registered node.

    Raises:
        ContextError: If no pipeline context is active.
    """
    pipeline = _current_pipeline()
    node_id = _make_id(name)
    pipeline._add_node(node_id, node_type, name, config)

    for inp in inputs:
        if isinstance(inp, NodeRef):
            pipeline._add_edge(inp.node_id, node_id)

    return NodeRef(node_id, pipeline)


def _input_args(input: Optional[NodeRef]) -> tuple[NodeRef, ...]:
    """Return a tuple of inputs suitable for ``_register_node``."""
    if input is None:
        return ()
    return (input,)


# ===================================================================
# Sources
# ===================================================================

def source_db(
    name: str,
    query: str = "",
    conn_id: str = "",
    uri: str = "",
    retries: int = 0,
    retry_backoff: str = "exponential",
    timeout: int = 0,
) -> NodeRef:
    """Database source -- query Postgres, MySQL, or SQLite.

    Example::

        with Pipeline("Sales ETL") as p:
            raw = source_db(
                "Extract orders",
                query="SELECT * FROM orders WHERE date > '2025-01-01'",
                conn_id="warehouse",
                retries=3,
            )
    """
    optional: dict = {
        "conn_id": conn_id,
        "uri": uri,
    }
    if retries > 0:
        optional["max_retries"] = retries
        optional["retry_backoff"] = retry_backoff
    if timeout > 0:
        optional["timeout"] = timeout

    config = _build_config({"query": query}, optional)
    return _register_node("source_db", name, config)


def source_api(
    name: str,
    url: str = "",
    method: str = "GET",
    headers: dict | None = None,
    body: str = "",
    conn_id: str = "",
    retries: int = 0,
    retry_backoff: str = "exponential",
    timeout: int = 30,
) -> NodeRef:
    """REST API source -- fetch data from an HTTP endpoint.

    Example::

        with Pipeline("Weather Ingest") as p:
            weather = source_api(
                "Fetch forecast",
                url="https://api.weather.gov/gridpoints/OKX/33,37/forecast",
                headers={"Accept": "application/geo+json"},
                timeout=60,
            )
    """
    optional: dict = {
        "headers": dict(headers) if headers else {},
        "body": body,
        "conn_id": conn_id,
    }
    if retries > 0:
        optional["max_retries"] = retries
        optional["retry_backoff"] = retry_backoff
    if timeout:
        optional["timeout"] = timeout

    config = _build_config({"url": url, "method": method}, optional)
    return _register_node("source_api", name, config)


def source_file(
    name: str,
    path: str = "",
    format: str = "csv",
) -> NodeRef:
    """File source -- read CSV, JSON, Excel, or XML.

    Example::

        with Pipeline("CSV Import") as p:
            data = source_file("Read users", path="/data/users.csv", format="csv")
    """
    config = _build_config({"path": path, "format": format}, {})
    return _register_node("source_file", name, config)


# ===================================================================
# Processing
# ===================================================================

def _parse_transform_rules(rules: list) -> list:
    """Convert a list of transform rules (strings or dicts) to rule objects.

    Validates and normalizes dict rules against the backend's
    TransformRule schema so schema drift surfaces at Pipeline build
    time, not at pipeline run time.

    Accepts this shorthand for ergonomic reasons and rewrites it to
    the canonical shape:

        # rename shorthand
        {"type": "rename", "from": "ts", "to": "event_time"}
        #  → {"type": "rename", "mapping": {"ts": "event_time"}}

    Rejects rename shapes with no canonical equivalent so the user
    sees a clear error immediately instead of "rename_columns
    requires mapping" at run time from a worker pod log they can't
    easily reach.

    String rules are passed through as legacy "expression" rules
    unchanged — several existing tests rely on this shape. Note
    that these do NOT actually execute on the backend today; that's
    a separate known issue.
    """
    parsed: list = []
    for i, rule in enumerate(rules):
        if isinstance(rule, str):
            # Legacy passthrough — the backend doesn't understand this
            # shape, but tests and older user code construct it.
            parsed.append({"type": "expression", "expression": rule})
            continue
        if not isinstance(rule, dict):
            raise TypeError(
                f"transform rule #{i + 1} must be a dict or string, "
                f"got {type(rule).__name__}. Use e.g. "
                "{'type': 'rename', 'mapping': {'old': 'new'}}"
            )
        rule = dict(rule)  # defensive copy
        rtype = rule.get("type", "")

        # Rename normalization: accept {from, to} shorthand, convert
        # to the canonical {mapping: {from: to}} shape the engine
        # expects. Without this the SDK sends the shorthand straight
        # through and every rename rule fails at run time with
        # "rename_columns requires mapping".
        if rtype in ("rename", "rename_columns"):
            if "from" in rule and "to" in rule and "mapping" not in rule:
                rule["mapping"] = {rule.pop("from"): rule.pop("to")}
            elif "from" in rule or "to" in rule:
                # Partial shorthand — missing half of the pair.
                raise ValueError(
                    f"transform rule #{i + 1} (rename): {{from, to}} shorthand "
                    "requires both keys; use {'mapping': {old: new}} for the canonical form"
                )
            if not isinstance(rule.get("mapping"), dict) or not rule["mapping"]:
                raise ValueError(
                    f"transform rule #{i + 1} (rename): requires non-empty 'mapping' "
                    "dict, e.g. {'type': 'rename', 'mapping': {'old_col': 'new_col'}}"
                )

        parsed.append(rule)
    return parsed


def transform(
    name: str,
    input: Optional[NodeRef] = None,
    rules: list | None = None,
) -> NodeRef:
    """Transform data -- filter, sort, rename, aggregate, deduplicate.

    Example::

        with Pipeline("Clean") as p:
            raw = source_db("Extract", query="SELECT * FROM events")
            clean = transform(
                "Normalize",
                input=raw,
                rules=[
                    {"type": "filter_rows", "condition": "status != 'deleted'"},
                    {"type": "rename", "mapping": {"ts": "event_time"}},
                ],
            )

    Rule types match the backend engine's TransformRule schema:

        rename / rename_columns  -> {"mapping": {"old": "new", ...}}
        drop_columns             -> {"columns": ["col1", "col2"]}
        add_column               -> {"name": "x", "expression": "a + b"}
        filter_rows              -> {"condition": "col > 0"}
        replace_values           -> {"column": "status", "mapping": {"a": "b"}}
        sort                     -> {"column": "col", "ascending": true}
        deduplicate              -> {"columns": ["id"]}  (optional)
        aggregate                -> {"group_by": ["k"],
                                     "agg_fields": [{"column": "v",
                                                     "function": "sum"}]}
    """
    config: dict = {}
    if rules:
        config["rules"] = _parse_transform_rules(list(rules))

    return _register_node("transform", name, config, *_input_args(input))


def join(
    name: str,
    left: Optional[NodeRef] = None,
    right: Optional[NodeRef] = None,
    on: str = "",
    how: str = "inner",
) -> NodeRef:
    """Join two datasets -- inner, left, right, or full.

    Example::

        with Pipeline("Merge") as p:
            users = source_db("Users", query="SELECT * FROM users")
            orders = source_db("Orders", query="SELECT * FROM orders")
            merged = join("User orders", left=users, right=orders,
                          on="user_id", how="left")
    """
    config: dict = {"join_type": how}

    if "=" in on:
        parts = on.split("=", 1)
        config["left_key"] = parts[0].strip()
        config["right_key"] = parts[1].strip()
    else:
        config["left_key"] = on
        config["right_key"] = on

    args: list[NodeRef] = []
    if left is not None:
        args.append(left)
    if right is not None:
        args.append(right)
    return _register_node("join", name, config, *args)


def _parse_quality_rules(rules: list) -> list:
    """Convert a list of quality rules (strings or dicts) to rule objects.

    String rules are parsed via ``brokoli.parsing.parse_quality_rule``.
    Dict rules are shallow-copied.
    """
    parsed: list = []
    for rule in rules:
        if isinstance(rule, str):
            parsed.append(parse_quality_rule(rule))
            continue
        if isinstance(rule, dict):
            parsed.append(dict(rule))
            continue
    return parsed


def quality_check(
    name: str,
    input: Optional[NodeRef] = None,
    rules: list | None = None,
) -> NodeRef:
    """Quality check -- validate data against rules.

    Example::

        with Pipeline("QA") as p:
            data = source_db("Extract", query="SELECT * FROM users")
            quality_check(
                "Validate users",
                input=data,
                rules=[
                    "not_null(email)",
                    "unique(user_id)",
                    "min(age, 0)",
                    {"column": "name", "rule": "not_null", "params": {},
                     "on_failure": "block"},
                ],
            )
    """
    config: dict = {}
    if rules:
        config["rules"] = _parse_quality_rules(list(rules))

    return _register_node("quality_check", name, config, *_input_args(input))


def code(
    name: str,
    input: Optional[NodeRef] = None,
    language: str = "python",
    script: str = "",
    python_path: str = "",
) -> NodeRef:
    """Custom code node -- run Python (or other) scripts.

    Example::

        with Pipeline("Custom") as p:
            data = source_db("Extract", query="SELECT * FROM events")
            code("Enrich", input=data, script=\"\"\"
                import pandas as pd
                df = pd.DataFrame(rows, columns=columns)
                df['year'] = pd.to_datetime(df['date']).dt.year
                output_data = {"columns": list(df.columns), "rows": df.to_dict("records")}
            \"\"\")
    """
    config = _build_config(
        {"language": language, "script": script},
        {"python_path": python_path},
    )
    return _register_node("code", name, config, *_input_args(input))


# ===================================================================
# Sinks
# ===================================================================

def sink_db(
    name: str,
    input: Optional[NodeRef] = None,
    table: str = "",
    mode: str = "append",
    conn_id: str = "",
    uri: str = "",
    retries: int = 0,
) -> NodeRef:
    """Database sink -- write data to a table.

    Example::

        with Pipeline("Load") as p:
            data = source_db("Extract", query="SELECT * FROM staging.users")
            sink_db("Write users", input=data, table="public.users",
                    mode="upsert", conn_id="warehouse")
    """
    optional: dict = {
        "conn_id": conn_id,
        "uri": uri,
    }
    if retries > 0:
        optional["max_retries"] = retries

    config = _build_config({"table": table, "mode": mode}, optional)
    return _register_node("sink_db", name, config, *_input_args(input))


def sink_file(
    name: str,
    input: Optional[NodeRef] = None,
    path: str = "",
    format: str = "csv",
    compress: str = "",
) -> NodeRef:
    """File sink -- write data to CSV, JSON, Parquet, etc.

    Example::

        with Pipeline("Export") as p:
            data = source_db("Extract", query="SELECT * FROM reports")
            sink_file("Save report", input=data, path="/output/report.parquet",
                      format="parquet", compress="snappy")
    """
    config = _build_config(
        {"path": path, "format": format},
        {"compress": compress},
    )
    return _register_node("sink_file", name, config, *_input_args(input))


def sink_api(
    name: str,
    input: Optional[NodeRef] = None,
    url: str = "",
    method: str = "POST",
    body: str = "",
    headers: dict | None = None,
) -> NodeRef:
    """API sink -- send data to an HTTP endpoint.

    Example::

        with Pipeline("Webhook") as p:
            data = source_db("Extract", query="SELECT * FROM events")
            sink_api("Post events", input=data,
                     url="https://ingest.example.com/events",
                     headers={"Authorization": "Bearer $API_TOKEN"})
    """
    optional: dict = {
        "body_template": body,
        "headers": dict(headers) if headers else {},
    }
    config = _build_config({"url": url, "method": method}, optional)
    return _register_node("sink_api", name, config, *_input_args(input))


# ===================================================================
# Flow control
# ===================================================================

def migrate(
    name: str,
    source_uri: str = "",
    target_uri: str = "",
    query: str = "",
    table: str = "",
    mode: str = "append",
    source_conn_id: str = "",
    target_conn_id: str = "",
) -> NodeRef:
    """Database migration -- copy data between two databases.

    Example::

        with Pipeline("Replicate") as p:
            migrate("Copy users",
                    source_conn_id="oltp", target_conn_id="warehouse",
                    query="SELECT * FROM users WHERE updated_at > NOW() - INTERVAL '1 day'",
                    table="analytics.users", mode="upsert")
    """
    config = _build_config(
        {
            "source_uri": source_uri,
            "dest_uri": target_uri,
            "source_query": query,
            "dest_table": table,
            "mode": mode,
        },
        {
            "source_conn_id": source_conn_id,
            "dest_conn_id": target_conn_id,
        },
    )
    return _register_node("migrate", name, config)


def dbt(
    name: str,
    command: str = "run",
    project_dir: str = "",
    target: str = "",
    select: str = "",
    profiles_dir: str = "",
    vars: str = "",
    input: Optional[NodeRef] = None,
) -> NodeRef:
    """Run dbt commands -- run, test, build, seed, snapshot.

    Example::

        with Pipeline("Analytics") as p:
            raw = source_db("Extract", query="SELECT * FROM raw.events",
                            conn_id="warehouse")
            models = dbt("Transform", command="build",
                         select="staging.events marts.revenue",
                         project_dir="/app/dbt", target="prod")
            raw >> models >> notify("Done", notify_type="slack",
                                    webhook_url="https://hooks.slack.com/...")
    """
    config = _build_config(
        {"command": command},
        {
            "project_dir": project_dir,
            "target": target,
            "select": select,
            "profiles_dir": profiles_dir,
            "vars": vars,
        },
    )
    return _register_node("dbt", name, config, *_input_args(input))


def notify(
    name: str,
    input: Optional[NodeRef] = None,
    notify_type: str = "webhook",
    webhook_url: str = "",
    message: str = "",
    channel: str = "",
) -> NodeRef:
    """Send a notification -- Slack, webhook, or email.

    Example::

        with Pipeline("ETL") as p:
            data = source_api("Fetch", url="https://api.example.com/data")
            data >> notify("Alert Team", notify_type="slack",
                           webhook_url="https://hooks.slack.com/...",
                           message="Pipeline {{pipeline}} completed with {{rows}} rows",
                           channel="#data-alerts")
    """
    config = _build_config(
        {"notify_type": notify_type, "webhook_url": webhook_url},
        {
            "message": message,
            "channel": channel,
        },
    )
    return _register_node("notify", name, config, *_input_args(input))


def condition_node(
    name: str,
    expression: str = "",
    input: Optional[NodeRef] = None,
) -> NodeRef:
    """If/else branch based on a data condition.

    For more complex conditions, use the ``@condition`` decorator instead.

    Example::

        with Pipeline("Branch") as p:
            data = source_db("Extract", query="SELECT * FROM events")
            gate = condition_node("Has data?", expression="row_count > 0",
                                  input=data)
    """
    config = _build_config({"expression": expression}, {})
    return _register_node("condition", name, config, *_input_args(input))


def parallel(*nodes: NodeRef) -> _MultiRef | NodeRef:
    """Mark nodes as parallel.  Returns a multi-ref for chaining.

    Example::

        with Pipeline("Fan-out") as p:
            raw = source_db("Extract", query="SELECT 1")
            a = transform("A", input=raw, rules=["FILTER x > 0"])
            b = transform("B", input=raw, rules=["FILTER x < 0"])
            parallel(a, b) >> sink_db("Load", table="results")
    """
    pipeline = Pipeline._current
    refs = [n for n in nodes if isinstance(n, NodeRef)]

    if len(refs) == 1:
        return refs[0]
    if pipeline is not None:
        return _MultiRef(refs, pipeline)
    return refs[0] if refs else nodes[0]
