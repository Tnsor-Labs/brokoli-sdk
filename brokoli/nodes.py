"""Built-in node types -- source, transform, sink, flow control."""

from __future__ import annotations

from typing import Optional, Any

from brokoli.exceptions import ContextError
from brokoli.pagination import PaginationStrategy
from brokoli.resources import ResourceRef
from brokoli.parsing import ParseError, parse_quality_rule
from brokoli.pipeline import (
    Pipeline, NodeRef, ConditionRef, _MultiRef,
    ArtifactRef, DatasetRef, ScalarRef,
    _build_union_node,
)
# UNSET is defined in its own module (not here) so brokoli.pipeline can use
# it too without a circular import; imported here so existing call sites
# below and ``from brokoli.nodes import UNSET`` keep working unchanged.
from brokoli.sentinel import UNSET


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _current_pipeline() -> Pipeline:
    """Return the active pipeline or raise ContextError."""
    pipeline = Pipeline.current()
    if pipeline is not None:
        return pipeline
    raise ContextError("node registration")


def _build_config(base: dict, optional: dict) -> dict:
    """Build a node config dict from required fields and optional overrides.

    Only keys whose values are not UNSET and not None are included
    from *optional*.  Dict values are shallow-copied to prevent caller
    mutation.

    Args:
        base: Always-included key/value pairs.
        optional: Conditionally included key/value pairs.

    Returns:
        Merged config dict.
    """
    config: dict = {k: _normalize_value(v) for k, v in base.items()}
    for key, value in optional.items():
        if value is UNSET:
            continue
        if value is None:
            continue
        if isinstance(value, dict):
            config[key] = {k: _normalize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            config[key] = [_normalize_value(v) for v in value]
        else:
            config[key] = _normalize_value(value)
    return config


def _normalize_value(value: Any) -> Any:
    """Compile a typed resource reference to its wire value; pass others through.

    Lets ``conn_id=Connection("warehouse")`` be used interchangeably with the
    plain string -- the ``Connection`` becomes its bare name in the IR, so
    validation, serialization, and the backend all see the string they
    already expect.
    """
    if isinstance(value, ResourceRef):
        return value.ir_value()
    return value


def _register_node(
    node_type: str,
    name: str,
    config: dict,
    *inputs: NodeRef,
    ref_cls: type = NodeRef,
    node_key: Optional[str] = None,
) -> NodeRef:
    """Register a node in the current pipeline and connect inputs.

    Args:
        node_type: The node type string (e.g. ``"source_db"``).
        name: Human-readable node name.
        config: Node configuration dict.
        *inputs: Upstream ``NodeRef`` objects to connect as edges.
        ref_cls: The ``NodeRef`` subclass to return (see
            ``brokoli.pipeline``'s typed refs -- ``ScalarRef``,
            ``ArtifactRef``, ``DatasetRef``, ``CollectionRef``). Defaults
            to plain ``NodeRef`` for node kinds where none of the typed
            refs cleanly apply.

    Returns:
        A ``ref_cls`` instance pointing to the newly registered node.

    Raises:
        ContextError: If no pipeline context is active.
    """
    pipeline = _current_pipeline()
    pipeline._validate_refs(list(inputs))
    node_id = pipeline._allocate_node_id(name, node_key)
    pipeline._add_node(node_id, node_type, name, config)

    for inp in inputs:
        if isinstance(inp, NodeRef):
            pipeline._add_edge(inp.node_id, node_id)

    return ref_cls(node_id, pipeline)


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
    conn_id: Any = UNSET,
    uri: Any = UNSET,
    retries: Any = UNSET,
    retry_backoff: str = "exponential",
    timeout: Any = UNSET,
    node_key: Optional[str] = None,
) -> DatasetRef:
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
    if retries is not UNSET and retries is not None:
        optional["max_retries"] = retries
        optional["retry_backoff"] = retry_backoff
    if timeout is not UNSET and timeout is not None:
        optional["timeout"] = timeout

    config = _build_config({"query": query}, optional)
    return _register_node(
        "source_db", name, config, ref_cls=DatasetRef, node_key=node_key
    )


def source_api(
    name: str,
    url: str = "",
    method: str = "GET",
    headers: Any = UNSET,
    body: Any = UNSET,
    conn_id: Any = UNSET,
    retries: Any = UNSET,
    retry_backoff: str = "exponential",
    timeout: Any = UNSET,
    params: Any = UNSET,
    response: str = "dataset",
    records: Any = UNSET,
    value_path: Any = UNSET,
    pagination: Any = UNSET,
    node_key: Optional[str] = None,
) -> DatasetRef | ScalarRef | ArtifactRef:
    """REST API source -- fetch data from an HTTP endpoint.

    Example::

        with Pipeline("Weather Ingest") as p:
            weather = source_api(
                "Fetch forecast",
                url="https://api.weather.gov/gridpoints/OKX/33,37/forecast",
                headers={"Accept": "application/geo+json"},
                timeout=60,
            )

    Query params
    ------------
    ``params`` is a plain dict of query-string key/value pairs, merged
    onto ``url`` by the backend at request time. Values may contain the
    same ``{{ ds }}``-style template placeholders already supported in
    ``url``/``headers``/``query`` elsewhere in this SDK (see the
    "Template Variables" section of the README) -- the SDK does not
    interpret or expand these itself; it passes ``params`` through
    unchanged as opaque config for the backend/scheduler to resolve.

    Response shape (closing RFC V2 §5.1's ambiguous-output-contract gap)
    ----------------------------------------------------------------------
    ``response`` declares how to interpret the HTTP response body, and
    is always present in the compiled config (like ``method``) so the
    contract is never ambiguous:

    - ``"dataset"`` (default): the body is, or contains via ``records``,
      a list of records forming a tabular dataset. Returns a
      :class:`~brokoli.pipeline.DatasetRef`.
    - ``"scalar"``: the body contains a single value at ``value_path``.
      Returns a :class:`~brokoli.pipeline.ScalarRef`.
    - ``"artifact"``: the raw response is stored as an opaque artifact
      (e.g. a file/binary download); no record extraction happens.
      Returns an :class:`~brokoli.pipeline.ArtifactRef`.

    ``records`` is a dot-path into the JSON body pointing at the list of
    records to extract for ``response="dataset"`` (e.g. ``"results"``,
    or ``"data.items"`` for a nested ``{"data": {"items": [...]}}``
    shape). ``value_path`` is the equivalent dot-path for
    ``response="scalar"``. Setting both is rejected by
    ``brokoli.validation`` -- they are mutually exclusive.

    Pagination (RFC V2 §14.2-14.4) -- SDK-side config only
    ---------------------------------------------------------
    ``pagination`` accepts a strategy built with one of
    ``brokoli.pagination.offset_pages``, ``cursor_pages``,
    ``numbered_pages``, ``next_link_pages``, or ``link_header_pages``,
    optionally chained with ``.with_execution(...)`` to attach a
    concurrency/rate-limit/retry/checkpoint policy. Requires
    ``response="dataset"``.

    This function only produces the declarative ``pagination`` /
    ``execution`` config blocks in the compiled IR. Expanding a
    paginated source into concrete per-page fetch instances, running
    them under the configured concurrency/rate-limit policy, and
    stitching per-page results back together is backend
    (physical-planner) work -- separate, not yet implemented, and out
    of scope for this SDK.

    Example (the RFC's GBIF worked example)::

        from brokoli import source_api
        from brokoli.pagination import offset_pages

        occurrences = source_api(
            "GBIF Occurrences",
            url="https://api.gbif.org/v1/occurrence/search",
            params={"hasCoordinate": "true", "occurrenceStatus": "PRESENT"},
            records="results",
            pagination=offset_pages(
                page_size=300, max_records=30_000, end_flag="endOfRecords",
            ).with_execution(max_concurrency=4, requests_per_second=5),
        )
    """
    optional: dict = {
        "headers": dict(headers) if headers is not UNSET and headers is not None else UNSET,
        "body": body,
        "conn_id": conn_id,
        "params": dict(params) if params is not UNSET and params is not None else UNSET,
        "records": records,
        "value_path": value_path,
    }
    if retries is not UNSET and retries is not None:
        optional["max_retries"] = retries
        optional["retry_backoff"] = retry_backoff
    if timeout is not UNSET and timeout is not None:
        optional["timeout"] = timeout

    if pagination is not UNSET and pagination is not None:
        if isinstance(pagination, PaginationStrategy):
            optional["pagination"] = pagination.to_config()
            exec_config = pagination.execution_config()
            if exec_config:
                optional["execution"] = exec_config
        elif isinstance(pagination, dict):
            optional["pagination"] = dict(pagination)
        else:
            raise TypeError(
                "source_api(pagination=...) must be a PaginationStrategy "
                "(from brokoli.pagination, e.g. offset_pages(...)) or a "
                f"plain dict, got {type(pagination).__name__}"
            )

    config = _build_config({"url": url, "method": method, "response": response}, optional)
    ref_cls = {
        "dataset": DatasetRef,
        "scalar": ScalarRef,
        "artifact": ArtifactRef,
    }.get(response, DatasetRef)
    return _register_node(
        "source_api", name, config, ref_cls=ref_cls, node_key=node_key
    )


def source_file(
    name: str,
    path: str = "",
    format: str = "csv",
    node_key: Optional[str] = None,
) -> DatasetRef:
    """File source -- read CSV, JSON, Excel, or XML.

    Example::

        with Pipeline("CSV Import") as p:
            data = source_file("Read users", path="/data/users.csv", format="csv")
    """
    config = _build_config({"path": path, "format": format}, {})
    return _register_node(
        "source_file", name, config, ref_cls=DatasetRef, node_key=node_key
    )


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
    node_key: Optional[str] = None,
) -> DatasetRef:
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

    return _register_node(
        "transform", name, config, *_input_args(input),
        ref_cls=DatasetRef, node_key=node_key,
    )


def join(
    name: str,
    left: Optional[NodeRef] = None,
    right: Optional[NodeRef] = None,
    on: str = "",
    how: str = "inner",
    node_key: Optional[str] = None,
) -> DatasetRef:
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
    return _register_node(
        "join", name, config, *args, ref_cls=DatasetRef, node_key=node_key
    )


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
    node_key: Optional[str] = None,
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

    return _register_node(
        "quality_check", name, config, *_input_args(input), node_key=node_key
    )


def code(
    name: str,
    input: Optional[NodeRef] = None,
    language: str = "python",
    script: str = "",
    python_path: Any = UNSET,
    node_key: Optional[str] = None,
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
    return _register_node(
        "code", name, config, *_input_args(input), node_key=node_key
    )


# ===================================================================
# Sinks
# ===================================================================

def sink_db(
    name: str,
    input: Optional[NodeRef] = None,
    table: str = "",
    mode: str = "append",
    conn_id: Any = UNSET,
    uri: Any = UNSET,
    key_columns: Any = UNSET,
    retries: Any = UNSET,
    node_key: Optional[str] = None,
) -> NodeRef:
    """Database sink -- write a dataset to a table.

    The server generates and runs the write SQL from the input rows, so no
    upstream ``sql_generate`` node is needed. ``mode`` selects how:

    * ``"append"`` (default) -- add the rows to whatever is already there.
    * ``"overwrite"`` -- clear the table first, then add the rows.
    * ``"upsert"`` -- insert, updating rows that collide on ``key_columns``
      (required for upsert; the conflict target). Supported on Postgres,
      SQLite, and MySQL.

    Example::

        with Pipeline("Load") as p:
            users = source_db("Extract", query="SELECT * FROM staging.users")
            sink_db("Write users", input=users, table="public.users",
                    mode="upsert", key_columns=["id"], conn_id="warehouse")
    """
    optional: dict = {
        "conn_id": conn_id,
        "uri": uri,
        "key_columns": (
            list(key_columns)
            if key_columns is not UNSET and key_columns is not None
            else UNSET
        ),
    }
    if retries is not UNSET and retries is not None:
        optional["max_retries"] = retries

    config = _build_config({"table": table, "mode": mode}, optional)
    return _register_node(
        "sink_db", name, config, *_input_args(input), node_key=node_key
    )


def sink_file(
    name: str,
    input: Optional[NodeRef] = None,
    path: str = "",
    format: str = "csv",
    compress: Any = UNSET,
    node_key: Optional[str] = None,
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
    return _register_node(
        "sink_file", name, config, *_input_args(input), node_key=node_key
    )


def sink_api(
    name: str,
    input: Optional[NodeRef] = None,
    url: str = "",
    method: str = "POST",
    body: Any = UNSET,
    headers: Any = UNSET,
    node_key: Optional[str] = None,
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
        "headers": dict(headers) if headers is not UNSET and headers is not None else UNSET,
    }
    config = _build_config({"url": url, "method": method}, optional)
    return _register_node(
        "sink_api", name, config, *_input_args(input), node_key=node_key
    )


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
    key_columns: Any = UNSET,
    source_conn_id: Any = UNSET,
    target_conn_id: Any = UNSET,
    node_key: Optional[str] = None,
) -> DatasetRef:
    """Database migration -- copy rows from one database to another.

    Runs ``query`` against the source and writes the results into ``table``
    on the target, generating the SQL from the rows. ``mode`` works the same
    as :func:`sink_db`:

    * ``"append"`` (default) -- add the rows.
    * ``"overwrite"`` -- clear the target table first, then add the rows.
    * ``"upsert"`` -- insert, updating rows that collide on ``key_columns``
      (required for upsert).

    Example::

        with Pipeline("Replicate") as p:
            migrate("Copy users",
                    source_conn_id="oltp", target_conn_id="warehouse",
                    query="SELECT * FROM users WHERE updated_at > NOW() - INTERVAL '1 day'",
                    table="analytics.users", mode="upsert", key_columns=["id"])
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
            "key_columns": (
                list(key_columns)
                if key_columns is not UNSET and key_columns is not None
                else UNSET
            ),
        },
    )
    return _register_node(
        "migrate", name, config, ref_cls=DatasetRef, node_key=node_key
    )


def dbt(
    name: str,
    command: str = "run",
    project_dir: Any = UNSET,
    target: Any = UNSET,
    select: Any = UNSET,
    profiles_dir: Any = UNSET,
    vars: Any = UNSET,
    input: Optional[NodeRef] = None,
    node_key: Optional[str] = None,
) -> DatasetRef:
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
    return _register_node(
        "dbt", name, config, *_input_args(input),
        ref_cls=DatasetRef, node_key=node_key,
    )


def notify(
    name: str,
    input: Optional[NodeRef] = None,
    notify_type: str = "webhook",
    webhook_url: str = "",
    message: Any = UNSET,
    channel: Any = UNSET,
    node_key: Optional[str] = None,
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
    return _register_node(
        "notify", name, config, *_input_args(input), node_key=node_key
    )


def condition_node(
    name: str,
    expression: str = "",
    input: Optional[NodeRef] = None,
    node_key: Optional[str] = None,
) -> ConditionRef:
    """If/else branch based on a data condition.

    Example::

        with Pipeline("Branch") as p:
            data = source_db("Extract", query="SELECT * FROM events")
            gate = condition_node("Has data?", expression="row_count > 0",
                                  input=data)
            gate.when(sink_db("Load", table="events"))
            gate.otherwise(notify("No data", webhook_url="https://example.test"))
    """
    config = _build_config({"expression": expression}, {})
    return _register_node(
        "condition", name, config, *_input_args(input),
        ref_cls=ConditionRef, node_key=node_key,
    )


def parallel(*nodes: NodeRef) -> _MultiRef | NodeRef:
    """Mark nodes as parallel.  Returns a multi-ref for chaining.

    Example::

        with Pipeline("Fan-out") as p:
            raw = source_db("Extract", query="SELECT 1")
            a = transform("A", input=raw, rules=["FILTER x > 0"])
            b = transform("B", input=raw, rules=["FILTER x < 0"])
            parallel(a, b) >> sink_db("Load", table="results")
    """
    pipeline = Pipeline.current()
    refs = [n for n in nodes if isinstance(n, NodeRef)]

    owner = pipeline or (refs[0].pipeline if refs else None)
    if owner is not None:
        owner._validate_refs(refs)

    if len(refs) == 1:
        return refs[0]
    if owner is not None:
        return _MultiRef(refs, owner)
    return refs[0] if refs else nodes[0]


def union(
    name: str, *refs: NodeRef, node_key: Optional[str] = None
) -> DatasetRef:
    """Combine multiple dataset/collection refs' manifests into one dataset.

    Compiles to a single ``union`` IR node (capabilities: ``compute``,
    ``dataset-output``) with an edge from each of *refs*, rather than a
    chain of individual merge edges.

    Equivalent, for a single upstream dynamic collection, to
    ``collection_ref.collect(mode="union")``
    (:meth:`brokoli.pipeline.CollectionRef.collect`) -- both compile to
    the same node type/capabilities/config, just with a different edge
    count (one edge per explicit ref here, vs. one edge from the
    collection there).

    Example::

        combined = union("Combine Pages", page_a, page_b, page_c)

    Note:
        SDK API surface and IR compilation only -- there is no backend
        support yet for actually combining dataset manifests at run time
        (brokoli-sdk#2).
    """
    if not refs:
        raise ValueError("union() requires at least one ref to combine")
    pipeline = _current_pipeline()
    return _build_union_node(pipeline, name, list(refs), node_key=node_key)
