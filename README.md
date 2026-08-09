# Brokoli Python SDK

Define data pipelines in Python, run them visually.

See the [developer-experience roadmap](docs/developer-experience-roadmap.md)
for the current support boundaries and planned SDK/backend work.

```bash
pip install brokoli
```

## Quick Start

```python
from brokoli import Pipeline, task, source_api, quality_check, sink_file

with Pipeline("my_pipeline", schedule="0 6 * * *") as p:

    data = source_api("Fetch Data", url="https://api.example.com/data", retries=3)

    @task("Transform")
    def clean(raw):
        return [r for r in raw if r.get("status") == "active"]

    cleaned = clean(data)

    quality_check("Validate", cleaned, rules=["not_null(id)", "unique(id)", "row_count(min=1)"])

    cleaned >> sink_file("Save", path="/tmp/output.csv")
```

```bash
# Deploy to Brokoli server (validates first)
brokoli deploy my_pipeline.py --server http://localhost:9900

# Pipeline appears in the visual editor instantly
```

## Core Concepts

### Pipeline

Context manager that collects nodes and edges into a DAG.

```python
with Pipeline(
    "name",
    description="...",
    schedule="0 6 * * *",          # cron expression
    sla="07:30 America/New_York",  # must finish by this time
    depends_on=["other_pipeline"], # wait for upstream
    tags=["etl", "production"],
    catch_up=True,                 # backfill missed runs
    webhook=True,                  # enable HTTP trigger
    on_success=lambda p, run: ..., # lifecycle hooks
    on_failure=lambda p, run, err: ...,
) as p:
    ...
```

### Nodes

14 built-in node types:

```python
# Sources
source_db("Name", query="SELECT ...", conn_id="pg", retries=3)
source_api("Name", url="https://...", headers={...}, retries=3, timeout=30)
source_file("Name", path="/data/input.csv", format="csv")

# Processing
transform("Name", input, rules=["drop_null(id)", "deduplicate(email)"])
join("Name", left, right, on="id=customer_id", how="left")
quality_check("Name", input, rules=["not_null(id)", "unique(id)", "min(amount, 0)"])

# Outputs
sink_db("Name", input, table="output", mode="append", conn_id="pg")
sink_file("Name", input, path="/tmp/out.csv", format="csv")
sink_api("Name", input, url="https://hooks.slack.com/...", method="POST")
```

Every node-producing API accepts `node_key=` for explicit logical identity:

```python
orders = source_db("Daily Orders", query="SELECT ...", node_key="orders-source")
```

Keys are used exactly as IDs and must match
`^[a-z][a-z0-9_-]{0,63}$`; invalid or duplicate keys fail pipeline
construction. Without a key, IDs are deterministic within each pipeline:
the canonical display-name base plus a per-base counter, such as
`daily_orders_1` and `daily_orders_2`. Adding another node with the same
canonical name can therefore renumber later same-name nodes; use explicit
keys where identity must survive reordering or display-name changes.

**Migration:** releases before deterministic identity generated random node
IDs. The first deployment after upgrading will replace those old IDs once;
assign `node_key` to important nodes before that deployment if downstream
history or references need a deliberate stable identity.

### @task — Python Functions as Nodes

Real Python functions with full IDE support. Source code is extracted at deploy time.

```python
@task("Compute Features", retries=2, timeout=120)
def compute(df):
    import pandas as pd
    df = pd.DataFrame(df)
    df["score"] = df["revenue"] * 0.3 + df["usage"] * 0.7
    return df.to_dict("records")

result = compute(input_data)
```

- Functions receive `rows` (list of dicts) from upstream node
- Return a list of dicts (rows) or a pandas DataFrame
- Imports inside functions are fine — only needed on the server, not at deploy time
- Full pytest support — test functions locally before deploying

#### Module context — constants, helpers, and imports

By default (`package="auto"`), a task's deployed source isn't just its
isolated function body — any module-level constant or same-module helper
function it references is auto-detected (by inspecting the function's
bytecode) and included, and any module-level import it needs is re-emitted
too:

```python
API_BASE = "https://api.example.com"   # module-level constant

def _normalize(row, base):             # module-level helper
    row["base"] = base
    return row

@task
def clean(rows):
    return [_normalize(r, API_BASE) for r in rows]
```

`clean`'s deployed package includes `API_BASE`, `_normalize`, and `clean`
itself. If a task references something that can't be safely auto-included
this way — an imported class instance, a bound method, or any other object
that isn't JSON-serializable data, a same-module function, or an imported
name — pipeline construction fails **locally**, naming exactly what's
missing, instead of deploying something that only breaks once it runs
remotely.

For cases auto-detection can't handle (e.g. a task that legitimately needs
a whole helper module), pass `package="module"` to skip auto-detection and
deploy the task's entire containing module verbatim instead. This is
broader and heavier than the default — the whole file ships, including any
unrelated top-level code in it — so treat it as an escape hatch, not the
default choice:

```python
@task(package="module")
def clean(rows):
    return heavy_helpers.transform(rows)
```

> Deploying a task with a custom Python `runtime=`/`requirements=` or a
> container `image=` isn't supported yet — both need backend runtime/image
> dispatch that doesn't exist yet.

### Conditional branching

Use a runtime-supported condition expression and label each outgoing branch
explicitly. Conditional pipelines emit IR 2.1; ordinary pipelines remain on
IR 2.0.

```python
gate = condition_node("Has rows?", expression="row_count > 100", input=data)
gate.when(sink_db("Production", table="output", conn_id="prod"))
gate.otherwise(sink_file("Quarantine", path="/tmp/quarantine.csv"))
```

`@condition` predicates are rejected until the runtime IR can distinguish a
predicate result from the unchanged branch payload. Nested conditional routing
is also rejected instead of compiling an ambiguous graph.

### Operators — Chaining & Fan-out

```python
# Sequential
a >> b >> c

# Fan-out (one source, multiple destinations)
source >> [sink_db(...), sink_file(...), sink_api(...)]

# Fan-in (multiple sources converge)
a = source_db("A", ...)
b = source_db("B", ...)
joined = join("Merge", a, b, on="id")

# Fan-out then fan-in
source >> [branch_a, branch_b] >> merge >> output
```

### Typed References & Dynamic Expansion

> **Scope note:** everything in this section is SDK API surface and IR
> compilation only. There's no backend support yet for actually
> scheduling dynamic per-item task instances, combining dataset
> manifests, or running partition transforms — the primitives below let
> you author and validate the IR shape today; execution is
> physical-planner work that hasn't landed yet.

Node-building functions return one of five typed references (all are
`NodeRef` subclasses, so `>>`, fan-out/fan-in, and everything else above
keeps working unchanged):

- **`DatasetRef`** — a tabular dataset (rows). Returned by `source_db`,
  `source_file`, `source_api(..., response="dataset")` (the default),
  `transform`, `join`, `migrate`, `dbt`.
- **`ScalarRef`** — a single value. Returned by
  `source_api(..., response="scalar")`.
- **`ArtifactRef`** — an opaque file/binary blob. Returned by
  `source_api(..., response="artifact")`.
- **`CollectionRef`** — a dynamic collection of items whose size isn't
  known until the pipeline runs (e.g. one entry per file/page). Not
  returned by any built-in source yet — only produced by
  `@task.expand()` below.
- **`ConditionRef`** — a condition node with `.when()` and `.otherwise()`
  methods for explicit true/false routing.

Sinks, `quality_check`, `code`, and `notify` keep returning a plain
`NodeRef` — their output shape is either a
side-effect/gate or genuinely ambiguous (a `code` node can produce
anything), so they aren't force-fit into one of the typed kinds.

#### `.expand()` — dynamic fan-out

`a >> [b, c]` fans a node out to a fixed, Python-source-literal list of
destinations. `.expand()` does the same thing driven by *runtime* data —
one dynamic task instance per item of an upstream `CollectionRef` —
compiling to a **single** IR node with an `expansion` policy block, not N
static nodes:

```python
@task("Parse File")
def parse(rows):
    ...

# `files` is a CollectionRef (e.g. from a future paginated/listing source)
parsed = parse.expand(
    file=files,
    key=lambda f: f["path"],       # per-item expansion identity
    node_key="parse-files",       # identity of the logical expand node
)
```

`key=` is optional and gives each dynamic instance a stable identity
across re-runs. It's never executed locally or turned into a runnable
script — only a name/description reference is recorded; the real per-item
keying happens server-side once backend support for dynamic instances
exists. `.expand()` returns a `CollectionRef` (the dynamic collection of
per-instance outputs) — chain `.collect(mode="union")` on it to merge
results back into one dataset.

`key=` and `node_key=` are distinct: `key=` derives each runtime item's
expansion identity, while `node_key=` identifies the single logical node in
the compiled graph.

For backward compatibility, a task parameter literally named `node_key`
still works: `parse.expand(node_key=files, other=metadata)` treats a
`CollectionRef` value as an expansion input and uses the decorator/default
logical identity. A string value, such as
`parse.expand(node_key="parse-files", file=files)`, is the logical node ID.
The per-item `key=callable` behavior is unchanged.

#### `union()` / `.collect(mode="union")` — combine into one dataset

Both compile to the same `union` IR node (capabilities `compute`,
`dataset-output`) — a dedicated dataset-manifest-combination node, not a
chain of individual merge edges:

```python
from brokoli import union

# Explicit refs known at authoring time
combined = union("Combine Pages", page_a, page_b, page_c)

# Equivalent, for a single upstream dynamic collection:
combined = parsed.collect(mode="union")
```

#### `DatasetRef.map()` / `.filter()` vs. `@map` / `@filter`

Two similar-looking styles exist on purpose, for different jobs — pick
based on whether you want something that actually runs today:

| | `@map` / `@filter` decorators | `DatasetRef.map()` / `.filter()` |
|---|---|---|
| Executes today? | **Yes** — generates a runnable script, registers a `code` node | **No** — IR only; the function is recorded as a name reference |
| Operates on | The whole node output at once (a list of row dicts) | Conceptually, one partition at a time (RFC §12.1) |
| Node type | `code` | `dataset_map` / `dataset_filter` |

```python
@map("Enrich")            # runs today, whole-output
def enrich(row): ...

data.map(enrich_partition)  # IR-only, per-partition (no backend support yet)
```

Use `@map`/`@filter` for anything you need to actually run right now. Use
`DatasetRef.map()`/`.filter()` only when you're deliberately describing a
future per-partition transform ahead of backend support.

### Quality Rules

String-based quality rules — no boilerplate:

```python
quality_check("Validate", data, rules=[
    "not_null(email)",              # column must not have nulls
    "unique(id)",                   # all values unique
    "min(amount, 0)",               # minimum value
    "max(amount, 1000000)",         # maximum value
    "range(score, 0, 100)",         # value range
    "row_count(min=100)",           # minimum row count
    "row_count(min=1, max=10000)",  # row count range
    "regex(email, .*@.*\\..*)",     # regex pattern match
    "freshness(updated_at, max_hours=24)", # data freshness
])
```

## CLI

```bash
# Deploy pipeline to server (validates before pushing)
brokoli deploy pipeline.py --server http://localhost:9900 --api-key $API_KEY

# Deploy all pipelines in a directory
brokoli deploy pipelines/ --server http://localhost:9900

# Validate without deploying
brokoli validate pipeline.py --server http://localhost:9900

# Export pipeline as JSON (no server needed)
brokoli export pipeline.py -o pipeline.json

# Write a canonical normalized JSON comparison snapshot
brokoli compile pipeline.py --normalized > pipeline.snapshot.json

# Validate and normalize locally without printing IR or calling the server
brokoli compile pipeline.py --check

# Compare local pipeline semantics with deployed definitions
brokoli diff pipeline.py --server http://localhost:9900 --api-key $API_KEY

# Skip validation (not recommended)
brokoli deploy pipeline.py --skip-validation

# Trusted legacy servers without GET /api/capabilities only
brokoli deploy pipeline.py --allow-legacy-server
```

`deploy` and `validate` verify the target server's supported pipeline IR
versions before ordinary validation or persistence. Compatibility failures
block by default, including when `--skip-validation` is used. The
`--allow-legacy-server` escape hatch only permits a trusted server whose
capability endpoint is unavailable; it cannot override a version mismatch
reported by a reachable server.

Normalized snapshots are stable comparison artifacts, not deployment payloads.
They omit server metadata and layout and normalize only semantically unordered
values or equivalent defaults. Node, capability, tag, and `depends_on` order is
normalized; edge order is preserved because it can define input order.
`--normalized` always emits JSON and overrides the compile format; multiple
pipelines are emitted as one JSON array. Use the
ordinary deploy/export paths for wire output. Assign explicit `node_key` values
to important nodes for durable snapshots, especially where same-name node
insertion could renumber generated IDs.

`compile`, `diff`, and other file-based commands import pipeline modules.
Imports still execute module top-level code and its side effects.

### Validation

The SDK validates before deploying:

- Missing required fields (query, url, path, table, conn_id)
- Empty code nodes
- Missing quality check rules
- Missing join keys
- Disconnected nodes
- Referenced connections that don't exist on the server

```
$ brokoli deploy pipeline.py
  Validating Revenue Pipeline...
  ✗ [ERROR] Write to DWH: Connection 'dwh_postgres' does not exist on the server
  Deploy BLOCKED — fix 1 error(s) above
```

### Authentication

```bash
# Via CLI flag
brokoli deploy pipeline.py --api-key eyJhbG...

# Via environment variable
export BROKOLI_TOKEN=eyJhbG...
brokoli deploy pipeline.py
```

## Template Variables

Use Jinja-style templates in node configs — resolved at runtime:

```python
source_db("Extract", query="SELECT * FROM orders WHERE date = '{{ ds }}'")
source_api("Fetch", url="https://api.example.com/data?date={{ ds }}")
source_api("Auth API", headers={"Authorization": "Bearer {{ secret.api_key }}"})
sink_file("Save", path="/data/output_{{ ds }}.csv")
```

| Variable | Description |
|----------|-------------|
| `{{ ds }}` | Execution date (YYYY-MM-DD) |
| `{{ next_ds }}` | Next execution date |
| `{{ ts }}` | Execution timestamp (ISO 8601) |
| `{{ var.key }}` | Variable from Brokoli Variables store |
| `{{ secret.key }}` | Secret from environment (`BROKED_SECRET_*`) |
| `{{ param.key }}` | Runtime parameter |

## Examples

See [`examples/`](examples/) for runnable and architecture-reference pipelines:

| Example | Use Case |
|---------|----------|
| [Hello World](examples/01_hello_world.py) | Public API enrichment and CSV export |
| [API to Database](examples/02_api_to_database.py) | REST ingestion, validation, and warehouse load |
| [Join and Quality](examples/03_join_and_quality.py) | Multi-source join, quality gate, and fan-out |
| [dbt with Alerts](examples/04_dbt_with_alerts.py) | dbt orchestration with conditional notifications |
| [Custom Sensor Pipeline](examples/05_sensor_custom_pipeline.py) | Decorator-only ingestion with a file sensor |

## How It Works

1. You write Python pipelines using the SDK
2. `brokoli deploy` extracts function source code via `inspect.getsource()`
3. The SDK generates Brokoli pipeline JSON with auto-computed visual layout
4. Validates all nodes, edges, and server connections
5. Pushes to the Brokoli server via API
6. Pipeline appears in the visual editor — editable in both code and UI

```
Python SDK          Brokoli Server          Visual Editor
┌──────────┐       ┌──────────────┐       ┌──────────────┐
│ @task    │       │              │       │              │
│ condition │──────▶│  Pipeline    │──────▶│  Drag & Drop │
│ >>       │deploy │  Engine      │render │  Canvas      │
│ quality_ │       │  + Profiling │       │  + Preview   │
│ check()  │       │  + Alerts    │       │  + Profiling │
└──────────┘       └──────────────┘       └──────────────┘
```

## Requirements

- Python 3.9+
- Brokoli server running (for deploy)
- No external dependencies (stdlib only)

## License

Apache 2.0
