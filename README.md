# Brokoli Python SDK

Define data pipelines in Python, run them visually.

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

### @condition — Branching

Route data to different paths based on a Python function returning bool.

```python
@condition("Quality OK?")
def is_valid(df) -> bool:
    return len(df) > 100 and all(r.get("id") for r in df)

with is_valid(data) as (good, bad):
    good >> sink_db("Production", table="output", conn_id="prod")
    bad >> sink_file("Quarantine", path="/tmp/quarantine.csv")
```

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
brokoli deploy pipeline.py --server http://localhost:9900 --token $TOKEN

# Deploy all pipelines in a directory
brokoli deploy pipelines/ --server http://localhost:9900

# Validate without deploying
brokoli validate pipeline.py --server http://localhost:9900

# Export pipeline as JSON (no server needed)
brokoli export pipeline.py -o pipeline.json

# Skip validation (not recommended)
brokoli deploy pipeline.py --skip-validation
```

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
brokoli deploy pipeline.py --token eyJhbG...

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

See [`examples/demo/`](examples/demo/) for 7 runnable pipelines using free public APIs:

| Example | Data Source | Complexity |
|---------|-----------|------------|
| [Country Analytics](examples/demo/01_country_analytics.py) | REST Countries API | Beginner |
| [Crypto Market Tracker](examples/demo/02_crypto_market_tracker.py) | CoinGecko API | Beginner |
| [Earthquake Monitor](examples/demo/03_earthquake_monitor.py) | USGS GeoJSON | Intermediate |
| [GitHub Trending](examples/demo/04_github_trending.py) | GitHub API | Intermediate |
| [World Bank Dashboard](examples/demo/05_economic_indicators.py) | World Bank API | Intermediate |
| [FDA Adverse Events](examples/demo/06_drug_adverse_events.py) | OpenFDA API | Advanced |
| [Weather Anomaly Detection](examples/demo/07_weather_anomaly_ml.py) | Open-Meteo API | Advanced |

See [`examples/`](examples/) for architecture reference pipelines:

| Example | Use Case |
|---------|----------|
| [Revenue ETL](examples/revenue_pipeline.py) | Multi-source ETL with quality gates |
| [Fintech Reconciliation](examples/06_fintech_reconciliation.py) | 3-way payment reconciliation |
| [ML Model Training](examples/07_ml_training_deploy.py) | Train → evaluate → conditional deploy |
| [Data Drift Monitor](examples/08_data_drift_monitor.py) | Feature drift detection + auto-retrain |
| [Reverse ETL](examples/09_reverse_etl.py) | Warehouse → Salesforce/Intercom/Mailchimp |
| [dbt Orchestration](examples/10_dbt_orchestration.py) | Run dbt + validate + report |
| [GDPR Deletion](examples/11_gdpr_data_deletion.py) | Multi-system data deletion compliance |
| [Recommendation Engine](examples/12_recommendation_engine.py) | Collaborative filtering + deploy |

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
│ @condition│──────▶│  Pipeline    │──────▶│  Drag & Drop │
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
