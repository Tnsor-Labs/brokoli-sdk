# Projects: many pipelines in one repository

A single `pipeline.py` is fine for one pipeline. A team with dozens, spread
over several workspaces, wants them in one repository with a layout every
command understands. That is a **project**: a directory with a
`brokoli.yaml` that declares `version: 1`, and one folder per pipeline.

This guide covers creating a project, its layout and rules, the
`brokoli.yaml` reference, the project commands, and how to run them in CI.

## Quick start

```bash
pip install brokoli
brokoli init acme --workspace analytics
cd acme
brokoli new pipeline orders-daily --template python
brokoli validate
brokoli compile --project --out build
```

`brokoli init` creates:

```
acme/
  brokoli.yaml
  README.md
  .gitignore
  .github/workflows/brokoli.yml        # validates every PR (skip with --no-workflow)
  shared/__init__.py                   # code every pipeline can import
  workspaces/analytics/pipelines/example/pipeline.py
```

## The layout

```
brokoli.yaml
workspaces/
  <workspace>/
    pipelines/
      <pipeline_id>/
        pipeline.py          # exactly one Pipeline
        tasks/               # optional: code for the pipeline's tasks
        contracts/           # optional: data contracts
        tests/               # optional: fixtures
        README.md            # optional
shared/                      # optional: an importable package
```

The rules, checked by every project command:

| Rule | Why |
|---|---|
| Each pipeline has its own folder holding `pipeline.py` | the folder is the unit of review and ownership (CODEOWNERS works per folder) |
| The folder name is the pipeline's `pipeline_id` | renaming a folder is visibly renaming a pipeline; nothing is matched by guesswork |
| `pipeline.py` defines exactly one `Pipeline` | one folder, one pipeline, one diff |
| Folder names are lowercase letters, digits and single hyphens | the same shape `Pipeline("My Name")` generates as an id |
| A workspace folder is a plain relative path, not inside another's | each pipeline belongs to exactly one workspace |

Folders starting with `.` or `_` (such as `__pycache__`) are ignored.

Set `pipeline_id` explicitly in each `pipeline.py`, so changing the
display name does not change the id:

```python
from brokoli import Pipeline, source_api, transform

with Pipeline(
    "Orders, daily",                  # display name: free to change
    pipeline_id="orders-daily",       # must equal the folder name
    schedule="0 6 * * *",
) as p:
    orders = source_api("Fetch orders", url="https://api.example.com/orders")
    tidy = transform("Tidy", rules=[{"type": "rename_columns", "mapping": {"Id": "id"}}])
    orders >> tidy
```

When a rule is broken, the command lists every problem in the project at
once, each with the fix:

```
The project has problems:
  - workspaces/analytics/pipelines/Orders: folder names are pipeline ids: lowercase letters, digits and '-'
  - workspaces/analytics/pipelines/stray.py: a pipeline lives in its own folder, workspaces/analytics/pipelines/<pipeline_id>/pipeline.py
  - workspaces/finance/pipelines/ledger/pipeline.py: pipeline_id is 'ledgr' but the folder is 'ledger'; rename the folder or pass pipeline_id='ledger'
```

### Shared code

The project root is importable while pipelines load, so any
`pipeline.py` can import from `shared/`:

```python
# shared/endpoints.py
ORDERS_URL = "https://api.example.com/orders"
```

```python
# workspaces/analytics/pipelines/orders-daily/pipeline.py
from shared.endpoints import ORDERS_URL
from brokoli import Pipeline, source_api

with Pipeline("orders-daily", pipeline_id="orders-daily") as p:
    source_api("Fetch orders", url=ORDERS_URL)
```

## brokoli.yaml reference

```yaml
version: 1                     # required; this file format

workspaces:                    # required; at least one
  analytics:
    path: workspaces/analytics # the folder, relative to brokoli.yaml
    id: 01a0f3...              # optional: the server workspace, for `brokoli plan`
  finance:
    path: workspaces/finance

branches:                      # optional; which branch deploys where
  main: { production: true }
  staging:
    workspaces:                # project workspace -> server workspace
      analytics: 01a0f9...

environments:                  # optional; named servers, as before
  prod: { server: https://brokoli.example.com, token_env: BROKOLI_PROD_TOKEN }
```

| Key | Meaning |
|---|---|
| `version` | must be `1` |
| `workspaces.<name>` | a workspace of the project. Names are lowercase letters, digits, `-` and `_` |
| `workspaces.<name>.path` | its folder; must not contain `.`, `..` or empty segments, and must not overlap another workspace's |
| `workspaces.<name>.id` | the server workspace this folder corresponds to. Only commands that talk to a server from your machine use it |
| `branches.<branch>.production` | only `main` may be `true` |
| `branches.<branch>.workspaces` | for a non-production branch, the server workspace each project workspace deploys to |
| `environments` | named servers for `--env`, unchanged from earlier releases |

A `brokoli.yaml` without `version` is the older environments-only file.
It keeps working for `--env` and is not treated as a project.

## Commands

All work offline except `plan`, and `validate` when given a server.

### `brokoli init [directory]`

Creates a project. Options: `--workspace` (first workspace, default
`default`), `--example` (the example pipeline's id, default `example`),
`--template basic|python`, `--no-workflow`. It writes nothing if any file
it would create already exists, and refuses to create a project inside
another one.

### `brokoli new pipeline <id>`

Adds `<workspace>/pipelines/<id>/pipeline.py` from a template
(`--template basic` for built-in nodes, `--template python` for Python
steps). `--workspace` is needed when the project has more than one.

### `brokoli validate`

Without a file argument, loads every pipeline in the project and validates
each one. With `--server` or `--env`, workspaces that have an `id` are
also checked against that server (connections, supported node types);
the rest are checked offline and say so. `brokoli validate pipeline.py`
works on a single file exactly as before.

### `brokoli compile --project [--out build]`

Writes a build:

```
build/
  manifest.json
  pipelines/<workspace>/<pipeline_id>.json   # canonical normalized IR
  bundles/sha256-<hex>.tar.gz                # task bundles, once each
```

`manifest.json` lists every pipeline with its workspace, folder, IR file,
task bundles, and digest. The build depends only on the pipelines'
content, so the same commit compiles to the same bytes on any machine, and
each IR file's sha256 is its manifest digest:

```bash
sha256sum build/pipelines/analytics/orders-daily.json
jq -r '.pipelines[] | "\(.digest)  \(.ir)"' build/manifest.json
```

`--out` must be missing, empty, or a previous build (it has a
`manifest.json`); a previous build is replaced, anything else is left
alone. `brokoli compile --project --digest` prints the digests without
writing.

### `brokoli plan`

Compares the project with a server, workspace by workspace:

```bash
brokoli plan --env prod
```

```
Workspace analytics -> 01a0f3...
  + brand-new (added)
  ~ orders-daily (changed)
  = customers (unchanged)
  on the server, not in this folder: made-in-the-ui
Workspace finance -> 01a0f7...
  = ledger (unchanged)

2 pipeline(s) to add or change
```

- Each project workspace is compared with its own server workspace (its
  `id`). For a one-off, `--only <workspace> --workspace <server id>`.
- Pipelines are matched by `pipeline_id` (by name for a server pipeline that has none). "On the server, not in this
  folder" lists pipelines in that server workspace that the project does
  not have, for instance ones built in the UI. Nothing is ever removed.
- `--diff` prints each changed pipeline's IR diff; `--json` prints the
  whole plan as JSON; `--detailed-exitcode` exits 2 when something would
  be added or changed (0 when nothing would).

### `brokoli propose <folder> --changes <file>`

Writes changes to a pipeline's definition -- made by hand, or by a tool
that edits pipelines -- into its `pipeline.py`, keeping the rest of the
file exactly as written: comments, formatting, imports and helpers. It
needs LibCST:

```bash
pip install 'brokoli[propose]'
```

The changes are a JSON list, in the IR's terms:

```json
[
  {"target": "pipeline", "field": "schedule", "value": "*/30 * * * *"},
  {"target": "node", "node_id": "fetch_orders_1", "field": "config.timeout", "value": 30},
  {"target": "node", "node_id": "fetch_orders_1", "field": "name", "value": "Fetch shop orders"},
  {"target": "node", "node_id": "fetch_orders_1", "field": "config.records", "remove": true}
]
```

```bash
brokoli propose workspaces/analytics/pipelines/orders --changes changes.json
```

```diff
--- a/workspaces/analytics/pipelines/orders/pipeline.py
+++ b/workspaces/analytics/pipelines/orders/pipeline.py
@@ ...
-    schedule="0 * * * *",  # hourly
+    schedule="*/30 * * * *",  # hourly
 ) as p:
     orders = source_api(
-        "Fetch orders",
+        "Fetch shop orders",
         url="https://shop.example.com/orders",
-        timeout=15,
-        records="orders",
+        timeout=30,
+        node_key="fetch_orders_1",
     )
```

What it can change:

| Field | Written as |
|---|---|
| `name`, `description`, `schedule`, `schedule_timezone`, `tags` of the pipeline | the `Pipeline(...)` argument (the name is its first argument); an empty `schedule_timezone` removes the argument |
| a node's `name` | the call's first argument, plus `node_key="<its id>"` so the node keeps its id (ids come from names otherwise, and a new id would be a new node to the platform) |
| a node's `config.<key>` | the call's `<key>=` argument: replaced, added after the last argument in the call's own layout, or removed with `"remove": true` |

**Every change is verified.** After each edit the file is compiled again,
and the change is kept only when the new IR is the old IR with exactly that
change made. A change that does not come out that way is skipped, with the
reason, and the file is left without it:

| Skipped because | Example |
|---|---|
| the SDK compiles the argument to something else | `wait(timeout=30)` compiles to `"30s"`, so a change asking for `30` is refused; `"90s"` goes through |
| the node is made in a loop, a comprehension, or a helper that makes several nodes | `[sink_db(f"Copy {i}", ...) for i in (1, 2)]` |
| the node is made inside a function | `def audit(name): return transform(...)` |
| the argument does not exist | a config key the node function does not take |
| the field is not supported yet | `catch_up`, a node's inputs; adding or removing nodes and edges |

Nodes are found by where they were made: the file is compiled with a
recorder noting the call that created each node, so the right call is
edited even when two nodes have similar names, or a call contains other
calls on the same line.

Options: `--write` writes the result into `pipeline.py` (without it the
file is not touched and the diff is printed); `--json` prints `changed`,
`applied`, `skipped` (each with its `reason`), the new `source` and the
`diff`; `--output <file>` also writes that JSON to a file (whatever the pipeline
prints while it is imported stays out of it); `--project` names the project
root when not run from inside it.
`propose` writes no bytecode into the project. It exits 1 only when the
file cannot be worked on at all: it does not compile, defines more than one
pipeline, or is outside the project.

## In CI

The workflow `brokoli init` writes validates and compiles on every pull
request and on pushes to `main`:

```yaml
name: brokoli
on:
  pull_request:
  push:
    branches: [main]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "brokoli==<the version that ran init>"
      - run: brokoli validate
      - run: brokoli compile --project --out build
```

To post a plan on pull requests, add a step with a token in a repository
secret:

```yaml
      - run: brokoli plan --env prod --diff
        env:
          BROKOLI_PROD_TOKEN: ${{ secrets.BROKOLI_PROD_TOKEN }}
```

## Discovery and side effects

Loading a project imports every `pipeline.py`, as `brokoli deploy` always
has. `BROKOLI_DISCOVERY=1` is set while they load, so module-level code
that should only run for real (opening a connection, reading a large
file) can skip itself:

```python
import os
if not os.getenv("BROKOLI_DISCOVERY"):
    warehouse = connect()
```
