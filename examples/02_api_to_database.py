"""
Example 2: API to Database
──────────────────────────
Pull posts from a REST API, validate data quality, transform,
and load into a database table.

Showcases: source_api, @validate, transform (built-in rules), sink_db.
Shows how decorators and built-in nodes mix naturally.

Run:
    brokoli deploy examples/02_api_to_database.py --server http://localhost:8080
"""

from brokoli import Pipeline, source_api, validate, transform, sink_db


with Pipeline(
    "api-to-database",
    description="Ingest posts from API into warehouse",
    schedule="0 */6 * * *",
    tags=["ingest", "api"],
) as p:

    posts = source_api(
        "Fetch Posts",
        url="https://jsonplaceholder.typicode.com/posts",
        timeout=15,
    )

    @validate("Has data")
    def check_not_empty(rows):
        return len(rows) > 0, f"Received {len(rows)} posts"

    @validate("No missing titles", on_failure="warn")
    def check_titles(rows):
        missing = sum(1 for r in rows if not r.get("title"))
        return missing == 0, f"{missing} posts missing title"

    clean = transform("Clean", rules=[
        {"type": "rename_columns", "mapping": {"userId": "user_id"}},
        {"type": "drop_columns", "columns": ["body"]},
    ])

    load = sink_db("Load to Warehouse", table="raw.posts", conn_id="warehouse", mode="append")

    posts >> check_not_empty >> check_titles >> clean >> load
