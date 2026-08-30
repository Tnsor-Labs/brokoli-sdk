"""Process one million PostgreSQL orders with the Python SDK.

The SQL source does set-based filtering and normalization. The Python task
performs application-level enrichment, then a blocking quality gate protects
an idempotent warehouse upsert.

Run with the local test server and database:

    BROKOLI_SERVER=http://localhost:8090 \
    BROKOLI_USERNAME=ts-test-admin BROKOLI_PASSWORD='...' \
    python examples/08_postgres_order_load.py
"""

import os

from brokoli import Client, Pipeline, code, quality_check, sink_db, source_db


SOURCE_URI = os.getenv(
    "ORDERS_SOURCE_URI",
    "postgres://tsbench:tsbench-pass@127.0.0.1:55432/orders",
)
TARGET_URI = os.getenv("ORDERS_TARGET_URI", SOURCE_URI)


with Pipeline(
    "PostgreSQL order load test (Python)",
    pipeline_id=os.getenv("ORDERS_PIPELINE_ID", "postgres-order-load-test-python"),
    description="Process a large PostgreSQL order table with Python nodes.",
    tags=["load-test", "orders", "python"],
    sla="01:00 UTC",
) as p:
    source = source_db(
        "Read seeded orders",
        query=(
            "SELECT order_id, customer_id, upper(currency) AS currency, amount, "
            "ordered_at, status FROM orders "
            "WHERE amount >= 0 AND order_id IS NOT NULL"
        ),
        uri=SOURCE_URI,
        retries=2,
        timeout=120,
    )

    enriched = code(
        "Add amount cents",
        input=source,
        language="python",
        retries=2,
        timeout=300,
        script="""
output_rows = []
for row in rows:
    output_rows.append({
        **row,
        "amount_cents": round(float(row["amount"]) * 100),
        "processed_by": "brokoli-python",
    })
output_data = {
    "columns": [
        "order_id", "customer_id", "currency", "amount", "ordered_at",
        "status", "amount_cents", "processed_by",
    ],
    "rows": output_rows,
}
""",
    )
    checked = quality_check(
        "Check processed orders",
        input=enriched,
        rules=[
            "not_null(order_id)",
            "min(amount_cents, 0)",
        ],
    )
    output = sink_db(
        "Upsert processed orders",
        input=checked,
        uri=TARGET_URI,
        table="orders_processed_python",
        mode="upsert",
        key_columns=["order_id"],
    )
    enriched >> checked >> output


if __name__ == "__main__":
    client = Client.from_env(
        username=os.getenv("BROKOLI_USERNAME"),
        password=os.getenv("BROKOLI_PASSWORD"),
    )
    client.deploy(p)
    run = client.run(p)
    detail = run.wait(timeout=600, raise_on_failure=True)
    print({
        "status": detail.get("status"),
        "run_id": detail.get("id"),
        "nodes": [
            {
                "node": node.get("node_id"),
                "status": node.get("status"),
                "rows": node.get("row_count"),
                "duration_ms": node.get("duration_ms"),
                "rows_per_sec": node.get("rows_per_sec"),
            }
            for node in detail.get("node_runs", [])
        ],
    })
