"""Transfer a PostgreSQL table with database-side INSERT ... SELECT pushdown.

Source and target must resolve to the same database address. Core then keeps
the transfer inside PostgreSQL instead of materializing rows in the engine.
"""

import os

from brokoli import Client, Pipeline, sink_db, source_db


DATABASE_URI = os.getenv("ORDERS_DATABASE_URI", "postgres://tsbench:tsbench-pass@127.0.0.1:55432/orders")


with Pipeline(
    "PostgreSQL pushdown transfer (Python)",
    pipeline_id=os.getenv("ORDERS_PIPELINE_ID", "postgres-pushdown-transfer-python"),
    description="Move a PostgreSQL table with database-side INSERT SELECT pushdown.",
    tags=["production", "postgres", "pushdown"],
) as p:
    source = source_db(
        "Read source table",
        query="SELECT order_id, customer_id, currency, amount, ordered_at, status FROM orders",
        uri=DATABASE_URI,
    )
    target = sink_db(
        "Append target table",
        input=source,
        uri=DATABASE_URI,
        table="orders_pushdown_python",
        mode="append",
    )
    source >> target


if __name__ == "__main__":
    client = Client.from_env(
        username=os.getenv("BROKOLI_USERNAME"),
        password=os.getenv("BROKOLI_PASSWORD"),
    )
    client.deploy(p)
    detail = client.run(p).wait(timeout=600, raise_on_failure=True)
    print({"status": detail.get("status"), "run_id": detail.get("id"), "node_runs": detail.get("node_runs", [])})
