"""
Example 3: Multi-Source Join with Quality Gate
───────────────────────────────────────────────
Load orders and customers from two databases, join them,
run quality checks, apply custom scoring logic, and fan out
to both a database and a file backup.

Showcases: source_db, join, quality_check (string rules), @task,
           fan-out with >>, sink_db, sink_file.

Run:
    brokoli deploy examples/03_join_and_quality.py --server http://localhost:8080
"""

from brokoli import (
    Pipeline, task, source_db, join, quality_check,
    sink_db, sink_file,
)
from brokoli.result import TaskResult


with Pipeline(
    "customer-360",
    description="Join orders + customers, score, export",
    schedule="0 5 * * *",
    sla="06:30 UTC",
    tags=["analytics", "customer"],
) as p:

    customers = source_db(
        "Load Customers",
        query="SELECT id, name, email, segment FROM customers",
        conn_id="crm-pg",
    )

    orders = source_db(
        "Load Orders",
        query="""
            SELECT customer_id, SUM(amount) as total_spent, COUNT(*) as order_count
            FROM orders
            WHERE created_at > NOW() - INTERVAL '90 days'
            GROUP BY customer_id
        """,
        conn_id="billing-pg",
    )

    merged = join(
        "Join Customers + Orders",
        left=customers,
        right=orders,
        on="id=customer_id",
        how="left",
    )

    checked = quality_check("Validate", input=merged, rules=[
        "not_null(email)",
        "unique(id)",
        "min(total_spent, 0)",
    ])

    @task("Score Customers")
    def score(rows):
        warnings = []
        scored = []
        for r in rows:
            spent = float(r.get("total_spent") or 0)
            count = int(r.get("order_count") or 0)
            r["score"] = round(spent * 0.6 + count * 10, 2)
            r["tier"] = "gold" if r["score"] > 500 else "silver" if r["score"] > 100 else "bronze"
            if not r.get("email"):
                warnings.append(f"Customer {r.get('id')} has no email")
            scored.append(r)
        return TaskResult(data=scored, warnings=warnings)

    warehouse = sink_db("Write to DWH", table="analytics.customer_360", conn_id="warehouse")
    backup = sink_file("CSV Backup", path="/data/backups/customer_360.csv", format="csv")

    checked >> score >> [warehouse, backup]
