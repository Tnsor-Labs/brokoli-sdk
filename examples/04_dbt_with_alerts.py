"""
Example 4: dbt Orchestration with Conditional Alerts
─────────────────────────────────────────────────────
Extract raw events, run dbt models, check if results are valid,
and notify the team on success or alert on failure.

Showcases: source_db, dbt, @condition, notify, branching.

Run:
    brokoli deploy examples/04_dbt_with_alerts.py --server http://localhost:8080
"""

from brokoli import Pipeline, condition, source_db, dbt, notify


with Pipeline(
    "dbt-analytics",
    description="Run dbt models and alert team",
    schedule="0 4 * * *",
    tags=["dbt", "analytics"],
) as p:

    raw = source_db(
        "Extract Raw Events",
        query="SELECT COUNT(*) as cnt FROM raw.events WHERE date = CURRENT_DATE - 1",
        conn_id="warehouse",
    )

    models = dbt(
        "Build Models",
        command="build",
        select="staging.events marts.revenue marts.engagement",
        project_dir="/app/dbt",
        target="prod",
    )

    @condition("Models succeeded?")
    def models_ok(rows):
        # dbt output has status column — check no failures
        return all(r.get("status") != "error" for r in rows)

    success_alert = notify(
        "Slack: Success",
        notify_type="slack",
        webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
        message="dbt build completed: {{rows}} models processed",
        channel="#data-team",
    )

    failure_alert = notify(
        "Slack: Failure",
        notify_type="slack",
        webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
        message="dbt build FAILED — check logs for pipeline {{pipeline}}",
        channel="#data-alerts",
    )

    raw >> models
    with models_ok(models) as (ok, fail):
        ok >> success_alert
        fail >> failure_alert
