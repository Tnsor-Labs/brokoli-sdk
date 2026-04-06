"""Tests for the Brokoli Python SDK."""

import json
import pytest
from brokoli import Pipeline, task, condition, parallel
from brokoli import source_db, source_api, source_file
from brokoli import transform, join, quality_check
from brokoli import sink_db, sink_file, sink_api
from brokoli import dbt, notify, migrate, condition_node


class TestPipelineBasic:
    def test_create_pipeline(self):
        with Pipeline("test_basic", schedule="0 6 * * *") as p:
            src = source_db("Source", query="SELECT 1")
            snk = sink_db("Sink", src, table="output")

        data = p.to_json()
        assert data["name"] == "test_basic"
        assert data["schedule"] == "0 6 * * *"
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1
        assert data["edges"][0]["from"] == src.node_id
        assert data["edges"][0]["to"] == snk.node_id

    def test_pipeline_metadata(self):
        with Pipeline(
            "test_meta",
            description="Test pipeline",
            schedule="0 2 * * *",
            sla="07:30 America/New_York",
            tags=["test", "production"],
            depends_on=["upstream"],
            catch_up=True,
            webhook=True,
        ) as p:
            source_db("S", query="SELECT 1")

        data = p.to_json()
        assert data["description"] == "Test pipeline"
        assert data["sla_deadline"] == "07:30"
        assert data["sla_timezone"] == "America/New_York"
        assert data["tags"] == ["test", "production"]
        assert data["depends_on"] == ["upstream"]
        assert data["enabled"] is True

    def test_pipeline_sla_utc_default(self):
        with Pipeline("test_sla", sla="08:00") as p:
            source_db("S", query="SELECT 1")

        data = p.to_json()
        assert data["sla_deadline"] == "08:00"
        assert data["sla_timezone"] == "UTC"


class TestChaining:
    def test_sequential_rshift(self):
        with Pipeline("test_seq") as p:
            a = source_db("A", query="SELECT 1")
            b = transform("B", a)
            c = sink_db("C", b, table="out")

        data = p.to_json()
        edges = [(e["from"], e["to"]) for e in data["edges"]]
        assert (a.node_id, b.node_id) in edges
        assert (b.node_id, c.node_id) in edges

    def test_rshift_operator(self):
        with Pipeline("test_op") as p:
            a = source_db("A", query="SELECT 1")
            b = transform("B")
            c = sink_db("C", table="out")
            a >> b >> c

        data = p.to_json()
        assert len(data["edges"]) == 2

    def test_fanout_list(self):
        with Pipeline("test_fanout") as p:
            src = source_db("Source", query="SELECT 1")
            s1 = sink_db("DB", table="t1")
            s2 = sink_file("File", path="/tmp/out.csv")
            src >> [s1, s2]

        data = p.to_json()
        edges = [(e["from"], e["to"]) for e in data["edges"]]
        assert (src.node_id, s1.node_id) in edges
        assert (src.node_id, s2.node_id) in edges

    def test_fanin_list(self):
        with Pipeline("test_fanin") as p:
            a = source_db("A", query="SELECT 1")
            b = source_db("B", query="SELECT 2")
            j = join("Join", a, b, on="id", how="left")

        data = p.to_json()
        edges = [(e["from"], e["to"]) for e in data["edges"]]
        assert (a.node_id, j.node_id) in edges
        assert (b.node_id, j.node_id) in edges

    def test_fanout_fanin(self):
        """a >> [b, c] >> d — fan out then fan in."""
        with Pipeline("test_fof") as p:
            a = source_db("A", query="SELECT 1")
            b = transform("B")
            c = transform("C")
            d = sink_db("D", table="out")
            a >> [b, c] >> d

        data = p.to_json()
        edges = [(e["from"], e["to"]) for e in data["edges"]]
        assert (a.node_id, b.node_id) in edges
        assert (a.node_id, c.node_id) in edges
        assert (b.node_id, d.node_id) in edges
        assert (c.node_id, d.node_id) in edges


class TestNodeTypes:
    def test_source_db(self):
        with Pipeline("test") as p:
            n = source_db("DB", query="SELECT * FROM t", conn_id="pg", retries=3)

        node = p._nodes[n.node_id]
        assert node["type"] == "source_db"
        assert node["config"]["query"] == "SELECT * FROM t"
        assert node["config"]["conn_id"] == "pg"
        assert node["config"]["max_retries"] == 3

    def test_source_api(self):
        with Pipeline("test") as p:
            n = source_api("API", url="https://api.example.com", retries=5, timeout=30)

        node = p._nodes[n.node_id]
        assert node["type"] == "source_api"
        assert node["config"]["url"] == "https://api.example.com"
        assert node["config"]["max_retries"] == 5

    def test_source_file(self):
        with Pipeline("test") as p:
            n = source_file("CSV", path="/data/input.csv")

        node = p._nodes[n.node_id]
        assert node["type"] == "source_file"
        assert node["config"]["path"] == "/data/input.csv"

    def test_transform(self):
        with Pipeline("test") as p:
            n = transform("Clean", rules=["drop_null(id)"])

        node = p._nodes[n.node_id]
        assert node["type"] == "transform"
        assert len(node["config"]["rules"]) == 1

    def test_join(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1")
            b = source_db("B", query="SELECT 2")
            j = join("Merge", a, b, on="id=customer_id", how="left")

        node = p._nodes[j.node_id]
        assert node["config"]["left_key"] == "id"
        assert node["config"]["right_key"] == "customer_id"
        assert node["config"]["join_type"] == "left"

    def test_quality_check_string_rules(self):
        with Pipeline("test") as p:
            n = quality_check("QC", rules=[
                "not_null(email)",
                "unique(id)",
                "min(amount, 0)",
                "range(score, 0, 100)",
                "row_count(min=100)",
            ])

        node = p._nodes[n.node_id]
        rules = node["config"]["rules"]
        assert len(rules) == 5
        assert rules[0]["rule"] == "not_null"
        assert rules[0]["column"] == "email"
        assert rules[1]["rule"] == "unique"
        assert rules[2]["rule"] == "min"
        assert rules[2]["params"]["min"] == "0"
        assert rules[3]["rule"] == "range"
        assert rules[4]["rule"] == "row_count"
        assert rules[4]["params"]["min"] == "100"

    def test_sink_db(self):
        with Pipeline("test") as p:
            n = sink_db("DWH", table="fact_revenue", mode="append", conn_id="pg")

        node = p._nodes[n.node_id]
        assert node["type"] == "sink_db"
        assert node["config"]["table"] == "fact_revenue"

    def test_sink_file(self):
        with Pipeline("test") as p:
            n = sink_file("Out", path="/out.csv", format="csv", compress="gzip")

        node = p._nodes[n.node_id]
        assert node["config"]["compress"] == "gzip"

    def test_sink_api(self):
        with Pipeline("test") as p:
            n = sink_api("Webhook", url="https://hooks.slack.com", body='{"text":"hi"}')

        node = p._nodes[n.node_id]
        assert node["config"]["url"] == "https://hooks.slack.com"


class TestTaskDecorator:
    def test_task_basic(self):
        with Pipeline("test") as p:
            src = source_db("Source", query="SELECT 1")

            @task("My Task")
            def process(df):
                return df

            result = process(src)

        assert len(p._nodes) == 2
        node = list(p._nodes.values())[1]
        assert node["type"] == "code"
        assert "def process" in node["config"]["script"]

    def test_task_with_retries(self):
        with Pipeline("test") as p:
            @task("Risky", retries=3, retry_backoff="exponential")
            def risky(df):
                return df

            risky()

        node = list(p._nodes.values())[0]
        assert node["config"]["max_retries"] == 3

    def test_task_no_args_decorator(self):
        with Pipeline("test") as p:
            @task
            def my_func(df):
                return df

            my_func()

        node = list(p._nodes.values())[0]
        assert node["name"] == "My Func"  # auto-generated from function name

    def test_task_source_extraction(self):
        with Pipeline("test") as p:
            @task("Feature Eng")
            def build_features(df):
                """Compute features."""
                df["score"] = df["a"] * 0.5
                return df

            build_features()

        node = list(p._nodes.values())[0]
        code = node["config"]["script"]
        assert "def build_features" in code
        assert 'df["score"]' in code


class TestConditionDecorator:
    def test_condition_basic(self):
        with Pipeline("test") as p:
            src = source_db("Source", query="SELECT 1")

            @condition("Has data?")
            def has_data(df) -> bool:
                return len(df) > 0

            with has_data(src) as (good, bad):
                good >> sink_db("OK", table="good")
                bad >> sink_file("Bad", path="/bad.csv")

        data = p.to_json()
        assert len(data["nodes"]) >= 4  # source + eval code + condition + at least 1 sink
        # Find condition node
        cond_nodes = [n for n in data["nodes"] if n["type"] == "condition"]
        assert len(cond_nodes) == 1
        assert cond_nodes[0]["config"]["expression"] == "row_count > 0"
        # The eval code node should have the function
        code_nodes = [n for n in data["nodes"] if n["type"] == "code"]
        assert any("def has_data" in n["config"].get("script", "") for n in code_nodes)


class TestAutoLayout:
    def test_layout_positions(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1")
            b = transform("B", a)
            c = sink_db("C", b, table="out")

        data = p.to_json()
        positions = {n["id"]: n["position"] for n in data["nodes"]}
        # First node should be leftmost
        assert positions[a.node_id]["x"] < positions[b.node_id]["x"]
        assert positions[b.node_id]["x"] < positions[c.node_id]["x"]

    def test_parallel_layout(self):
        with Pipeline("test") as p:
            a = source_db("A", query="SELECT 1")
            b = source_db("B", query="SELECT 2")
            j = join("J", a, b, on="id")

        data = p.to_json()
        positions = {n["id"]: n["position"] for n in data["nodes"]}
        # Sources should be at same x, different y
        assert positions[a.node_id]["x"] == positions[b.node_id]["x"]
        assert positions[a.node_id]["y"] != positions[b.node_id]["y"]
        # Join should be to the right
        assert positions[j.node_id]["x"] > positions[a.node_id]["x"]


class TestToJson:
    def test_full_pipeline_json(self):
        with Pipeline(
            "revenue_etl",
            description="Daily revenue",
            schedule="0 6 * * *",
            sla="07:30 UTC",
            tags=["revenue"],
            depends_on=["upstream"],
        ) as p:
            orders = source_db("Orders", query="SELECT * FROM orders", conn_id="prod", retries=3)
            cleaned = transform("Clean", orders, rules=["drop_null(id)"])
            validated = quality_check("Validate", cleaned, rules=["unique(id)", "not_null(amount)"])
            validated >> [
                sink_db("DWH", table="fact_orders", conn_id="dwh"),
                sink_file("Backup", path="/backup/orders.csv"),
            ]

        data = p.to_json()

        # Structure
        assert data["name"] == "revenue_etl"
        assert data["description"] == "Daily revenue"
        assert data["schedule"] == "0 6 * * *"
        assert data["sla_deadline"] == "07:30"
        assert data["tags"] == ["revenue"]
        assert data["depends_on"] == ["upstream"]

        # Nodes
        assert len(data["nodes"]) == 5
        types = {n["type"] for n in data["nodes"]}
        assert types == {"source_db", "transform", "quality_check", "sink_db", "sink_file"}

        # Edges: orders→clean, clean→validate, validate→dwh, validate→backup
        assert len(data["edges"]) == 4

        # All nodes have positions
        for n in data["nodes"]:
            assert "position" in n
            assert "x" in n["position"]
            assert "y" in n["position"]

        # JSON serializable
        json.dumps(data)


class TestComplexPipeline:
    def test_multi_source_join_fanout(self):
        """3 sources → 2 joins → quality → condition → fan-out."""
        with Pipeline("complex") as p:
            crm = source_db("CRM", query="SELECT * FROM customers")
            billing = source_db("Billing", query="SELECT * FROM billing")
            usage = source_api("Usage", url="https://api/usage")

            merged = join("Join CRM+Billing", crm, billing, on="id=customer_id", how="left")
            enriched = join("Enrich", merged, usage, on="id=user_id", how="left")

            checked = quality_check("Validate", enriched, rules=["unique(id)", "not_null(email)"])

            @condition("Has data?")
            def has_data(df) -> bool:
                return len(df) > 0

            with has_data(checked) as (good, bad):
                good >> [
                    sink_db("Write DWH", table="customer_360"),
                    sink_file("S3 Backup", path="/backup/c360.csv"),
                ]
                bad >> sink_api("Alert", url="https://hooks.slack.com")

        data = p.to_json()
        # 3 sources + 2 joins + quality + condition + 3 sinks = 9+ nodes
        assert len(data["nodes"]) >= 9
        assert len(data["edges"]) >= 8
        json.dumps(data)  # serializable


class TestNewNodeTypes:
    """Tests for dbt, notify, migrate, and condition_node."""

    def test_dbt_node(self):
        with Pipeline("dbt-test") as p:
            raw = source_db("Extract", query="SELECT * FROM raw.events", conn_id="warehouse")
            models = dbt("Transform Models", command="build",
                        select="staging.events marts.revenue",
                        project_dir="/app/dbt", target="prod")
            raw >> models

        data = p.to_json()
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1

        dbt_node = next(n for n in data["nodes"] if n["type"] == "dbt")
        assert dbt_node["name"] == "Transform Models"
        assert dbt_node["config"]["command"] == "build"
        assert dbt_node["config"]["select"] == "staging.events marts.revenue"
        assert dbt_node["config"]["project_dir"] == "/app/dbt"
        assert dbt_node["config"]["target"] == "prod"
        json.dumps(data)

    def test_dbt_default_command(self):
        with Pipeline("dbt-defaults") as p:
            dbt("Run All")

        data = p.to_json()
        node = data["nodes"][0]
        assert node["config"]["command"] == "run"

    def test_notify_slack(self):
        with Pipeline("notify-test") as p:
            data_node = source_api("Fetch", url="https://api.example.com/data")
            alert = notify("Alert Team", input=data_node,
                          notify_type="slack",
                          webhook_url="https://hooks.slack.com/services/T/B/X",
                          message="Pipeline {{pipeline}} done: {{rows}} rows",
                          channel="#data-alerts")

        data = p.to_json()
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1

        n = next(n for n in data["nodes"] if n["type"] == "notify")
        assert n["config"]["notify_type"] == "slack"
        assert n["config"]["webhook_url"] == "https://hooks.slack.com/services/T/B/X"
        assert n["config"]["channel"] == "#data-alerts"
        assert "{{pipeline}}" in n["config"]["message"]
        json.dumps(data)

    def test_notify_webhook(self):
        with Pipeline("webhook-test") as p:
            notify("Ping", webhook_url="https://example.com/webhook")

        data = p.to_json()
        node = data["nodes"][0]
        assert node["config"]["notify_type"] == "webhook"

    def test_migrate_node(self):
        with Pipeline("migrate-test") as p:
            migrate("Sync Users",
                    source_uri="postgres://src/db",
                    target_uri="postgres://dst/db",
                    query="SELECT * FROM users WHERE updated > '2024-01-01'",
                    table="users_mirror",
                    mode="upsert")

        data = p.to_json()
        node = data["nodes"][0]
        assert node["type"] == "migrate"
        assert node["config"]["source_uri"] == "postgres://src/db"
        assert node["config"]["dest_uri"] == "postgres://dst/db"
        assert node["config"]["dest_table"] == "users_mirror"
        assert node["config"]["source_query"] == "SELECT * FROM users WHERE updated > '2024-01-01'"
        assert node["config"]["mode"] == "upsert"
        json.dumps(data)

    def test_condition_node(self):
        with Pipeline("cond-test") as p:
            src = source_db("Fetch", query="SELECT * FROM t")
            check = condition_node("Has Data?", expression="row_count > 0", input=src)

        data = p.to_json()
        assert len(data["nodes"]) == 2
        node = next(n for n in data["nodes"] if n["type"] == "condition")
        assert node["config"]["expression"] == "row_count > 0"
        json.dumps(data)

    def test_full_pipeline_with_dbt_and_notify(self):
        """End-to-end: extract → dbt → quality → notify."""
        with Pipeline("Full Analytics", schedule="0 5 * * *") as p:
            raw = source_db("Extract Events", query="SELECT * FROM raw.events", conn_id="warehouse")

            models = dbt("dbt Build", command="build",
                        select="staging.events marts.revenue",
                        project_dir="/app/dbt", target="prod")
            raw >> models

            qc = quality_check("Validate", input=models, rules=[
                "not_null(revenue)",
                "min(revenue, 0)",
                "row_count(min=1)",
            ])

            done = notify("Slack: Done", input=qc,
                         notify_type="slack",
                         webhook_url="https://hooks.slack.com/T/B/X",
                         message="Analytics pipeline completed: {{rows}} rows",
                         channel="#analytics")

        data = p.to_json()
        assert len(data["nodes"]) == 4
        assert len(data["edges"]) == 3
        assert data["schedule"] == "0 5 * * *"
        types = {n["type"] for n in data["nodes"]}
        assert types == {"source_db", "dbt", "quality_check", "notify"}
        json.dumps(data)
