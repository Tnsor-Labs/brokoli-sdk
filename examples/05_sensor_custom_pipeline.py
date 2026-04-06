"""
Example 5: Fully Custom Pipeline with Sensor
─────────────────────────────────────────────
Wait for a daily export file, read it with a custom source,
enrich with an API call, validate, and push to a custom sink.

Zero built-in nodes — 100% decorators. Shows you can build
a complete pipeline without knowing the node type system.

Showcases: @sensor, @source, @map, @filter, @validate, @sink.

Run:
    brokoli deploy examples/05_sensor_custom_pipeline.py --server http://localhost:8080
"""

from brokoli import Pipeline, sensor, source, map, filter, validate, sink


with Pipeline(
    "daily-partner-ingest",
    description="Wait for partner file, enrich, validate, push to CRM",
    schedule="0 8 * * 1-5",
    tags=["partner", "ingest"],
) as p:

    @sensor("Wait for partner file", poll_interval=30, timeout=1800)
    def file_ready():
        import os
        return os.path.exists("/data/incoming/partner_leads.csv")

    @source("Read Partner CSV")
    def read_csv():
        import csv
        rows = []
        with open("/data/incoming/partner_leads.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        return rows

    @map
    def normalize(row):
        row["email"] = row.get("email", "").strip().lower()
        row["name"] = row.get("name", "").strip().title()
        row["phone"] = row.get("phone", "").replace("-", "").replace(" ", "")
        return row

    @filter
    def valid_email(row):
        email = row.get("email", "")
        return "@" in email and "." in email.split("@")[-1]

    @validate("Minimum 10 leads")
    def enough_leads(rows):
        return len(rows) >= 10, f"Got {len(rows)} leads"

    @sink("Push to CRM", retries=3, timeout=120)
    def push_to_crm(rows):
        import urllib.request, json
        for row in rows:
            data = json.dumps(row).encode()
            req = urllib.request.Request(
                "https://api.crm.example.com/leads",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)

    file_ready >> read_csv >> normalize >> valid_email >> enough_leads >> push_to_crm
