"""
Example 1: Hello World
──────────────────────
Fetch users from a public API, enrich each row, filter active ones,
and save to CSV.

Showcases: @source, @map, @filter, sink_file, >> chaining.

Run:
    brokoli compile examples/01_hello_world.py
    brokoli deploy examples/01_hello_world.py --server http://localhost:8080
"""

from brokoli import Pipeline, source, map, filter, sink_file


with Pipeline("hello-world", description="Fetch users, enrich, filter, export") as p:

    @source("Fetch Users")
    def fetch_users():
        import urllib.request, json
        url = "https://jsonplaceholder.typicode.com/users"
        resp = urllib.request.urlopen(url, timeout=10)
        return json.loads(resp.read())

    @map
    def enrich(row):
        row["domain"] = row.get("email", "").split("@")[-1]
        row["city"] = row.get("address", {}).get("city", "")
        return row

    @filter
    def has_company(row):
        return bool(row.get("company", {}).get("name"))

    export = sink_file("Save CSV", path="/data/users.csv", format="csv")

    fetch_users >> enrich >> has_company >> export
