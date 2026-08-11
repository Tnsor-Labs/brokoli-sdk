"""
Global Earthquake Watch
───────────────────────
Fetch M2.5+ earthquakes from the official USGS real-time feed,
normalize the GeoJSON records, validate them, keep noteworthy
events, and export CSV and JSON reports.

No API keys, connections, databases, or secrets are required.

Run:
    brokoli deploy examples/global_earthquake_watch.py \
        --server https://in-brokoli.orkestri.site
"""

from brokoli import (
    Pipeline,
    source_api,
    task,
    map,
    filter,
    validate,
    sink_file,
)


USGS_FEED = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/"
    "summary/2.5_day.geojson"
)


with Pipeline(
    "global-earthquake-watch",
    description=(
        "Monitor noteworthy global earthquakes using the official "
        "USGS real-time feed"
    ),
    schedule="*/15 * * * *",
    tags=["public-data", "earthquakes", "monitoring", "usgs"],
) as p:

    raw_feed = source_api(
        "Fetch USGS Earthquakes",
        url=USGS_FEED,
        headers={
            "Accept": "application/geo+json",
            "User-Agent": "Brokoli-Global-Earthquake-Watch/1.0",
        },
        retries=3,
        timeout=30,
    )

    @task("Extract GeoJSON Features")
    def extract_features(payload):
        # Depending on the worker, source_api may return the JSON object
        # directly or wrap it in a one-item list.
        if isinstance(payload, list):
            if len(payload) == 1 and isinstance(payload[0], dict):
                payload = payload[0]
            elif payload and all(
                isinstance(item, dict) and item.get("type") == "Feature"
                for item in payload
            ):
                return payload

        if not isinstance(payload, dict):
            raise ValueError(
                f"Expected USGS GeoJSON object, got {type(payload).__name__}"
            )

        features = payload.get("features")

        if not isinstance(features, list):
            raise ValueError("USGS response does not contain a features list")

        return features

    @map
    def normalize_event(feature):
        from datetime import datetime, timezone

        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or [None, None, None]

        magnitude = properties.get("mag")
        magnitude = float(magnitude) if magnitude is not None else None

        def iso_time(milliseconds):
            if milliseconds is None:
                return None

            return datetime.fromtimestamp(
                milliseconds / 1000,
                tz=timezone.utc,
            ).isoformat()

        alert = properties.get("alert")
        felt_reports = int(properties.get("felt") or 0)
        tsunami = bool(properties.get("tsunami"))

        if alert in {"red", "orange"}:
            priority = "critical"
        elif alert == "yellow" or tsunami or (magnitude or 0) >= 6:
            priority = "high"
        elif (magnitude or 0) >= 4.5 or felt_reports >= 100:
            priority = "medium"
        else:
            priority = "low"

        return {
            "event_id": feature.get("id"),
            "occurred_at_utc": iso_time(properties.get("time")),
            "updated_at_utc": iso_time(properties.get("updated")),
            "magnitude": magnitude,
            "magnitude_type": properties.get("magType"),
            "place": properties.get("place"),
            "longitude": coordinates[0] if len(coordinates) > 0 else None,
            "latitude": coordinates[1] if len(coordinates) > 1 else None,
            "depth_km": coordinates[2] if len(coordinates) > 2 else None,
            "felt_reports": felt_reports,
            "alert": alert or "none",
            "tsunami": tsunami,
            "status": properties.get("status"),
            "event_type": properties.get("type"),
            "priority": priority,
            "details_url": properties.get("url"),
        }

    @validate("Valid USGS event records")
    def validate_events(rows):
        invalid = [
            row
            for row in rows
            if not row.get("event_id")
            or row.get("magnitude") is None
            or row.get("latitude") is None
            or row.get("longitude") is None
        ]

        return (
            bool(rows) and not invalid,
            f"Received {len(rows)} events; {len(invalid)} invalid records",
        )

    @filter
    def deserves_attention(event):
        return (
            event["priority"] in {"critical", "high", "medium"}
            or event["alert"] != "none"
            or event["tsunami"]
        )

    csv_report = sink_file(
        "Publish CSV Report",
        path="/tmp/global_earthquake_watch.csv",
        format="csv",
    )

    json_report = sink_file(
        "Publish JSON Report",
        path="/tmp/global_earthquake_watch.json",
        format="json",
    )

    (
        raw_feed
        >> extract_features
        >> normalize_event
        >> validate_events
        >> deserves_attention
        >> [csv_report, json_report]
    )