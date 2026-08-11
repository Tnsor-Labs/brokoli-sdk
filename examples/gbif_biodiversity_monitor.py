"""
GBIF Global Biodiversity Monitor
────────────────────────────────
Retrieve up to 30,000 georeferenced biodiversity occurrence records from
GBIF, normalize and validate them, then publish detailed, country, and species
reports.

No API key, database, connection, secret, or local input file is required.

Outputs:
    /tmp/gbif_occurrences.json
    /tmp/gbif_country_summary.csv
    /tmp/gbif_species_summary.csv

Run:
    brokoli deploy examples/gbif_biodiversity_monitor.py \
        --server https://in-brokoli.orkestri.site
"""

from brokoli import (
    Pipeline,
    filter,
    map,
    sink_file,
    source_api,
    task,
    validate,
)


GBIF_API = "https://api.gbif.org/v1/occurrence/search"
PAGE_SIZE = 300
MAX_RECORDS = 30_000

FIRST_PAGE_URL = (
    f"{GBIF_API}"
    f"?hasCoordinate=true"
    f"&occurrenceStatus=PRESENT"
    f"&limit={PAGE_SIZE}"
    f"&offset=0"
)


with Pipeline(
    "gbif-global-biodiversity-monitor",
    description=(
        "Process 30,000 georeferenced biodiversity observations from GBIF "
        "and publish country and species coverage reports"
    ),
    schedule="0 2 * * *",
    tags=["public-data", "biodiversity", "large-data", "gbif"],
) as p:

    first_page = source_api(
        "Fetch First GBIF Page",
        url=FIRST_PAGE_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": "Brokoli-GBIF-Monitor/1.0",
        },
        retries=3,
        timeout=60,
    )

    @task("Retrieve 30K Occurrences", retries=2, timeout=900)
    def retrieve_occurrences(first_response):
        import json
        import time
        import urllib.error
        import urllib.request

        gbif_api = "https://api.gbif.org/v1/occurrence/search"
        page_size = 300
        max_records = 30_000

        if (
            isinstance(first_response, list)
            and len(first_response) == 1
            and isinstance(first_response[0], dict)
            and "results" in first_response[0]
        ):
            first_response = first_response[0]

        if isinstance(first_response, dict):
            records = list(first_response.get("results") or [])
            available = int(
                first_response.get("count") or max_records
            )
        elif isinstance(first_response, list):
            records = list(first_response)
            available = max_records
        else:
            raise ValueError(
                "GBIF source returned an unsupported response type: "
                f"{type(first_response).__name__}"
            )

        target = min(max_records, available)

        seen = {
            str(record.get("key"))
            for record in records
            if record.get("key") is not None
        }

        for offset in range(page_size, target, page_size):
            url = (
                f"{gbif_api}"
                f"?hasCoordinate=true"
                f"&occurrenceStatus=PRESENT"
                f"&limit={page_size}"
                f"&offset={offset}"
            )

            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "Brokoli-GBIF-Monitor/1.0",
                },
            )

            page = None

            for attempt in range(5):
                try:
                    with urllib.request.urlopen(
                        request,
                        timeout=30,
                    ) as response:
                        page = json.loads(
                            response.read().decode("utf-8")
                        )
                    break

                except urllib.error.HTTPError as error:
                    if error.code != 429 or attempt == 4:
                        raise

                    time.sleep(2 ** attempt)

            if not page:
                break

            page_records = page.get("results") or []

            if not page_records:
                break

            for record in page_records:
                key = record.get("key")

                if key is None:
                    continue

                key = str(key)

                if key not in seen:
                    seen.add(key)
                    records.append(record)

            if page.get("endOfRecords"):
                break

            time.sleep(0.15)

        return records[:target]

    @map
    def normalize_occurrence(record):
        media = record.get("media") or []
        image_url = ""

        for item in media:
            if item.get("type") == "StillImage":
                image_url = item.get("identifier") or ""
                break

        issues = record.get("issues") or []

        return {
            "gbif_id": record.get("key"),
            "scientific_name": record.get("scientificName"),
            "accepted_name": record.get("acceptedScientificName"),
            "taxon_rank": record.get("taxonRank"),
            "kingdom": record.get("kingdom"),
            "phylum": record.get("phylum"),
            "class_name": record.get("class"),
            "order_name": record.get("order"),
            "family": record.get("family"),
            "genus": record.get("genus"),
            "species": record.get("species"),
            "country": record.get("country"),
            "country_code": record.get("countryCode"),
            "state_province": record.get("stateProvince"),
            "locality": record.get("locality"),
            "latitude": record.get("decimalLatitude"),
            "longitude": record.get("decimalLongitude"),
            "coordinate_uncertainty_m": record.get(
                "coordinateUncertaintyInMeters"
            ),
            "event_date": record.get("eventDate"),
            "year": record.get("year"),
            "basis_of_record": record.get("basisOfRecord"),
            "dataset_title": record.get("datasetTitle"),
            "publisher": record.get("publishingOrgKey"),
            "license": record.get("license"),
            "has_image": bool(image_url),
            "image_url": image_url,
            "issue_count": len(issues),
            "issues": ",".join(issues),
        }

    @filter
    def usable_occurrence(row):
        latitude = row.get("latitude")
        longitude = row.get("longitude")

        if row.get("gbif_id") is None:
            return False

        if latitude is None or longitude is None:
            return False

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except (TypeError, ValueError):
            return False

        return (
            -90 <= latitude <= 90
            and -180 <= longitude <= 180
        )

    @validate("Large Biodiversity Dataset")
    def validate_dataset(rows):
        unique_ids = {
            row.get("gbif_id")
            for row in rows
            if row.get("gbif_id") is not None
        }

        countries = {
            row.get("country_code")
            for row in rows
            if row.get("country_code")
        }

        passed = (
            len(rows) >= 10_000
            and len(unique_ids) == len(rows)
            and len(countries) >= 20
        )

        return (
            passed,
            f"{len(rows)} records, "
            f"{len(unique_ids)} unique IDs, "
            f"{len(countries)} countries",
        )

    @task("Summarize by Country")
    def summarize_countries(rows):
        summary = {}

        for row in rows:
            code = row.get("country_code") or "UNKNOWN"

            item = summary.setdefault(
                code,
                {
                    "country_code": code,
                    "country": row.get("country") or "Unknown",
                    "occurrence_count": 0,
                    "species": set(),
                    "datasets": set(),
                    "records_with_images": 0,
                    "records_with_issues": 0,
                },
            )

            item["occurrence_count"] += 1

            if row.get("species"):
                item["species"].add(row["species"])

            if row.get("dataset_title"):
                item["datasets"].add(row["dataset_title"])

            if row.get("has_image"):
                item["records_with_images"] += 1

            if row.get("issue_count", 0) > 0:
                item["records_with_issues"] += 1

        output = []

        for item in summary.values():
            count = item["occurrence_count"]

            output.append(
                {
                    "country_code": item["country_code"],
                    "country": item["country"],
                    "occurrence_count": count,
                    "unique_species": len(item["species"]),
                    "contributing_datasets": len(item["datasets"]),
                    "records_with_images": item[
                        "records_with_images"
                    ],
                    "records_with_issues": item[
                        "records_with_issues"
                    ],
                    "image_coverage_pct": round(
                        item["records_with_images"] * 100 / count,
                        2,
                    ),
                    "issue_rate_pct": round(
                        item["records_with_issues"] * 100 / count,
                        2,
                    ),
                }
            )

        return sorted(
            output,
            key=lambda item: item["occurrence_count"],
            reverse=True,
        )

    @task("Summarize by Species")
    def summarize_species(rows):
        summary = {}

        for row in rows:
            species = (
                row.get("species")
                or row.get("scientific_name")
            )

            if not species:
                continue

            item = summary.setdefault(
                species,
                {
                    "scientific_name": species,
                    "kingdom": row.get("kingdom"),
                    "class_name": row.get("class_name"),
                    "family": row.get("family"),
                    "occurrence_count": 0,
                    "countries": set(),
                    "datasets": set(),
                    "records_with_images": 0,
                },
            )

            item["occurrence_count"] += 1

            if row.get("country_code"):
                item["countries"].add(row["country_code"])

            if row.get("dataset_title"):
                item["datasets"].add(row["dataset_title"])

            if row.get("has_image"):
                item["records_with_images"] += 1

        output = []

        for item in summary.values():
            count = item["occurrence_count"]

            output.append(
                {
                    "scientific_name": item["scientific_name"],
                    "kingdom": item["kingdom"],
                    "class_name": item["class_name"],
                    "family": item["family"],
                    "occurrence_count": count,
                    "country_count": len(item["countries"]),
                    "contributing_datasets": len(item["datasets"]),
                    "records_with_images": item[
                        "records_with_images"
                    ],
                    "image_coverage_pct": round(
                        item["records_with_images"] * 100 / count,
                        2,
                    ),
                }
            )

        return sorted(
            output,
            key=lambda item: item["occurrence_count"],
            reverse=True,
        )

    raw_export = sink_file(
        "Publish Occurrence Dataset",
        path="/tmp/gbif_occurrences.json",
        format="json",
    )

    country_export = sink_file(
        "Publish Country Summary",
        path="/tmp/gbif_country_summary.csv",
        format="csv",
    )

    species_export = sink_file(
        "Publish Species Summary",
        path="/tmp/gbif_species_summary.csv",
        format="csv",
    )

    occurrences = retrieve_occurrences(first_page)
    normalized = normalize_occurrence(occurrences)
    usable = usable_occurrence(normalized)
    validated = validate_dataset(usable)

    country_summary = summarize_countries(validated)
    species_summary = summarize_species(validated)

    validated >> raw_export
    country_summary >> country_export
    species_summary >> species_export
