"""
Example 6: Paginated API Ingestion
──────────────────────────────────
Pull an entire multi-page collection from a REST API in one node.
You describe *how* the API paginates; Brokoli fans the source out into
one fetch instance per page and concatenates the results.

Showcases: source_api(pagination=...), numbered_pages, and — in the
comments below — the other four strategies the SDK ships: offset_pages,
cursor_pages, next_link_pages, link_header_pages.

The runnable pipeline uses the Rick & Morty API (public, no auth),
which paginates by page number and reports the page count at
``info.pages`` — a textbook fit for numbered_pages().
"""

from brokoli import (
    Pipeline,
    source_api,
    map,
    filter,
    sink_file,
    numbered_pages,
    # offset_pages, cursor_pages, next_link_pages, link_header_pages
)

with Pipeline(
    "paginated-characters",
    description="Fetch every page of a paginated API and export",
) as p:

    # numbered_pages: the API takes ?page=N and tells us how many pages
    # exist at info.pages. Brokoli reads that count at runtime and issues
    # one fetch per page — no hand-rolled loop, no off-by-one.
    characters = source_api(
        "Fetch Characters",
        url="https://rickandmortyapi.com/api/character",
        records="results",           # the array lives at response["results"]
        timeout=15,
        pagination=numbered_pages(
            page_param="page",
            start=1,
            total_pages_path="info.pages",
        ),
    )

    @map
    def slim(row):
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "status": row.get("status"),
            "species": row.get("species"),
        }

    @filter
    def alive(row):
        return row.get("status") == "Alive"

    export = sink_file(
        "Save CSV", path="/data/characters_alive.csv", format="csv"
    )

    characters >> slim >> alive >> export


# ─────────────────────────────────────────────────────────────────────
# The other four pagination strategies — swap in for `pagination=` above.
# Each compiles to the same paginated source_api; only the "how do I get
# the next page" contract differs.
#
#   offset_pages(page_size=100, offset_param="offset", limit_param="limit")
#       ?offset=0&limit=100, then ?offset=100&limit=100, …
#       Stop with max_records=… or end_flag="<json.path.to.done>".
#
#   cursor_pages(cursor_path="meta.next_cursor", cursor_param="cursor")
#       Read the next cursor out of each response body and pass it back
#       as ?cursor=…; stop when the path yields nothing.
#
#   next_link_pages(next_path="links.next")
#       The response carries the *full URL* of the next page in its body
#       (JSON:API style). Follow it until absent.
#
#   link_header_pages(rel="next")
#       The next page's URL is in the HTTP `Link:` header (GitHub style).
#       Follow rel="next" until the header stops offering one.
# ─────────────────────────────────────────────────────────────────────
