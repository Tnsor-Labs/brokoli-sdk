"""
Example 7: Fan-out with parallel(), Fan-in with union()
───────────────────────────────────────────────────────
Two shapes the DAG lets you express that a linear chain can't:

  • parallel(a, b, …)  — run independent branches off one upstream
    concurrently, then chain the group into a shared downstream.
  • union("…", a, b, …) — combine several dataset branches back into
    a single dataset for one downstream to consume.

Showcases: source_api, parallel, transform, union, sink_db, sink_file.

Note on union(): the SDK compiles a proper `union` IR node today, but
the *runtime* merge of dataset manifests is not implemented server-side
yet (brokoli-sdk#2). This pipeline deploys and validates; the union node
will start actually concatenating once the backend lands it. It's here
so the authoring surface is documented and exercised — a named,
compile-time contract rather than a silent gap.
"""

from brokoli import (
    Pipeline,
    source_api,
    transform,
    parallel,
    union,
    sink_db,
    sink_file,
)

with Pipeline(
    "fan-out-fan-in",
    description="Split one source into parallel branches, then reunite",
) as p:

    posts = source_api(
        "Fetch Posts",
        url="https://jsonplaceholder.typicode.com/posts",
        timeout=15,
    )

    # ── Fan-out: two independent transforms off the same source run
    #    concurrently instead of one-after-the-other. ──
    titles = transform(
        "Titles Only",
        input=posts,
        rules=[{"type": "drop_columns", "columns": ["body"]}],
    )
    bodies = transform(
        "Bodies Only",
        input=posts,
        rules=[{"type": "drop_columns", "columns": ["title"]}],
    )

    # parallel() marks the branches as concurrent and returns a group
    # ref you can chain into a shared downstream.
    archive = sink_file(
        "Archive Both", path="/data/posts_archive.json", format="json"
    )
    parallel(titles, bodies) >> archive

    # ── Fan-in: union() combines branch datasets back into one, for a
    #    single downstream sink. ──
    combined = union("Recombine", titles, bodies)
    combined >> sink_db(
        "Load Combined", table="staging.posts", conn_id="warehouse", mode="append"
    )
