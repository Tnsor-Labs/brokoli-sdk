"""brokoli-sdk#15 M2: the authoring context is nesting-, thread-, and
async-safe.

`with Pipeline(...)` used to set a single class attribute and null it on
exit. That corrupted three cases the issue calls out:

  * nested `with` blocks -- the inner block's exit lost the outer;
  * concurrent authoring in threads -- one build clobbered another's;
  * concurrent authoring in asyncio tasks -- same, across a single thread.

The context is now a contextvars.ContextVar with save/restore tokens, so
each thread and each task sees only its own in-progress pipeline, and
nested blocks restore the enclosing one.
"""

import asyncio
import threading

from brokoli import Pipeline, source_api


def _names(p):
    return {n["name"] for n in p.to_json()["nodes"]}


class TestNesting:
    def test_inner_block_restores_outer_on_exit(self):
        with Pipeline("outer") as outer:
            source_api("O1", url="https://x")

            with Pipeline("inner") as inner:
                source_api("I1", url="https://x")

                assert Pipeline.current() is inner

            # The bug: exit used to set the context to None, so this
            # registration would raise instead of landing in `outer`.
            assert Pipeline.current() is outer

            source_api("O2", url="https://x")

        assert Pipeline.current() is None
        assert _names(outer) == {"O1", "O2"}
        assert _names(inner) == {"I1"}


class TestThreadIsolation:
    def test_concurrent_builds_do_not_cross_contaminate(self):
        barrier = threading.Barrier(2)
        results: dict[str, set] = {}

        def build(tag):
            with Pipeline(tag) as p:
                source_api(f"{tag}-A", url="https://x")

                # Both threads are now simultaneously inside their own
                # `with` block -- the exact interleaving a shared global
                # would corrupt.
                barrier.wait()

                source_api(f"{tag}-B", url="https://x")

                results[tag] = _names(p)

        t1 = threading.Thread(target=build, args=("t1",))
        t2 = threading.Thread(target=build, args=("t2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert results["t1"] == {"t1-A", "t1-B"}
        assert results["t2"] == {"t2-A", "t2-B"}


class TestAsyncIsolation:
    def test_concurrent_tasks_do_not_cross_contaminate(self):
        async def build(tag, mine, theirs):
            with Pipeline(tag) as p:
                source_api(f"{tag}-A", url="https://x")

                # Hand off so the sibling task authors while we're
                # suspended mid-block -- both are "inside" at once.
                mine.set()
                await theirs.wait()

                source_api(f"{tag}-B", url="https://x")

            return _names(p)

        async def main():
            e1, e2 = asyncio.Event(), asyncio.Event()
            return await asyncio.gather(
                build("a1", e1, e2),
                build("a2", e2, e1),
            )

        r1, r2 = asyncio.run(main())
        assert r1 == {"a1-A", "a1-B"}
        assert r2 == {"a2-A", "a2-B"}
