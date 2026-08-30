# Contributing to the Brokoli SDK

The canonical contributing guide lives in the core repo:
[`Tnsor-Labs/brokoli/CONTRIBUTING.md`](https://github.com/Tnsor-Labs/brokoli/blob/main/CONTRIBUTING.md).
Everything there about claiming an issue, branch/commit conventions, and
the review norm applies here too. This file only covers what's specific
to this repo.

## What's different here

- **No CI today.** That's a real gap, not an oversight — run `pytest`
  locally before opening a PR (`pip install -e .` then `pytest`).
- **No git tags yet.** `pyproject.toml` is currently pinned at a static
  `0.2.0`. This repo's first real tag lands the cycle it needs one — see
  [`RELEASING.md`](https://github.com/Tnsor-Labs/brokoli/blob/main/RELEASING.md)
  in the core repo for how cadence-named milestones (`2026-08 cycle`
  etc.) work for repos that don't yet track `brokoli` core's semver.
- **Keep the SDK and the backend honest with each other.** If you're
  changing anything that crosses the SDK/API boundary (deploy payloads,
  pipeline IR, capability checks), check it against the current
  `GET /api/capabilities` shape in `Tnsor-Labs/brokoli` — a change here
  that silently drifts from what the backend actually accepts is the
  kind of bug that's invisible until a real pipeline deploy fails.

## Tests

```
pip install -e .
pytest
```
