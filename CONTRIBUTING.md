# Contributing to the Brokoli SDK

The canonical contributing guide lives in the core repo:
[`Tnsor-Labs/brokoli/CONTRIBUTING.md`](https://github.com/Tnsor-Labs/brokoli/blob/main/CONTRIBUTING.md).
Everything there about claiming an issue, branch/commit conventions, and
the review norm applies here too. This file only covers what's specific
to this repo.

## What's different here

- **CI runs the test matrix, a build/package gate, and an examples
  compile-check on every PR** (`.github/workflows/ci.yml`) — Python
  3.9-3.13. Lint and mypy run too, advisory-only for now (pre-existing
  code predates both; gating would be a red wall on day one).
- **Releases are real git tags now**, `v0.3.0` onward — see "Releasing"
  below. `pyproject.toml`'s version is bumped by hand as part of cutting
  each one; there's no automated version-bump tooling yet.
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

## Releasing

Pushing a `vX.Y.Z` tag on `main` triggers `.github/workflows/release.yml`:
test → build the wheel/sdist → publish to PyPI. There is no manual
`twine upload` path and no long-lived PyPI token stored anywhere —
publishing uses PyPI's **trusted publishing** (OIDC): PyPI trusts the
GitHub Actions workflow itself, scoped to one repo, one workflow file,
and one GitHub *environment* name, with no secret to leak or rotate.

**One-time setup** (needed once, before the very first tag push can
publish):

1. On PyPI, go to [Publishing → Add a new pending
   publisher](https://pypi.org/manage/account/publishing/) (the project
   `brokoli` doesn't exist on PyPI yet, so this is a *pending* publisher,
   not a per-project setting).
2. Fill in exactly:
   - **PyPI Project Name:** `brokoli`
   - **Owner:** `Tnsor-Labs`
   - **Repository name:** `brokoli-sdk`
   - **Workflow name:** `release.yml`
   - **Environment name:** `pypi`
3. The first successful publish from that workflow creates the `brokoli`
   project on PyPI automatically, already configured for trusted
   publishing going forward — no further setup after that.

**Cutting a release**, once the above is done:

1. Bump `version` in `pyproject.toml`.
2. Move `CHANGELOG.md`'s `## Unreleased` section under a new dated
   version heading (`## 0.5.0 — 2026-08-19`), and add a fresh empty
   `## Unreleased` above it.
3. Commit, merge to `main`.
4. `git tag vX.Y.Z && git push origin vX.Y.Z` — this alone triggers the
   publish; nothing else to run by hand.
5. Curate a GitHub release from the tag (`gh release create vX.Y.Z
   --notes-file ...`) — release notes are written by hand from the
   CHANGELOG entry, same as `v0.3.0`/`v0.4.0`; this step is **not**
   automated by the workflow.
