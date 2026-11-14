# Releasing the worker

A `vX.Y.Z` tag publishes `ghcr.io/molecularfoundrycrucible/crucible-ingestion-worker:X.Y.Z` (and `:X.Y` for a final release) through `.github/workflows/release-image.yml`. The workflow checks that the tag equals the `version` in `pyproject.toml`, runs the tests, and builds the image from `uv.lock`. Run it by hand from the Actions tab for a build-only dry run; a manual run never publishes.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), like crucible-api.

- Add an entry under `Unreleased` in the same change, for anything an operator notices: settings, image, job handling. Internal changes get no entry.
- Each entry is one short line stating what changed. No rationale, background or narration.
- Sections, in this order, only when used: `Breaking changes`, `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`.
- At release, each version starts with a one-line summary of the release, ending with the bundled crucible-ingestion version.

## Cutting a release

1. Pick `X.Y.Z` (semver, independent of crucible-ingestion's version).
2. Set `version = "X.Y.Z"` in `pyproject.toml` and run `uv lock`.
3. In `CHANGELOG.md`, add `## [X.Y.Z] - YYYY-MM-DD` under `Unreleased` with the summary line, and update the comparison links at the bottom.
4. Commit as `release: X.Y.Z`, tag, and push:

   ```bash
   git tag -a vX.Y.Z -m "X.Y.Z"
   git push origin main vX.Y.Z
   ```

5. When the workflow finishes, create the GitHub release with the changelog section as its notes:

   ```bash
   awk '/^## \[X.Y.Z\]/{f=1;next} /^(## |\[)/{f=0} f' CHANGELOG.md > notes.md
   gh release create vX.Y.Z --verify-tag --title vX.Y.Z --notes-file notes.md
   ```

6. Check the published digest (in the workflow summary) and its labels, `org.opencontainers.image.version` and `gov.lbl.crucible.ingestion.version`.

Deploying a release is the deployment's own step; the Molecular Foundry pins the digest in mf-crucible-deploy.
