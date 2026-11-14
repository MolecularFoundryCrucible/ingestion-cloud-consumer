# Changelog

This changelog records changes that affect operators of the worker: its configuration, its image, and how it handles jobs. Add entries under `Unreleased` in the same change. Internal-only changes do not require an entry.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each release is published as `ghcr.io/molecularfoundrycrucible/crucible-ingestion-worker:X.Y.Z` from a `vX.Y.Z` tag; the bundled crucible-ingestion version is in the image label `gov.lbl.crucible.ingestion.version`.

## [Unreleased]

## [0.3.0] - 2026-11-13

### Added

- The worker reads files straight from Google Cloud Storage or an S3-compatible store when no mount covers the job's bucket: `STORAGE_BACKEND` (`gcs` or `s3`), `STORAGE_BUCKETS` (the buckets it may read), and for S3 `S3_ENDPOINT_URL`, `S3_REGION`, `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`. GCS uses Application Default Credentials (Workload Identity on GKE); S3 without a key uses the platform's credentials. The file is downloaded to `WORK_DIR` (default `/tmp/work`), which must hold the largest file of the tier, and deleted after the job. A configured mount still takes precedence.
- A redelivered job whose ingestion request is already `complete` is acknowledged without running again, so a worker stopped between reporting and acknowledging no longer adds duplicate thumbnails. Requests in any other status run again.

### Changed

- `STORAGE_EXISTS_CHECK=gcs` checks objects through obstore, with the same Application Default Credentials. The worker no longer depends directly on `google-cloud-storage` or `google-auth`.
- The image keeps matplotlib's and fontconfig's caches under `/tmp` and creates hyperspy's settings file at build time, so it runs on a read-only root filesystem as long as `/tmp` is writable.

## [0.2.0] - 2026-11-13

First release from this repository, renamed from `ingestion-cloud-consumer`. It runs the same crucible-ingestion parsers on the same queues and message format.

### Breaking changes for operators

- Configuration comes only from environment variables, and nothing names a Foundry host, bucket or secret: `CRUCIBLE_API_URL`, `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_VHOST` and `QUEUE` replace `RMQ_HOST`, `RMQ_PORT`, `RMQ_ROUTING_SUFFIX`, `SECRET_STORE` and `GCS_CLIENT_ID`.
- Secrets (`CRUCIBLE_API_KEY`, `RABBITMQ_PASSWORD`) are given exactly one way: the value, `<NAME>_FILE` with a file path, or `<NAME>_VERSION` with a Secret Manager version path. Giving one more than one way stops the worker.
- `STORAGE_MOUNTS` maps each bucket to the directory where the deployment mounts it, replacing the built-in mounts; `STORAGE_EXISTS_CHECK=gcs` keeps the check that works around a stale gcsfuse listing.
- The image runs as an unprivileged user (uid 10001) and installs crucible-ingestion 0.2.0 from PyPI, locked in `uv.lock`, instead of following its main branch.

### Added

- Each job runs in its own thread, so the RabbitMQ connection keeps sending heartbeats during long jobs instead of dropping and redelivering them.
- The worker takes one job at a time (`PREFETCH_COUNT`, default 1).
- On SIGTERM the worker stops taking jobs and lets the running one finish within `SHUTDOWN_TIMEOUT` (default 25 seconds); a job still running after that is redelivered.
- The worker touches `HEALTH_FILE` (default `/tmp/worker-alive`) every `HEALTH_INTERVAL` seconds while connected, for a liveness probe.
- A message that cannot be read, or that fails in an unexpected way, moves to the failed queue instead of crashing the worker.
- Images are published to ghcr from version tags, with the crucible-ingestion version as a label; `CRUCIBLE_INGESTION_SPEC` builds against another crucible-ingestion for testing.

[Unreleased]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/releases/tag/v0.2.0
