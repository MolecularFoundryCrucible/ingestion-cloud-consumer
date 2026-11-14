# Changelog

Changes that affect operators of the worker. Add entries under `Unreleased` in the same change.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-11-13

Direct reads from GCS or S3, no rerun of completed jobs, and a read-only root filesystem. crucible-ingestion 0.2.0.

### Added

- Direct reads from GCS or an S3-compatible store when no mount covers the bucket: `STORAGE_BACKEND`, `STORAGE_BUCKETS`, `S3_ENDPOINT_URL`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`. Files download to `WORK_DIR` (default `/tmp/work`).
- A redelivered job whose request is already `complete` is acknowledged without running.

### Changed

- `STORAGE_EXISTS_CHECK=gcs` uses obstore; `google-cloud-storage` and `google-auth` are no longer direct dependencies.
- The image runs on a read-only root filesystem; `/tmp` must be writable.

## [0.2.0] - 2026-11-13

First release as `crucible-ingestion-worker`: generic configuration, threaded jobs, graceful shutdown, published to ghcr. crucible-ingestion 0.2.0.

### Breaking changes

- `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_VHOST` and `QUEUE` replace `RMQ_HOST`, `RMQ_PORT`, `RMQ_ROUTING_SUFFIX`, `SECRET_STORE` and `GCS_CLIENT_ID`.
- Secrets `CRUCIBLE_API_KEY` and `RABBITMQ_PASSWORD` are given exactly one way: the value, `<NAME>_FILE` or `<NAME>_VERSION` (Secret Manager).
- `STORAGE_MOUNTS` replaces the built-in Foundry mounts.
- Runs as uid 10001.
- crucible-ingestion comes from PyPI, locked in `uv.lock`.

### Added

- Jobs run in a thread, so long jobs no longer drop the RabbitMQ connection.
- `PREFETCH_COUNT` (default 1), `SHUTDOWN_TIMEOUT` (default 25 s), `HEALTH_FILE` and `HEALTH_INTERVAL`.
- Unreadable or crashing messages move to the failed queue.
- Images on ghcr from version tags, with the crucible-ingestion version in the label `gov.lbl.crucible.ingestion.version`; `CRUCIBLE_INGESTION_SPEC` builds against another crucible-ingestion.

[Unreleased]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/MolecularFoundryCrucible/crucible-ingestion-worker/releases/tag/v0.2.0
