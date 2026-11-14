# Crucible ingestion worker

Runs [crucible-ingestion](https://github.com/MolecularFoundryCrucible/crucible-ingestion) parsers on files uploaded to a [Crucible](https://github.com/MolecularFoundryCrucible/crucible-api) server. When someone requests ingestion of a file, the API queues a job in RabbitMQ; the worker takes the job, reads the file, extracts its scientific metadata and thumbnails, and writes them back through the API. Everything it writes goes through the API; it only ever reads from storage.

```
upload ──▶ Crucible API ──job──▶ RabbitMQ ──▶ worker ──reads──▶ file store
               ▲                                 │
               └──── metadata, thumbnails, status┘
```

A full quick start for a single-host installation is not written yet. This page covers the image, the configuration and how the worker reaches files. [docs/design.md](docs/design.md) explains the design, and [CHANGELOG.md](CHANGELOG.md) lists changes by release.

## Image

```
ghcr.io/molecularfoundrycrucible/crucible-ingestion-worker:X.Y.Z
```

Releases are tagged `X.Y.Z` and `X.Y`. The label `gov.lbl.crucible.ingestion.version` gives the bundled crucible-ingestion version, which the worker also logs at startup. Pin the image by digest in production.

The image runs as uid 10001. It works on a read-only root filesystem as long as `/tmp` is writable, and `/tmp` (or `WORK_DIR`) needs room for the largest file the worker downloads.

## How the worker reaches files

A job names its file as `bucket/key`. For each job the worker uses the first of these that applies:

1. **A mounted directory.** `STORAGE_MOUNTS` maps the bucket to a directory where the deployment mounts it (the Cloud Storage FUSE driver on GKE, the Mountpoint for Amazon S3 driver, or a host mount). The worker reads the file in place.
2. **The store directly.** `STORAGE_BACKEND` is `gcs` or `s3` and the bucket is listed in `STORAGE_BUCKETS`. The worker downloads the file to `WORK_DIR` under its original name, runs the parsers on it, and deletes it after the job. Google Cloud Storage uses Application Default Credentials (Workload Identity on GKE). An S3-compatible store uses `S3_ENDPOINT_URL` and a key, or the platform's credentials when no key is given.

A file in any other bucket is refused and reported as `file not found`. A missing file is retried with backoff before it is reported. A storage error such as a wrong key ends the job as `failed`.

## Configuration

Configuration comes only from environment variables. Each secret is given exactly one way: as the variable itself, as `<NAME>_FILE` with the path of a file holding it (Docker and Kubernetes secrets), or as `<NAME>_VERSION` with a Google Secret Manager version path.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CRUCIBLE_API_URL` | required | API base URL, for example `https://crucible.example.org/api/v3` |
| `CRUCIBLE_API_KEY` | required, secret | API key the worker acts as |
| `RABBITMQ_HOST` | required | Broker host |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_USER` | `guest` | Broker user |
| `RABBITMQ_PASSWORD` | required, secret | Broker password |
| `RABBITMQ_VHOST` | `/` | Broker virtual host |
| `RABBITMQ_HEARTBEAT` | `60` | Connection heartbeat, in seconds |
| `QUEUE` | required | Queue to consume, for example `ingestion-main-small` |
| `FAILED_QUEUE` | `<QUEUE>-failed` | Where failed jobs are copied |
| `NOT_SUPPORTED_QUEUE` | `not-supported` | Where unsupported files are copied; empty to turn off |
| `PREFETCH_COUNT` | `1` | Jobs held at once |
| `STORAGE_MOUNTS` | empty | JSON object of bucket to directory, for example `{"crucible-files": "/mnt/files"}` |
| `STORAGE_EXISTS_CHECK` | empty | `gcs` to confirm a mounted file in GCS when the mount's listing may be stale |
| `STORAGE_BACKEND` | empty | `gcs` or `s3` to read files straight from the store |
| `STORAGE_BUCKETS` | required with a backend | Comma-separated buckets the worker may read directly |
| `S3_ENDPOINT_URL` | empty | S3-compatible endpoint, for example `http://seaweedfs:8333`; empty for AWS |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ACCESS_KEY_ID` | empty, secret | S3 key ID, given together with the secret key |
| `S3_SECRET_ACCESS_KEY` | empty, secret | S3 secret key |
| `WORK_DIR` | `/tmp/work` | Where downloaded files are kept during a job |
| `SHUTDOWN_TIMEOUT` | `25` | Seconds a running job may finish after SIGTERM |
| `HEALTH_FILE` | `/tmp/worker-alive` | File touched while connected; empty to turn off |
| `HEALTH_INTERVAL` | `15` | Seconds between touches |
| `LOG_LEVEL` | `INFO` | Log level |

## Running

One worker against an existing API and RabbitMQ, reading an S3-compatible store with keys from files:

```
docker run -d --name ingestion-worker \
  -e CRUCIBLE_API_URL=https://crucible.example.org/api/v3 \
  -e CRUCIBLE_API_KEY_FILE=/run/secrets/api_key \
  -e RABBITMQ_HOST=rabbitmq.example.org \
  -e RABBITMQ_USER=ingestion \
  -e RABBITMQ_PASSWORD_FILE=/run/secrets/rabbitmq_password \
  -e QUEUE=ingestion-main-small \
  -e STORAGE_BACKEND=s3 \
  -e STORAGE_BUCKETS=crucible-files \
  -e S3_ENDPOINT_URL=https://s3.example.org \
  -e S3_ACCESS_KEY_ID_FILE=/run/secrets/s3_access_key_id \
  -e S3_SECRET_ACCESS_KEY_FILE=/run/secrets/s3_secret_access_key \
  -v "$PWD/secrets:/run/secrets:ro" \
  --restart unless-stopped \
  ghcr.io/molecularfoundrycrucible/crucible-ingestion-worker:X.Y.Z
```

The worker exits when it loses the broker, so run it under something that restarts it (`--restart unless-stopped`, or a Kubernetes Deployment).

On Kubernetes, check liveness with the heartbeat file's age, which needs no extra binaries:

```
livenessProbe:
  exec:
    command: ["sh", "-c", "test $(( $(date +%s) - $(stat -c %Y /tmp/worker-alive) )) -lt 120"]
  initialDelaySeconds: 60
  periodSeconds: 30
```

Give the pod a termination grace period longer than `SHUTDOWN_TIMEOUT`, and longer than the slowest job you want to finish rather than redeliver.

## Development

```
uv sync
uv run pytest
```

`CRUCIBLE_INGESTION_SPEC` builds the image against another crucible-ingestion, such as a pre-release or a branch: `docker build --build-arg CRUCIBLE_INGESTION_SPEC=crucible-ingestion==0.3.0a1 .`. Releases are built from `uv.lock`.
