# crucible-ingestion-worker: design

Status: proposed, 2026-11-13. Not yet implemented. This repository is being renamed from `ingestion-cloud-consumer` to `crucible-ingestion-worker`.

## Purpose

The ingestion worker is the Crucible component that turns uploaded files into dataset metadata. The Crucible API queues an ingestion job on RabbitMQ; the worker takes it, reads the file, runs the matching parser from the `crucible-ingestion` library, writes the result to the dataset through the API, and reports the job's status back to the API.

It must be a generic, versioned component of a Crucible deployment, like the API and the Hub: anyone can run it next to their own Crucible, on any storage the API supports (S3-compatible stores such as SeaweedFS, or Google Cloud Storage), on a single Docker host or on Kubernetes. Site-specific configuration (the Molecular Foundry's included) lives in each site's deployment, never in this repository.

## Goals

- Runs against any Crucible API and any storage backend the API supports, with no cloud SDK required.
- Reads files fast, and only the parts a parser needs, whether they come from a local path, a mounted directory or a signed link, through one code path.
- Survives long jobs, crashes and redeliveries without losing or silently repeating work.
- Configured entirely through environment variables and secret files; a quick start that a lab can follow in minutes.
- Published as a versioned image built on a pinned `crucible-ingestion` release.

## Non-goals

- A shared library for all workers. The worker is structured so its plumbing can be extracted later (see Multiple workers); extracting it now would be speculative.
- A Helm chart. The image, a Docker Compose overlay and an example Kubernetes manifest are enough to start.
- Owning the job contract, the queue topology or RabbitMQ itself. The API produces the jobs and owns their contract; RabbitMQ is third-party infrastructure.

## How it works today, and what changes

The current code (about 330 lines) works for the Foundry but is tied to it:

- Foundry names are hardcoded: the `crucible-uploads` bucket mapped to `/mnt/gcs`, `mf-storage-prod` as the default bucket mount, the RabbitMQ user `admin`, and Secret Manager paths such as `crucible_admin_apikey/versions/4`.
- Google Cloud is required at import: `google.cloud.storage` checks that files exist, and secrets fall back to Secret Manager.
- The job runs inside the RabbitMQ callback on a `BlockingConnection` with a 60-second heartbeat. A job that blocks longer than the heartbeat timeout stops heartbeats, so the broker can close the connection and redeliver the message while it is still being processed (pika issue #418). No such drop appears in the last 30 days of the Foundry's logs, so today's jobs are short enough; it is a latent risk that remote reads, which make jobs longer, would make real.
- `prefetch_count=10` with one job at a time means one pod can hold ten waiting messages while other pods sit idle, which defeats scaling.
- Failures are copied by hand to `ingestion-<suffix>-failed` on classic queues; a message that crashes the worker is redelivered without limit.
- Upgrades re-resolve the library from git `main` and redeploy automatically.

## Architecture

```
            publishes jobs                    consumes jobs
Crucible API ------------> RabbitMQ <------------------- crucible-ingestion-worker
     ^                    (third party)                     |   |
     |  status updates, dataset metadata, download links    |   |  reads files
     +------------------------------------------------------+   v
                                              local path, mounted directory, or a
                                              signed link, read through fsspec
```

- The API decides which queue a job goes to (`ingestion-<suffix>-<tier>`, tiers by file size: small under 0.5 GiB, medium under 3 GiB, large above) and records each job as an ingestion request.
- Each worker deployment consumes one queue. The size tiers are separate deployments of the same image, differing only in queue name and resources.

## The job contract

The API owns the contract and documents it; the worker implements it. Today's message:

| Field | Meaning |
| --- | --- |
| `reqid` | Ingestion request ID; the worker updates its status |
| `dsid` | Dataset that receives the metadata |
| `filename` | Storage path of the file, `<bucket>/<key>` |
| `storage_generation` | Object generation, when the backend has one |
| `file_name` | Original file name |
| `ingestion_class` | Requested ingestor, or empty for automatic choice |

Production on API 3.3.1 publishes the same shape (`files.py` at commit `b5fa0e5`); a real message looks like `filename: mf-storage-prod/<dataset>/_files/<id>` with `storage_generation`, `file_name`, `reqid`, `dsid` and `ingestion_class`.

Proposed additions on the API side: `file_mfid` (needed to request a download link), and a `contract_version` so a worker can refuse a message it does not understand.

The worker also publishes unsupported files to a shared `not-supported` queue with no environment suffix, so environments sharing a broker share that queue. The API already records the `not supported` status, so the proposal is to stop publishing there (or suffix it if anything consumes it).

Status flow, reported through the API: `started`, then one of `complete`, `failed`, `not supported`, `file not found`. The worker records the `crucible-ingestion` version it ran.

## File access

There is one code path. The worker turns each job into a file reference and hands it to the ingestor; the ingestor reads it through fsspec, which treats local paths, mounted directories and remote URLs alike. What differs between deployments is only how the worker builds the reference:

1. If `STORAGE_MOUNTS` maps the job's bucket to a directory, the reference is that local path. The deployment provides the mount: the Cloud Storage FUSE CSI driver on GKE, the Mountpoint for Amazon S3 CSI driver on other Kubernetes, a host mount (rclone, s3fs, or the store's own FUSE mount) bind-mounted on a single Docker host, or a plain directory. The worker never mounts anything itself.
2. Otherwise, if `STORAGE_BACKEND` is set and the job's bucket is listed in `STORAGE_BUCKETS`, the worker reads the object straight from the store under its own identity: Google Cloud Storage through Application Default Credentials (Workload Identity on GKE), or any S3-compatible store through `S3_ENDPOINT_URL` and a key (or the platform's credentials when no key is given). This is the standard pattern for a service on the same network as the store, and it works on a single host because nothing depends on the server's public address. The setting names match the API's, so a shared deployment hands the worker the same values. Reads use `obstore`, one compiled library for both stores with no Python dependencies; `gcsfs`/`s3fs` were rejected because botocore pins `python-dateutil` below 3, which would downgrade the parsers' environment, and boto3 has the same pin.
3. Later, for a worker outside the store's network: a short-lived signed link from the API (`GET /files/{file_mfid}/download_link`). It needs no storage credentials, but a presigned URL is bound to the host it was signed for, so the server's public address must be a real name the worker can reach; a worker on the same host as a `localhost` installation cannot use it. Not implemented yet.

The bucket allowlist matters for direct reads: a mount limits the worker to what is mounted, but store credentials may reach more, so the worker refuses any bucket not in `STORAGE_BUCKETS` (`file not found`, sent to the failed queue). A missing object is retried like a missing mounted file and ends as `file not found`; any other store error (credentials, endpoint, permissions) ends as `failed`. The object read is the current one: `storage_generation` is not pinned, as with mounts, because an S3 store may report an ETag rather than a version id there.

Today a direct read downloads the whole object to a per-job directory under `WORK_DIR`, named after the basename of the original file name so parsers that check extensions keep working, and deletes it when the job ends, whatever the outcome. `WORK_DIR` therefore has to hold the largest file of the tier. Partial reads arrive with the ingestor input interface below. The target is:

Reads are partial wherever the parser allows it: a parser that opens the reference as a file object reads only the byte ranges it needs, over HTTP range requests or through the mount, which matters because most ingestors read headers out of multi-gigabyte files. Parsers built on libraries that require a real path get one: a local reference is returned as is, and a remote one is downloaded once to `WORK_DIR` through fsspec's local cache and deleted after the job.

Details the worker handles:

- Backslashes in storage paths are normalized, and a path already under a configured directory is used as is, which covers older requests. The Foundry's current behavior is pure configuration: `{"crucible-uploads": "/mnt/gcs", "mf-storage-prod": "/mnt/gcs-prod"}`.
- Signed links expire (one hour today); a longer job requests a fresh link and reopens. The worker passes the file size, known from the API, so fsspec does not need a `HEAD` request that a link signed for `GET` may refuse.
- HTTP reads use fsspec's block cache, with a block size tuned for the scattered small reads HDF5 makes; without it, each read becomes its own request.
- FUSE mounts cache metadata, so a just-uploaded file can appear late. The worker retries a missing file with backoff before reporting `file not found`, and the retry window must exceed the mount's metadata cache lifetime. Today the worker works around a stale cache with a GCS `exists()` call; the Foundry's volumes mount with only `implicit-dirs`, so gcsfuse's default metadata cache applies, while the current retries add up to about 30 seconds. Removing the GCS call therefore requires an explicit change in mf-crucible-deploy first: set the gcsfuse metadata cache lifetimes on the volumes in `crucible-prod/storage/` (checked against the gcsfuse documentation) and size the retry window to match. Signed links read the store directly and avoid the problem altogether.

Whether the Foundry keeps its mounts or moves to signed links is decided by measurement: a header-only read of a large HDF5 file and a large DM file, signed link with block cache against gcsfuse. If they are comparable, the consumers can drop the production storage volumes, which also removes a cross-environment coupling.

## Ingestor input interface (crucible-ingestion)

Every ingestor accepts either a local path or a remote reference, so the same ingestor runs in local tests, in a local deployment, and against signed links in production. The base class gives ingestors two ways to read their input:

- `open(mode="rb")`: a file object through fsspec. For a local path it is the local file; for a URL it reads byte ranges. Readers that accept file objects use this: h5py, PIL, plain file reads, JSON, YAML and XML, which cover most of the 29 ingestors (h5py appears 23 times, PIL 48 times, plain `open()` 67 times).
- `local_path()`: a real filesystem path. A local path is returned unchanged; a remote reference is downloaded once and cached for the job. Readers that need a path use this: hyperspy, ncempy's DM and SER readers, aicspylibczi and gdstk.

Ingestor selection already works from the original file name (`file_name`) rather than the storage key, since signed links and storage keys carry no extension. The change is backward compatible: an ingestor that still reads `self.file_to_upload` as a path keeps working through `local_path()`, at the cost of a full download for remote input, and ingestors move to `open()` one at a time, the heaviest readers (HDF5, images) first. Tests keep passing local paths, and a test fixture serving files over a local HTTP server covers the remote path. The library depends on `fsspec` with its HTTP support; `s3fs` and `gcsfs` stay optional extras for anyone who prefers direct bucket URLs.

## Messaging reliability

Following RabbitMQ's guidance and pika's documented pattern:

- Manual acknowledgements, sent only after the final status update reaches the API.
- `prefetch_count=1`, so each worker holds one job and idle workers get the next one.
- The job runs in a worker thread; the connection thread keeps serving heartbeats and acknowledges through `add_callback_threadsafe`.
- Quorum queues with a delivery limit and a dead-letter queue (`ingestion-<suffix>-<tier>-failed`, at-least-once dead-lettering), replacing the hand-copied failed queue. A message that keeps crashing the worker is parked for inspection instead of looping. Switching a queue from classic to quorum means declaring new queues, so the API and the worker must agree on names and arguments; this is a migration step, not an in-place change.
- Redelivery can run a job twice. The worker skips a request whose status is already final, which covers most redeliveries. A job interrupted mid-way stays `started` and runs again, and the library is only partly idempotent today: child datasets are de-duplicated by name and scientific metadata is merged (`overwrite=False`), but thumbnails are added again on every run, and keywords depend on whether the API de-duplicates them. Making reruns safe is a requirement on `crucible-ingestion` (skip existing thumbnails), not a property it has yet.

## Configuration

Configuration comes from environment variables. Each secret is given exactly one way, as a variable or as `<NAME>_FILE` pointing to a file (which suits Docker secrets, Kubernetes Secrets, and the GKE Secret Manager add-on).

| Variable | Default | Purpose |
| --- | --- | --- |
| `CRUCIBLE_API_URL` | required | Base URL of the API |
| `CRUCIBLE_API_KEY` | required, secret | API key the worker acts as |
| `RABBITMQ_HOST` | required | Broker host |
| `RABBITMQ_PORT` | `5672` | Broker port |
| `RABBITMQ_USER` | `guest` | Broker user |
| `RABBITMQ_VHOST` | `/` | Broker virtual host |
| `RABBITMQ_PASSWORD` | required, secret | Broker password |
| `QUEUE` | required | Queue to consume, for example `ingestion-main-small` |
| `STORAGE_MOUNTS` | empty | JSON map of bucket to directory; a mount wins over a direct read |
| `STORAGE_BACKEND` | empty | `gcs` or `s3` to read objects straight from the store; empty reads mounts only |
| `STORAGE_BUCKETS` | required with a backend | Comma-separated buckets the worker may read directly |
| `S3_ENDPOINT_URL` | empty | S3-compatible endpoint, for example `http://seaweedfs:8333`; empty for AWS |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | empty, secrets | S3 key, given together; empty uses the platform's credentials |
| `WORK_DIR` | `/tmp/work` | Scratch for downloaded files; must hold the largest file of the tier |
| `LOG_LEVEL` | `INFO` | Logging level |

Nothing in the code names a Foundry bucket, secret or host. The password is a secret of its own rather than part of a URL, so the Foundry keeps using its existing `rabbitmq_default_pw` and `crucible_admin_apikey` secrets unchanged. The Google Cloud libraries leave the default image, and reading secrets from Secret Manager becomes the deployment's job (the `_FILE` convention covers it). For the Foundry that means, before the first release ships: the GKE Secret Manager add-on enabled on `crucible-cluster`, and read access to the two secrets for the identity the consumers run as (the `default` Kubernetes service account today, through Workload Identity).

## Image and versions

- The image's harness version is its tag (`X.Y.Z`); the bundled `crucible-ingestion` version is an OCI label (`org.crucible.ingestion-version`) and is logged at startup.
- A default build installs the library version locked in `uv.lock`, from PyPI, so a tag is reproducible. The build argument `CRUCIBLE_INGESTION_SPEC` overrides it for custom builds (a pre-release, `git+...@main`, a fork); production runs default builds.
- The first release locks the library at the version the Foundry runs today: commit `7a3078d` (crucible-ingestion PR #54, version `0.1.0`). Publishing it as `0.1.0` through trusted publishing needs a tag on a commit that adds only `release.yml` to `7a3078d`, since `release.yml` is not in `7a3078d` and the publisher is bound to it; the package is then identical to what the Foundry runs, and the worker switches to `crucible-ingestion==0.1.0`.
- Pinned `uv` version, a `.dockerignore`, and a non-root user.
- Published to `ghcr.io/molecularfoundrycrucible/crucible-ingestion-worker` by GitHub Actions on release tags; `:latest` is the newest release.
- Library updates are reviewed version bumps (Dependabot's `uv` support, with `uv lock --check` in CI); the automatic re-pin-and-redeploy chain is removed.

## Operations

- Shutdown: on SIGTERM the worker stops consuming, lets the running job finish within a timeout shorter than the orchestrator's grace period, and acknowledges or requeues it. Long jobs need a long grace period (minutes, not the default 30 seconds).
- Health: the worker touches a heartbeat file while connected and making progress; a liveness probe checks the file's age with a built-in shell test, so the probe needs no extra binaries.
- Scaling: queue workers sit at low CPU while waiting on I/O, so CPU-based autoscaling can leave a backlog growing. Deployments should scale on queue length (for example KEDA's RabbitMQ scaler on Kubernetes). With `prefetch_count=1`, queue length per replica is a direct measure of backlog.
- Resources: tiers differ in memory and in `WORK_DIR` scratch disk, which must hold the largest file of the tier for path-only parsers reading remote input.

## Local deployment

The README leads with a quick start in the style of crucible-api's `deploy/compose` guide: requirements, numbered steps with copy-paste commands, and one check that proves it works.

- Docker Compose: an optional overlay in crucible-api's compose bundle (`compose.ingestion.yaml`, alongside `compose.hub.yaml`) adds RabbitMQ and the worker to an existing installation. The worker reads the bundle's SeaweedFS directly (`STORAGE_BACKEND=s3`, `S3_ENDPOINT_URL=http://seaweedfs:8333`) with the key in `secrets/`; a read-only key for the worker is a later improvement in the bundle.
- Standalone: one `docker run` against an existing API and RabbitMQ.
- Kubernetes: an example manifest for one tier, with the mounted-mode volume shown for GKE and for Mountpoint for Amazon S3.

## Multiple workers

The post-processing workers (rga-analysis, mosaic-stitch, insitu-aggregation, carrier-segmentation) follow the same pattern against the API's pipelines contract. The worker keeps that boundary visible in its layout: a generic core (configuration, connection, consume loop, acknowledgements, status reporting, file access, health, shutdown) and an ingestion handler. When the second worker is rewritten, the core moves to a small shared package, building on the helpers already in `crucible-rabbit-mq/crucible_utils`.

## Changes outside this repository

- crucible-api: allow ingestion for non-GCS storage (it refuses any `storage_backend` other than `gcs` today); add `file_mfid` and `contract_version` to the job; document the worker contract; agree queue names and arguments (quorum, delivery limit, dead-letter); add the compose overlay.
- mf-crucible-deploy: set the Foundry's configuration explicitly in `crucible-prod/ingestion/` (mounts, API URL, queue, secrets through files) before an image without the old defaults reaches production; deliver secrets through the GKE Secret Manager add-on or Kubernetes Secrets; consider queue-length scaling.
- crucible-ingestion (also maintained by this team): the input interface above, the `fsspec` dependency, rerun safety (skip existing thumbnails), and retiring the `notify-consumer` workflow.

## Migration

Each step keeps production working; production is on API 3.3.1 and mounted access until its 4.0 cut-over.

1. Rename the repository and package; remove the auto-bump; Dockerfile hygiene; lock at `7a3078d`.
2. Generic configuration with the Foundry's current values supplied by its deployment, reading through the existing mounts; same queues and message format. Heartbeat-safe threading and `prefetch_count=1`. First release to ghcr; the Foundry deploys it from `mf-crucible-deploy`.
3. Direct reads from GCS and S3 in the worker (done, unreleased). The ingestor input interface in crucible-ingestion; signed-link references in the worker (with `file_mfid` from the API, or looked up from the request); the compose overlay and the quick start. Then the benchmark that decides whether the Foundry keeps its mounts.
4. Quorum queues with dead-lettering, coordinated with the API.
5. Extract the shared core when the post-processing workers move over.

## Open questions

- Who declares queues: the API on publish, the worker on start, or both with identical arguments (required for quorum queues).
- Whether the API should bound tier sizes by the largest file a tier's scratch disk can hold, for path-only parsers reading remote input.
- How signed links behave with range requests and without `HEAD` on SeaweedFS, S3 and GCS; verify on each before relying on it.
- Whether to keep a temporary Secret Manager fallback in the image for the Foundry's first release, or move straight to secret files (which needs the add-on and the access grants above first).
- The Foundry broker's RabbitMQ version: quorum queue delivery limits and at-least-once dead-lettering depend on it, so check it before step 4.

## References

- RabbitMQ quorum queues (delivery limit, poison message handling, at-least-once dead-lettering): https://www.rabbitmq.com/docs/quorum-queues
- Pika connection adapters and `add_callback_threadsafe`: https://pika.readthedocs.io/en/stable/modules/adapters/ ; long tasks blocking heartbeats: https://github.com/pika/pika/issues/418
- GKE Cloud Storage FUSE CSI driver: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/cloud-storage-fuse-csi-driver ; performance tuning (file cache, parallel downloads): https://docs.cloud.google.com/kubernetes-engine/docs/how-to/cloud-storage-fuse-csi-driver-perf
- Mountpoint for Amazon S3 CSI driver: https://docs.aws.amazon.com/eks/latest/userguide/s3-csi.html
- rclone Docker volume plugin: https://rclone.org/docker/
- Queue-length autoscaling with KEDA (Zapier's experience with CPU-based scaling of queue workers): https://www.cncf.io/blog/2022/01/21/keda-at-zapier/
- fsspec HTTP filesystem and caching: https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/implementations/http.html ; block caching for h5py over HTTP: https://earthaccess.readthedocs.io/en/stable/user/howto/fsspec/ ; tuning block size and cache type: https://docs.astropy.org/en/stable/io/fits/usage/cloud.html ; h5py-tuned remote file: https://pypi.org/project/remfile/0.1.7
- crucible-api single-host installation and storage model: `deploy/compose/README.md` and `deploy/compose/docs/storage.md` in crucible-api
