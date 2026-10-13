# Agent/contributor guidance

## Deploy model

Every push to `main` or `staging` triggers Cloud Build (`cloudbuild.yaml`), which builds and pushes a new image, applies `k8s-conf-${BRANCH_NAME}.yaml`, and runs `kubectl set image` on all three `ingestion-consumer-${BRANCH_NAME}-{small,medium,large}` deployments.

**A merge to `main` is a production deploy.** It is not staged, gated, or reviewed again after merge — the moment the merge commit lands on `main`, Cloud Build starts rolling it out to production. The same is true of `staging` for the staging consumers.

Separately, `.github/workflows/update-ingestion.yml` listens for a `repository_dispatch` from crucible-ingestion (fired when that repo's main is updated) and auto-commits + pushes a `uv.lock` pin bump straight to whichever branch it runs on, with no PR. That push triggers its own Cloud Build run — **so every merge to crucible-ingestion's main also deploys this repo's production consumers automatically**, on whatever code is currently on this repo's `main` at that moment, independent of and unsynchronized with any deploy already in flight from a push here.

## Incident: concurrent builds raced on `kubectl set image` (2026-10-13)

PR #1 (commit `c2c2188`) merged consumer code that calls `data_ingestion(..., file_name=...)`, while `uv.lock` still pinned `crucible-ingestion` at `5c3050848a8f`, a commit that doesn't accept `file_name`. Seconds after the merge, the `update-ingestion.yml` dispatch fired and pushed the pin bump (`c3abb8d`) directly to `main`.

Both pushes triggered independent Cloud Build runs on `main` concurrently. The pin-bump build (`c3abb8d`) finished first; the merge-commit build (`c2c2188`) finished about 13 seconds later and won the last `kubectl set image`, so all three `ingestion-consumer-main-*` deployments ended up running the **broken** image. Every ingestion attempt raised `TypeError` on the missing `file_name` argument from approximately 20:10 to 20:14 UTC.

Recovered by re-running, on all three deployments:

```bash
kubectl set image deployment/ingestion-consumer-main-small  ingestion-consumer=us-central1-docker.pkg.dev/mf-crucible/ingestion-cloud-consumer/main:c3abb8d
kubectl set image deployment/ingestion-consumer-main-medium ingestion-consumer=us-central1-docker.pkg.dev/mf-crucible/ingestion-cloud-consumer/main:c3abb8d
kubectl set image deployment/ingestion-consumer-main-large  ingestion-consumer=us-central1-docker.pkg.dev/mf-crucible/ingestion-cloud-consumer/main:c3abb8d
kubectl rollout status deployment/ingestion-consumer-main-small
kubectl rollout status deployment/ingestion-consumer-main-medium
kubectl rollout status deployment/ingestion-consumer-main-large
```

Messages that fail land on the `ingestion-${RMQ_ROUTING_SUFFIX}-failed` queue (e.g. `ingestion-main-failed`) rather than being lost; requeue them once the fix is confirmed live.

## Rules

- A consumer change that depends on a new crucible-ingestion API must bump the `uv.lock` pin in the **same PR**, after the library PR has already merged — never merge consumer code ahead of the pin it needs.
- Never push two commits to `main` or `staging` back to back while a build from the first is still running. If this does happen (including via the auto pin-bump dispatch), check `kubectl get deploy -l app -o custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image` once both builds finish and correct the image if the wrong one won.
- Keep the signature-guard test on the pinned `data_ingestion` (or whichever function changed) — it fails loudly if the pin is behind what the consumer code calls, instead of failing at runtime in production.
- Worth considering: making the Cloud Build trigger serial per branch, or having the deploy step skip when its `SHORT_SHA` is not the current branch head.
