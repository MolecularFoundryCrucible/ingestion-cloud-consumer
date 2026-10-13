"""Path/bucket resolution, config validation, and the file-not-found/unrecognized-
prefix failure paths. The bucket_mounts fixture stubs out ingestion_process's
module-level RabbitMQ/secret setup so importing it is safe in a test environment.
"""
import json

import pytest


class _FakeChannel:
    """Stands in for pika's real channel: the module consumes forever at
    import time (channel.start_consuming() runs at module scope), so this
    must no-op every method the import path touches."""

    def queue_declare(self, queue):
        pass

    def basic_qos(self, prefetch_count=None):
        pass

    def basic_consume(self, queue=None, auto_ack=None, on_message_callback=None):
        pass

    def start_consuming(self):
        pass

    def basic_ack(self, delivery_tag=None):
        pass

    def basic_publish(self, exchange=None, routing_key=None, body=None):
        pass


class _FakeMethod:
    delivery_tag = 1


def _stub_cloud(monkeypatch):
    import ingestion_consumer.cloud as cloud_mod
    monkeypatch.setattr(cloud_mod, "get_secret", lambda *a, **k: "dummy")
    monkeypatch.setattr(cloud_mod, "setup_pika_client", lambda *a, **k: (None, _FakeChannel()))


@pytest.fixture
def bucket_mounts(monkeypatch):
    _stub_cloud(monkeypatch)
    monkeypatch.setenv(
        "GCS_BUCKET_MOUNTS",
        json.dumps({"mf-storage-prod": "/mnt/gcs-prod", "mf-storage-staging": "/mnt/gcs-staging"}),
    )
    import importlib
    import ingestion_consumer.ingestion_process as mod
    importlib.reload(mod)
    return mod


def test_legacy_mnt_gcs_path_passes_through(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("/mnt/gcs/foo/bar.h5") == "/mnt/gcs/foo/bar.h5"


def test_legacy_crucible_uploads_prefix_maps_to_mnt_gcs(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("crucible-uploads/foo/bar.h5") == "/mnt/gcs/foo/bar.h5"


def test_prod_bucket_prefix_maps_to_configured_mount(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("mf-storage-prod/MF123/_files/abcd1234") == "/mnt/gcs-prod/MF123/_files/abcd1234"


def test_staging_bucket_prefix_maps_to_configured_mount(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("mf-storage-staging/MF123/_files/abcd1234") == "/mnt/gcs-staging/MF123/_files/abcd1234"


def test_legacy_object_key_under_shared_bucket_also_resolves(bucket_mounts):
    # Legacy-named objects now live in the same per-environment bucket as new ones.
    assert bucket_mounts.resolve_dataset_path("mf-storage-prod/MF123/original_name.h5") == "/mnt/gcs-prod/MF123/original_name.h5"


def test_unrecognized_prefix_returns_none(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("some-other-bucket/foo") is None


def test_bucket_prefix_match_does_not_loosely_match_similar_names(bucket_mounts):
    # mf-storage-prod must not prefix-match an unconfigured lookalike bucket.
    assert bucket_mounts.resolve_dataset_path("mf-storage-production/foo") is None


def test_bare_bucket_name_with_no_object_key_is_not_matched(bucket_mounts):
    # A message naming just the bucket (no object key) is malformed; it must
    # hit the clean "file not found" path, not resolve to the mount root.
    assert bucket_mounts.resolve_bucket_and_mount("mf-storage-staging") == (None, None)


def test_resolve_bucket_and_mount_identifies_staging_bucket(bucket_mounts):
    bucket, mount = bucket_mounts.resolve_bucket_and_mount("mf-storage-staging/MF123/_files/abcd1234")
    assert bucket == "mf-storage-staging"
    assert mount == "/mnt/gcs-staging"


def test_resolve_bucket_and_mount_returns_none_for_legacy_paths(bucket_mounts):
    bucket, mount = bucket_mounts.resolve_bucket_and_mount("/mnt/gcs/foo/bar.h5")
    assert bucket is None


def test_missing_env_var_falls_back_to_known_defaults(monkeypatch):
    # Deploys that haven't set GCS_BUCKET_MOUNTS yet must keep working exactly
    # as before (mf-storage-prod -> /mnt/gcs-prod), so the rollout order
    # (code first, env var second) can't create a window with no mapping at all.
    _stub_cloud(monkeypatch)
    monkeypatch.delenv("GCS_BUCKET_MOUNTS", raising=False)
    import importlib
    import ingestion_consumer.ingestion_process as mod
    importlib.reload(mod)
    assert mod.resolve_dataset_path("mf-storage-prod/MF123/foo.h5") == "/mnt/gcs-prod/MF123/foo.h5"


def test_invalid_json_in_bucket_mounts_raises_at_import(monkeypatch):
    # A typo in a value someone explicitly set must fail loudly at startup,
    # not silently fall back to prod-only (which would then drop every
    # staging message per test_unrecognized_filename_prefix_marks_failed...).
    _stub_cloud(monkeypatch)
    monkeypatch.setenv("GCS_BUCKET_MOUNTS", "{not valid json")
    import importlib
    import ingestion_consumer.ingestion_process as mod
    with pytest.raises(ValueError):
        importlib.reload(mod)


def test_wrong_shaped_bucket_mounts_raises_at_import(monkeypatch):
    _stub_cloud(monkeypatch)
    monkeypatch.setenv("GCS_BUCKET_MOUNTS", json.dumps(["mf-storage-prod"]))
    import importlib
    import ingestion_consumer.ingestion_process as mod
    with pytest.raises(ValueError):
        importlib.reload(mod)


def test_is_file_lost_treats_missing_mount_as_lost_without_calling_gcs(bucket_mounts, monkeypatch):
    update_calls = []
    monkeypatch.setattr(bucket_mounts.client.ingestions, "update", lambda *a, **k: update_calls.append((a, k)))

    gcs_client_calls = []
    monkeypatch.setattr(bucket_mounts.gcs, "Client", lambda: gcs_client_calls.append(1))

    # /mnt/gcs-staging is never mounted in this test environment.
    message = {"reqid": "r1", "filename": "mf-storage-staging/MF123/_files/abcd1234"}
    lost = bucket_mounts.is_file_lost(message, "/mnt/gcs-staging/MF123/_files/abcd1234", ch=None, update_status=True)

    assert lost is True
    assert update_calls == [(("r1",), {"status": "file not found"})]
    assert gcs_client_calls == [], "a missing mount must short-circuit before the GCS check runs"


def test_unrecognized_filename_prefix_marks_failed_and_republishes(bucket_mounts, monkeypatch):
    update_calls = []
    monkeypatch.setattr(bucket_mounts.client.ingestions, "update", lambda *a, **k: update_calls.append((a, k)))
    published = []
    ch = _FakeChannel()
    monkeypatch.setattr(ch, "basic_publish", lambda exchange, routing_key, body: published.append((routing_key, body)))

    message = {"filename": "some-other-bucket/foo", "reqid": "r2", "dsid": "d1", "ingestion_class": None}
    body = json.dumps(message).encode()

    bucket_mounts.callback(ch, _FakeMethod(), None, body)

    assert update_calls == [(("r2",), {"status": "file not found"})]
    assert published == [(f"ingestion-{bucket_mounts.RMQ_ROUTING_SUFFIX}-failed", json.dumps(message))]
