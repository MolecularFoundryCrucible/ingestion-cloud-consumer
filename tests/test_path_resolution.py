"""Path resolution must handle both the legacy FUSE-mounted multipart bucket and
the new per-environment buckets, driven entirely by the GCS_BUCKET_MOUNTS env
var (not a hardcoded mapping), and must fail loudly (None) rather than
guessing when it sees something it doesn't recognize.

Importing ingestion_consumer.ingestion_process runs real setup at module level
(RabbitMQ connect, secret fetch) -- not reachable in a test environment, and
contrary to this module's own stated intent (pure functions "unit-testable
without RabbitMQ/GCS/Crucible credentials"). The bucket_mounts fixture stubs
those two side effects out before import/reload so the module loads cleanly.
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


@pytest.fixture
def bucket_mounts(monkeypatch):
    import ingestion_consumer.cloud as cloud_mod
    monkeypatch.setattr(cloud_mod, "get_secret", lambda *a, **k: "dummy")
    monkeypatch.setattr(cloud_mod, "setup_pika_client", lambda *a, **k: (None, _FakeChannel()))
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
    import ingestion_consumer.cloud as cloud_mod
    monkeypatch.setattr(cloud_mod, "get_secret", lambda *a, **k: "dummy")
    monkeypatch.setattr(cloud_mod, "setup_pika_client", lambda *a, **k: (None, _FakeChannel()))
    monkeypatch.delenv("GCS_BUCKET_MOUNTS", raising=False)
    import importlib
    import ingestion_consumer.ingestion_process as mod
    importlib.reload(mod)
    assert mod.resolve_dataset_path("mf-storage-prod/MF123/foo.h5") == "/mnt/gcs-prod/MF123/foo.h5"
