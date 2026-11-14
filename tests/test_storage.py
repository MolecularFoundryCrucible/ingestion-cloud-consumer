import pytest

from crucible_ingestion_worker import storage
from crucible_ingestion_worker.storage import file_exists, resolve_path

MOUNTS = {
    "crucible-uploads": "/mnt/gcs",
    "mf-storage-prod": "/mnt/gcs-prod",
    "mf-storage-staging": "/mnt/gcs-staging",
}


def test_path_already_under_a_mount_passes_through():
    assert resolve_path("/mnt/gcs/foo/bar.h5", MOUNTS) == ("/mnt/gcs/foo/bar.h5", "crucible-uploads")


def test_crucible_uploads_prefix_maps_to_its_mount():
    assert resolve_path("crucible-uploads/foo/bar.h5", MOUNTS) == ("/mnt/gcs/foo/bar.h5", "crucible-uploads")


def test_bucket_prefix_maps_to_configured_mount():
    assert resolve_path("mf-storage-prod/MF123/_files/abcd1234", MOUNTS) == (
        "/mnt/gcs-prod/MF123/_files/abcd1234", "mf-storage-prod")
    assert resolve_path("mf-storage-staging/MF123/_files/abcd1234", MOUNTS) == (
        "/mnt/gcs-staging/MF123/_files/abcd1234", "mf-storage-staging")


def test_legacy_object_key_under_bucket_resolves():
    assert resolve_path("mf-storage-prod/MF123/original_name.h5", MOUNTS)[0] == "/mnt/gcs-prod/MF123/original_name.h5"


def test_backslashes_are_normalized():
    assert resolve_path("mf-storage-prod\\MF123\\a.h5", MOUNTS)[0] == "/mnt/gcs-prod/MF123/a.h5"


def test_unrecognized_prefix_is_not_resolved():
    assert resolve_path("some-other-bucket/foo", MOUNTS) == (None, None)


def test_lookalike_bucket_is_not_matched():
    assert resolve_path("mf-storage-production/foo", MOUNTS) == (None, None)


def test_bare_bucket_name_is_not_matched():
    assert resolve_path("mf-storage-staging", MOUNTS) == (None, None)
    assert resolve_path("mf-storage-staging/", MOUNTS) == (None, None)


def test_nothing_resolves_without_mounts():
    assert resolve_path("mf-storage-prod/MF123/a.h5", {}) == (None, None)


def test_missing_mount_is_lost_without_checking_gcs(monkeypatch):
    calls = []
    monkeypatch.setattr(storage, "gcs_blob_exists", lambda *a: calls.append(a) or True)
    assert file_exists("/mnt/gcs-staging/MF123/x", "mf-storage-staging", MOUNTS, "gcs") is False
    assert calls == [], "a missing mount must short-circuit before the object-store check"


def test_gcs_check_used_when_enabled(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(storage, "gcs_blob_exists", lambda bucket, key: calls.append((bucket, key)) or True)
    mounts = {"mf-storage-prod": str(tmp_path)}
    assert file_exists(f"{tmp_path}/MF123/_files/abc", "mf-storage-prod", mounts, "gcs") is True
    assert calls == [("mf-storage-prod", "MF123/_files/abc")]


def test_gcs_check_failure_falls_back_to_the_mount(tmp_path, monkeypatch):
    def boom(bucket, key):
        raise RuntimeError("no credentials")

    monkeypatch.setattr(storage, "gcs_blob_exists", boom)
    (tmp_path / "present.h5").write_text("x")
    mounts = {"b": str(tmp_path)}
    assert file_exists(f"{tmp_path}/present.h5", "b", mounts, "gcs") is True
    assert file_exists(f"{tmp_path}/absent.h5", "b", mounts, "gcs") is False


def test_without_gcs_check_only_the_filesystem_is_used(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "gcs_blob_exists", lambda *a: pytest.fail("GCS must not be called"))
    (tmp_path / "present.h5").write_text("x")
    mounts = {"b": str(tmp_path)}
    assert file_exists(f"{tmp_path}/present.h5", "b", mounts, "") is True
    assert file_exists(f"{tmp_path}/absent.h5", "b", mounts, "") is False
