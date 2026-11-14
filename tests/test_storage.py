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


def object_storage(env=None, **objects):
    import obstore
    from obstore.store import MemoryStore

    from crucible_ingestion_worker.config import load_config

    base = {"CRUCIBLE_API_URL": "u", "CRUCIBLE_API_KEY": "k", "RABBITMQ_HOST": "h", "RABBITMQ_PASSWORD": "pw",
            "QUEUE": "q", "STORAGE_BACKEND": "s3", "STORAGE_BUCKETS": "crucible-files"}
    base.update(env or {})
    store = MemoryStore()
    for key, data in objects.items():
        obstore.put(store, key, data)
    made = []

    def make_store(bucket):
        made.append(bucket)
        return store

    return storage.ObjectStorage(load_config(base), make_store=make_store), made


def test_locate_only_allowed_buckets():
    objects, _ = object_storage()
    assert objects.locate("crucible-files/MF1/_files/abc") == ("crucible-files", "MF1/_files/abc")
    assert objects.locate("other-bucket/MF1/_files/abc") is None
    assert objects.locate("crucible-files") is None
    assert objects.locate("crucible-files/") is None


def test_locate_is_off_without_a_backend():
    objects, _ = object_storage({"STORAGE_BACKEND": "", "STORAGE_BUCKETS": ""})
    assert objects.locate("crucible-files/MF1/_files/abc") is None


def test_exists_and_download(tmp_path):
    objects, made = object_storage(**{"MF1/_files/abc": b"payload"})
    assert objects.exists("crucible-files", "MF1/_files/abc")
    assert not objects.exists("crucible-files", "MF1/_files/missing")
    objects.download("crucible-files", "MF1/_files/abc", str(tmp_path / "out"))
    assert (tmp_path / "out").read_bytes() == b"payload"
    assert made == ["crucible-files"], "one client per bucket, reused"


@pytest.mark.parametrize("file_name,key,expected", [
    ("scan.h5", "MF1/_files/abc", "scan.h5"),
    ("../../etc/passwd", "MF1/_files/abc", "passwd"),
    ("dir\\scan.dm4", "MF1/_files/abc", "scan.dm4"),
    (None, "MF1/_files/abc", "abc"),
    ("..", "MF1/_files/abc", "abc"),
])
def test_local_name_keeps_only_a_safe_basename(file_name, key, expected):
    assert storage.local_name(file_name, key) == expected


def test_local_name_shortens_long_names_and_keeps_the_extension():
    name = storage.local_name("é" * 600 + ".tar.gz", "MF1/_files/abc")
    assert len(name.encode()) <= 200
    assert name.endswith(".gz")


def test_gcs_blob_exists_reads_through_obstore(monkeypatch):
    import obstore
    from obstore.store import MemoryStore

    store = MemoryStore()
    obstore.put(store, "MF123/_files/abc", b"x")
    monkeypatch.setattr(storage, "_gcs_store", lambda bucket: store)
    assert storage.gcs_blob_exists("mf-storage-prod", "MF123/_files/abc") is True
    assert storage.gcs_blob_exists("mf-storage-prod", "MF123/_files/missing") is False
