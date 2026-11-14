import logging
import os
from typing import Callable, Optional, Tuple

logger = logging.getLogger(__name__)


def _under(path: str, directory: str) -> bool:
    return path == directory or path.startswith(directory.rstrip("/") + "/")


def resolve_path(storage_path: str, mounts: dict) -> Tuple[Optional[str], Optional[str]]:
    """Map a job's storage path to (local path, bucket), or (None, None) when no
    configured mount covers it. A path already under a mount directory is used as is."""
    path = storage_path.replace("\\", "/")
    for bucket, directory in mounts.items():
        if _under(path, directory):
            return path, bucket
    for bucket, directory in mounts.items():
        if path.startswith(bucket + "/") and len(path) > len(bucket) + 1:
            return directory + path[len(bucket):], bucket
    return None, None


def gcs_blob_exists(bucket: str, key: str) -> bool:
    from google.cloud import storage

    return storage.Client().bucket(bucket).blob(key).exists()


def file_exists(local_path: str, bucket: Optional[str], mounts: dict, exists_check: str) -> bool:
    if bucket is not None:
        directory = mounts[bucket]
        if not os.path.isdir(directory):
            # The mount is configured but missing from this container, so an
            # object-store check would pass while the file stays unreachable.
            logger.error(f"Mount {directory} for bucket {bucket} is not available")
            return False
        if exists_check == "gcs":
            key = local_path[len(directory):].lstrip("/")
            try:
                return gcs_blob_exists(bucket, key)
            except Exception as err:
                logger.warning(f"GCS existence check failed for {bucket}/{key}: {err}; checking the mount instead")
    return os.path.exists(local_path)


def local_name(file_name: Optional[str], key: str) -> str:
    """A safe file name for a downloaded object: the original name's basename, which
    keeps the extension parsers look for, else the object key's last segment."""
    for candidate in (file_name, key):
        name = os.path.basename((candidate or "").replace("\\", "/"))
        if name not in ("", ".", ".."):
            return name
    return "input"


class ObjectStorage:
    """Reads objects straight from a GCS or S3 store, for buckets listed in STORAGE_BUCKETS."""

    def __init__(self, config, make_store: Optional[Callable] = None):
        self.config = config
        self._make_store = make_store or self._connect
        self._stores = {}

    def locate(self, storage_path: str) -> Optional[Tuple[str, str]]:
        if not self.config.storage_backend:
            return None
        bucket, separator, key = storage_path.partition("/")
        if not separator or not key or bucket not in self.config.storage_buckets:
            return None
        return bucket, key

    def _store(self, bucket: str):
        if bucket not in self._stores:
            self._stores[bucket] = self._make_store(bucket)
        return self._stores[bucket]

    def _connect(self, bucket: str):
        from obstore.store import GCSStore, S3Store

        c = self.config
        if c.storage_backend == "gcs":
            return GCSStore(bucket)
        options = {"region": c.s3_region}
        if c.s3_endpoint_url:
            options["endpoint"] = c.s3_endpoint_url
        if c.s3_access_key_id:
            options.update(access_key_id=c.s3_access_key_id, secret_access_key=c.s3_secret_access_key)
        allow_http = bool(c.s3_endpoint_url and c.s3_endpoint_url.startswith("http://"))
        return S3Store(bucket, client_options={"allow_http": allow_http}, **options)

    def exists(self, bucket: str, key: str) -> bool:
        import obstore

        try:
            obstore.head(self._store(bucket), key)
        except FileNotFoundError:
            return False
        return True

    def download(self, bucket: str, key: str, destination: str) -> None:
        import obstore

        result = obstore.get(self._store(bucket), key)
        with open(destination, "wb") as out:
            for chunk in result.stream():
                out.write(chunk)
