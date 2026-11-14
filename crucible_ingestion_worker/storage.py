import logging
import os
from typing import Optional, Tuple

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
