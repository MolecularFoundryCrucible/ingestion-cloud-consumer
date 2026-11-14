import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional


class ConfigError(ValueError):
    pass


def read_secret(name: str, env: Mapping[str, str], required: bool = True) -> Optional[str]:
    """Read a secret given exactly one way: NAME, NAME_FILE (a file path), or
    NAME_VERSION (a Google Secret Manager version path)."""
    given = {key: env[key] for key in (name, f"{name}_FILE", f"{name}_VERSION") if env.get(key)}
    if len(given) > 1:
        raise ConfigError(f"{name} is given more than one way: {', '.join(sorted(given))}")
    if not given:
        if required:
            raise ConfigError(f"{name} is not set; set {name}, {name}_FILE or {name}_VERSION")
        return None
    key, value = next(iter(given.items()))
    if key == name:
        return value
    if key.endswith("_FILE"):
        return Path(value).read_text().rstrip("\n")
    return _read_secret_manager(value)


def _read_secret_manager(version_path: str) -> str:
    from google.cloud import secretmanager

    response = secretmanager.SecretManagerServiceClient().access_secret_version(name=version_path)
    return response.payload.data.decode("utf-8")


@dataclass(frozen=True)
class Config:
    api_url: str
    api_key: str = field(repr=False)
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_vhost: str
    rabbitmq_password: str = field(repr=False)
    rabbitmq_heartbeat: int
    queue: str
    failed_queue: str
    not_supported_queue: Optional[str]
    prefetch_count: int
    storage_mounts: dict
    storage_exists_check: str
    log_level: str
    shutdown_timeout: int
    health_file: Optional[str]
    health_interval: int


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ConfigError(f"{name} is required")
    return value


def _integer(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None


def _storage_mounts(env: Mapping[str, str]) -> dict:
    raw = env.get("STORAGE_MOUNTS")
    if not raw:
        return {}
    try:
        mounts = json.loads(raw)
    except json.JSONDecodeError:
        raise ConfigError(f"STORAGE_MOUNTS is not valid JSON: {raw!r}") from None
    if not isinstance(mounts, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in mounts.items()):
        raise ConfigError(f"STORAGE_MOUNTS must be a JSON object of bucket to directory, got {raw!r}")
    for bucket, directory in mounts.items():
        if not bucket or "/" in bucket:
            raise ConfigError(f"STORAGE_MOUNTS has an invalid bucket name {bucket!r}")
        if not directory.startswith("/"):
            raise ConfigError(f"STORAGE_MOUNTS directory for {bucket!r} must be an absolute path, got {directory!r}")
    return {bucket: directory.rstrip("/") or "/" for bucket, directory in mounts.items()}


def load_config(env: Mapping[str, str] = os.environ) -> Config:
    queue = _required(env, "QUEUE")
    exists_check = env.get("STORAGE_EXISTS_CHECK", "")
    if exists_check not in ("", "gcs"):
        raise ConfigError(f"STORAGE_EXISTS_CHECK must be empty or 'gcs', got {exists_check!r}")
    return Config(
        api_url=_required(env, "CRUCIBLE_API_URL"),
        api_key=read_secret("CRUCIBLE_API_KEY", env),
        rabbitmq_host=_required(env, "RABBITMQ_HOST"),
        rabbitmq_port=_integer(env, "RABBITMQ_PORT", 5672),
        rabbitmq_user=env.get("RABBITMQ_USER") or "guest",
        rabbitmq_vhost=env.get("RABBITMQ_VHOST") or "/",
        rabbitmq_password=read_secret("RABBITMQ_PASSWORD", env),
        rabbitmq_heartbeat=_integer(env, "RABBITMQ_HEARTBEAT", 60),
        queue=queue,
        failed_queue=env.get("FAILED_QUEUE") or f"{queue}-failed",
        not_supported_queue=env.get("NOT_SUPPORTED_QUEUE", "not-supported") or None,
        prefetch_count=_integer(env, "PREFETCH_COUNT", 1),
        storage_mounts=_storage_mounts(env),
        storage_exists_check=exists_check,
        log_level=env.get("LOG_LEVEL") or "INFO",
        shutdown_timeout=_integer(env, "SHUTDOWN_TIMEOUT", 25),
        health_file=env.get("HEALTH_FILE", "/tmp/worker-alive") or None,
        health_interval=_integer(env, "HEALTH_INTERVAL", 15),
    )
