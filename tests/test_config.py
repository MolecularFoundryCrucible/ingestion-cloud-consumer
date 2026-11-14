import json

import pytest

from crucible_ingestion_worker import config as config_mod
from crucible_ingestion_worker.config import ConfigError, load_config, read_secret

BASE = {
    "CRUCIBLE_API_URL": "https://crucible.example/api/v3",
    "CRUCIBLE_API_KEY": "key",
    "RABBITMQ_HOST": "rabbit",
    "RABBITMQ_PASSWORD": "pw",
    "QUEUE": "ingestion-main-small",
}


def test_minimal_config_and_defaults():
    c = load_config(BASE)
    assert (c.rabbitmq_port, c.rabbitmq_user, c.rabbitmq_vhost, c.rabbitmq_heartbeat) == (5672, "guest", "/", 60)
    assert c.failed_queue == "ingestion-main-small-failed"
    assert c.not_supported_queue == "not-supported"
    assert c.storage_mounts == {} and c.storage_exists_check == ""
    assert c.prefetch_count == 1 and c.log_level == "INFO"
    assert (c.shutdown_timeout, c.health_file, c.health_interval) == (25, "/tmp/worker-alive", 15)


def test_health_file_can_be_disabled():
    assert load_config(dict(BASE, HEALTH_FILE="")).health_file is None


def test_foundry_style_config():
    env = dict(BASE, RABBITMQ_USER="admin", RABBITMQ_PORT="5672", STORAGE_EXISTS_CHECK="gcs",
               STORAGE_MOUNTS=json.dumps({"crucible-uploads": "/mnt/gcs", "mf-storage-prod": "/mnt/gcs-prod/"}))
    c = load_config(env)
    assert c.rabbitmq_user == "admin"
    assert c.storage_mounts == {"crucible-uploads": "/mnt/gcs", "mf-storage-prod": "/mnt/gcs-prod"}
    assert c.storage_exists_check == "gcs"


@pytest.mark.parametrize("missing", ["CRUCIBLE_API_URL", "RABBITMQ_HOST", "QUEUE"])
def test_required_settings(missing):
    env = {k: v for k, v in BASE.items() if k != missing}
    with pytest.raises(ConfigError, match=missing):
        load_config(env)


@pytest.mark.parametrize("secret", ["CRUCIBLE_API_KEY", "RABBITMQ_PASSWORD"])
def test_secrets_are_required(secret):
    env = {k: v for k, v in BASE.items() if k != secret}
    with pytest.raises(ConfigError, match=secret):
        load_config(env)


def test_secret_given_two_ways_is_refused(tmp_path):
    f = tmp_path / "pw"
    f.write_text("from-file\n")
    with pytest.raises(ConfigError, match="more than one way"):
        read_secret("RABBITMQ_PASSWORD", {"RABBITMQ_PASSWORD": "x", "RABBITMQ_PASSWORD_FILE": str(f)})


def test_secret_from_file_drops_the_trailing_newline(tmp_path):
    f = tmp_path / "pw"
    f.write_text("from-file\n")
    assert read_secret("RABBITMQ_PASSWORD", {"RABBITMQ_PASSWORD_FILE": str(f)}) == "from-file"


def test_secret_from_secret_manager(monkeypatch):
    seen = []
    monkeypatch.setattr(config_mod, "_read_secret_manager", lambda path: seen.append(path) or "from-sm")
    path = "projects/1/secrets/rabbitmq_default_pw/versions/1"
    assert read_secret("RABBITMQ_PASSWORD", {"RABBITMQ_PASSWORD_VERSION": path}) == "from-sm"
    assert seen == [path]


def test_blank_secret_counts_as_unset():
    assert read_secret("X", {"X": ""}, required=False) is None


@pytest.mark.parametrize("mounts", [
    "{not json",
    json.dumps(["mf-storage-prod"]),
    json.dumps({"mf-storage-prod": 3}),
    json.dumps({"mf-storage-prod": "relative/dir"}),
    json.dumps({"bad/bucket": "/mnt/x"}),
])
def test_invalid_storage_mounts_fail_loudly(mounts):
    with pytest.raises(ConfigError, match="STORAGE_MOUNTS"):
        load_config(dict(BASE, STORAGE_MOUNTS=mounts))


def test_invalid_exists_check_fails():
    with pytest.raises(ConfigError, match="STORAGE_EXISTS_CHECK"):
        load_config(dict(BASE, STORAGE_EXISTS_CHECK="s3"))


def test_not_supported_queue_can_be_disabled():
    assert load_config(dict(BASE, NOT_SUPPORTED_QUEUE="")).not_supported_queue is None


def test_secrets_are_not_in_repr():
    r = repr(load_config(dict(BASE, CRUCIBLE_API_KEY="SECRET-KEY", RABBITMQ_PASSWORD="SECRET-PW")))
    assert "SECRET-KEY" not in r and "SECRET-PW" not in r
