import json

import pytest
import requests

from crucible_ingestion_worker import worker
from crucible_ingestion_worker.config import load_config


class FakeChannel:
    def __init__(self):
        self.published, self.acked, self.declared = [], [], []

    def basic_publish(self, exchange, routing_key, body):
        self.published.append((routing_key, json.loads(body)))

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def queue_declare(self, queue):
        self.declared.append(queue)


class FakeIngestions:
    def __init__(self, status="requested"):
        self.calls = []
        self.status = status

    def get(self, reqid):
        if isinstance(self.status, Exception):
            raise self.status
        return {"id": reqid, "status": self.status}

    def update(self, reqid, status=None, **fields):
        self.calls.append((reqid, status, fields))


class FakeClient:
    def __init__(self):
        self.ingestions = FakeIngestions()


class Method:
    delivery_tag = 7


def make_handler(tmp_path, monkeypatch, **env):
    monkeypatch.setattr(worker, "get_ingestion_githash", lambda: None)
    monkeypatch.setattr(worker, "version", lambda name: "0.2.0")
    base = {
        "CRUCIBLE_API_URL": "https://crucible.example/api/v3", "CRUCIBLE_API_KEY": "k",
        "RABBITMQ_HOST": "rabbit", "RABBITMQ_PASSWORD": "pw", "QUEUE": "ingestion-main-small",
        "STORAGE_MOUNTS": json.dumps({"mf-storage-prod": str(tmp_path)}),
    }
    base.update(env)
    client = FakeClient()
    sleeps = []
    return worker.JobHandler(load_config(base), client, sleep=sleeps.append), client, sleeps


def body(**fields):
    msg = {"filename": "mf-storage-prod/MF1/_files/abc", "reqid": 42, "dsid": "MF1",
           "ingestion_class": None, "file_name": "scan.h5"}
    msg.update(fields)
    return msg, json.dumps(msg).encode()


def statuses(client):
    return [status for _, status, _ in client.ingestions.calls]


def test_unresolvable_filename_is_file_not_found_and_requeued(tmp_path, monkeypatch):
    handler, client, _ = make_handler(tmp_path, monkeypatch)
    ch = FakeChannel()
    msg, raw = body(filename="some-other-bucket/foo")
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["file not found"]
    assert ch.published == [("ingestion-main-small-failed", msg)]
    assert ch.acked == [7]


def test_missing_file_after_retries(tmp_path, monkeypatch):
    handler, client, sleeps = make_handler(tmp_path, monkeypatch)
    ch = FakeChannel()
    _, raw = body()
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "file not found"]
    assert sleeps == [2, 4, 8, 16]
    assert ch.acked == [7] and ch.published == []


def test_successful_ingestion(tmp_path, monkeypatch):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch)
    seen = {}

    def fake_ingestion(**kwargs):
        seen.update(kwargs)
        return {"unique_id": "MF1"}, "SomeIngestor", True

    monkeypatch.setattr(worker, "data_ingestion", fake_ingestion)
    ch = FakeChannel()
    _, raw = body()
    handler(ch, Method(), None, raw)
    assert seen["dataset_to_process"] == f"{tmp_path}/MF1/_files/abc"
    assert seen["file_name"] == "scan.h5" and seen["include_file"] is False
    assert client.ingestions.calls[0] == (42, "started", {"ingestion_githash": "0.2.0"})
    assert client.ingestions.calls[-1] == (42, "complete", {"ingestion_githash": "0.2.0", "ingestion_class": "SomeIngestor"})
    assert ch.acked == [7]


@pytest.mark.parametrize("queue_env,expected", [({}, ["not-supported"]), ({"NOT_SUPPORTED_QUEUE": ""}, [])])
def test_unsupported_file(tmp_path, monkeypatch, queue_env, expected):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch, **queue_env)
    monkeypatch.setattr(worker, "data_ingestion", lambda **k: (None, None, False))
    ch = FakeChannel()
    _, raw = body()
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "not supported"]
    assert [q for q, _ in ch.published] == expected
    assert ch.acked == [7]


def test_parser_error_marks_failed_and_requeues(tmp_path, monkeypatch):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch)

    def boom(**k):
        raise ValueError("bad file")

    monkeypatch.setattr(worker, "data_ingestion", boom)
    ch = FakeChannel()
    msg, raw = body()
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "failed"]
    assert ch.published == [("ingestion-main-small-failed", msg)]
    assert ch.acked == [7]


def test_dataset_not_yet_visible_is_retried(tmp_path, monkeypatch):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, sleeps = make_handler(tmp_path, monkeypatch)
    attempts = []

    def flaky(**k):
        attempts.append(1)
        if len(attempts) < 3:
            response = requests.Response()
            response.status_code = 404
            raise requests.exceptions.HTTPError(response=response)
        return {"unique_id": "MF1"}, "SomeIngestor", True

    monkeypatch.setattr(worker, "data_ingestion", flaky)
    ch = FakeChannel()
    _, raw = body()
    handler(ch, Method(), None, raw)
    assert len(attempts) == 3 and sleeps == [2, 4]
    assert statuses(client) == ["started", "complete"]


def test_ingestion_version_prefers_git_commit(monkeypatch):
    monkeypatch.setattr(worker, "get_ingestion_githash", lambda: "abc123")
    assert worker.ingestion_version() == "abc123"
    monkeypatch.setattr(worker, "get_ingestion_githash", lambda: None)
    monkeypatch.setattr(worker, "version", lambda name: "0.2.0")
    assert worker.ingestion_version() == "0.2.0"


def test_connect_declares_work_failed_and_not_supported_queues(monkeypatch, tmp_path):
    import pika

    ch = FakeChannel()

    class FakeConnection:
        def __init__(self, params):
            self.params = params

        def channel(self):
            return ch

    monkeypatch.setattr(pika, "BlockingConnection", FakeConnection)
    cfg = load_config({"CRUCIBLE_API_URL": "u", "CRUCIBLE_API_KEY": "k", "RABBITMQ_HOST": "h",
                       "RABBITMQ_USER": "admin", "RABBITMQ_PASSWORD": "pw", "QUEUE": "ingestion-main-small"})
    connection, _ = worker.connect(cfg)
    assert ch.declared == ["ingestion-main-small", "ingestion-main-small-failed", "not-supported"]
    assert connection.params.credentials.username == "admin"
    assert connection.params.virtual_host == "/"


DIRECT = {"STORAGE_MOUNTS": "", "STORAGE_BACKEND": "s3", "STORAGE_BUCKETS": "crucible-files"}


def direct_handler(tmp_path, monkeypatch, **objects):
    import obstore
    from obstore.store import MemoryStore

    from crucible_ingestion_worker.storage import ObjectStorage

    store = MemoryStore()
    for key, data in objects.items():
        obstore.put(store, key, data)
    handler, client, sleeps = make_handler(tmp_path, monkeypatch, WORK_DIR=str(tmp_path / "work"), **DIRECT)
    handler.storage = ObjectStorage(handler.config, make_store=lambda bucket: store)
    return handler, client, sleeps


def test_direct_read_downloads_under_the_original_name_and_cleans_up(tmp_path, monkeypatch):
    handler, client, _ = direct_handler(tmp_path, monkeypatch, **{"MF1/_files/abc": b"data"})
    seen = {}

    def fake_ingestion(**kwargs):
        path = kwargs["dataset_to_process"]
        seen["path"], seen["content"] = path, open(path, "rb").read()
        return {"unique_id": "MF1"}, "SomeIngestor", True

    monkeypatch.setattr(worker, "data_ingestion", fake_ingestion)
    ch = FakeChannel()
    _, raw = body(filename="crucible-files/MF1/_files/abc")
    handler(ch, Method(), None, raw)
    assert seen["content"] == b"data"
    assert seen["path"].startswith(str(tmp_path / "work")) and seen["path"].endswith("/scan.h5")
    assert statuses(client) == ["started", "complete"]
    assert list((tmp_path / "work").iterdir()) == []
    assert ch.acked == [7]


def test_direct_read_cleans_up_when_the_parser_fails(tmp_path, monkeypatch):
    handler, client, _ = direct_handler(tmp_path, monkeypatch, **{"MF1/_files/abc": b"data"})

    def boom(**k):
        raise ValueError("bad file")

    monkeypatch.setattr(worker, "data_ingestion", boom)
    ch = FakeChannel()
    _, raw = body(filename="crucible-files/MF1/_files/abc")
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "failed"]
    assert list((tmp_path / "work").iterdir()) == []


def test_direct_read_missing_object_after_retries(tmp_path, monkeypatch):
    handler, client, sleeps = direct_handler(tmp_path, monkeypatch)
    ch = FakeChannel()
    _, raw = body(filename="crucible-files/MF1/_files/abc")
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "file not found"]
    assert sleeps == [2, 4, 8, 16]
    assert ch.acked == [7] and ch.published == []


def test_direct_read_store_error_is_failed_not_file_not_found(tmp_path, monkeypatch):
    handler, client, sleeps = direct_handler(tmp_path, monkeypatch)

    def denied(bucket, key):
        raise PermissionError("access denied")

    handler.storage.exists = denied
    ch = FakeChannel()
    msg, raw = body(filename="crucible-files/MF1/_files/abc")
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["started", "failed"]
    assert sleeps == []
    assert ch.published == [("ingestion-main-small-failed", msg)] and ch.acked == [7]


def test_direct_read_refuses_buckets_not_configured(tmp_path, monkeypatch):
    handler, client, _ = direct_handler(tmp_path, monkeypatch)
    ch = FakeChannel()
    msg, raw = body(filename="someone-elses-bucket/MF1/_files/abc")
    handler(ch, Method(), None, raw)
    assert statuses(client) == ["file not found"]
    assert ch.published == [("ingestion-main-small-failed", msg)]


def test_a_mount_wins_over_direct_reads(tmp_path, monkeypatch):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch, STORAGE_BACKEND="gcs", STORAGE_BUCKETS="mf-storage-prod")

    def no_direct(*a):
        raise AssertionError("the mount should be used")

    handler.storage.exists = handler.storage.download = no_direct
    seen = {}
    monkeypatch.setattr(worker, "data_ingestion", lambda **k: (seen.update(k), ({"unique_id": "MF1"}, "I", True))[1])
    handler(FakeChannel(), Method(), None, body()[1])
    assert seen["dataset_to_process"] == f"{tmp_path}/MF1/_files/abc"
    assert statuses(client) == ["started", "complete"]


def test_a_completed_request_is_not_run_again(tmp_path, monkeypatch):
    handler, client, _ = make_handler(tmp_path, monkeypatch)
    client.ingestions.status = "complete"

    def must_not_run(**k):
        raise AssertionError("a completed request must not be parsed again")

    monkeypatch.setattr(worker, "data_ingestion", must_not_run)
    ch = FakeChannel()
    handler(ch, Method(), None, body()[1])
    assert client.ingestions.calls == []
    assert ch.acked == [7] and ch.published == []


@pytest.mark.parametrize("status", ["started", "failed", "not supported", "file not found"])
def test_other_statuses_run_again(tmp_path, monkeypatch, status):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch)
    client.ingestions.status = status
    monkeypatch.setattr(worker, "data_ingestion", lambda **k: ({"unique_id": "MF1"}, "I", True))
    handler(FakeChannel(), Method(), None, body()[1])
    assert statuses(client) == ["started", "complete"]


def test_status_lookup_failure_does_not_block_the_job(tmp_path, monkeypatch):
    (tmp_path / "MF1" / "_files").mkdir(parents=True)
    (tmp_path / "MF1" / "_files" / "abc").write_text("data")
    handler, client, _ = make_handler(tmp_path, monkeypatch)
    client.ingestions.status = requests.exceptions.ConnectionError("api down")
    monkeypatch.setattr(worker, "data_ingestion", lambda **k: ({"unique_id": "MF1"}, "I", True))
    handler(FakeChannel(), Method(), None, body()[1])
    assert statuses(client) == ["started", "complete"]
