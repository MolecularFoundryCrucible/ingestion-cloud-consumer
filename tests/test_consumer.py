import json
import threading
import time

from crucible_ingestion_worker import worker
from crucible_ingestion_worker.config import load_config

BASE = {
    "CRUCIBLE_API_URL": "u", "CRUCIBLE_API_KEY": "k", "RABBITMQ_HOST": "h", "RABBITMQ_PASSWORD": "pw",
    "QUEUE": "ingestion-main-small",
}


class Recorder:
    """A channel that records calls in the order the connection thread runs them."""

    def __init__(self):
        self.calls = []
        self.consumed = None

    def basic_publish(self, exchange, routing_key, body):
        self.calls.append(("publish", routing_key, body))

    def basic_ack(self, delivery_tag):
        self.calls.append(("ack", delivery_tag))

    def basic_consume(self, queue, auto_ack, on_message_callback):
        self.consumed = (queue, auto_ack)

    def start_consuming(self):
        pass

    def stop_consuming(self):
        self.calls.append(("stop_consuming",))


class FakeConnection:
    """Queues thread-safe callbacks and runs them only in process_data_events, like pika."""

    def __init__(self):
        self.pending, self.later = [], []
        self.lock = threading.Lock()
        self.closed = False

    def add_callback_threadsafe(self, callback):
        with self.lock:
            self.pending.append(callback)

    def process_data_events(self, time_limit=0):
        with self.lock:
            callbacks, self.pending = self.pending, []
        for callback in callbacks:
            callback()
        time.sleep(min(time_limit, 0.02))

    def call_later(self, delay, callback):
        self.later.append((delay, callback))

    def close(self):
        self.process_data_events()
        self.closed = True


class Method:
    delivery_tag = 3


def consumer_with(handler, **env):
    config = load_config(dict(BASE, **env))
    connection, channel = FakeConnection(), Recorder()
    return worker.Consumer(connection, channel, handler, config), connection, channel


def test_thread_safe_channel_defers_and_keeps_order():
    connection, channel = FakeConnection(), Recorder()
    proxy = worker.ThreadSafeChannel(connection, channel)
    proxy.basic_publish(exchange="", routing_key="q-failed", body="m")
    proxy.basic_ack(delivery_tag=9)
    assert channel.calls == [], "nothing runs until the connection thread processes events"
    connection.process_data_events()
    assert channel.calls == [("publish", "q-failed", "m"), ("ack", 9)]


def test_job_runs_in_a_thread_and_acks_through_the_connection():
    seen = {}

    def handler(ch, method, props, body):
        seen["thread"] = threading.current_thread().name
        ch.basic_ack(delivery_tag=method.delivery_tag)

    consumer, connection, channel = consumer_with(handler)
    consumer.on_message(channel, Method(), None, b"{}")
    consumer.job.join(2)
    assert seen["thread"] == "job"
    assert channel.calls == []
    connection.process_data_events()
    assert channel.calls == [("ack", 3)]


def test_unhandled_error_moves_message_to_failed_queue():
    def handler(ch, method, props, body):
        raise KeyError("reqid")

    consumer, connection, channel = consumer_with(handler)
    consumer.on_message(channel, Method(), None, b'{"no": "reqid"}')
    consumer.job.join(2)
    connection.process_data_events()
    assert channel.calls == [("publish", "ingestion-main-small-failed", b'{"no": "reqid"}'), ("ack", 3)]


def test_unparseable_message_is_moved_to_failed_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "get_ingestion_githash", lambda: "abc")
    handler = worker.JobHandler(load_config(BASE), client=None)
    channel = Recorder()
    handler(channel, Method(), None, b"\xff not json")
    assert channel.calls == [("publish", "ingestion-main-small-failed", b"\xff not json"), ("ack", 3)]


def test_heartbeat_touches_file_and_reschedules(tmp_path):
    health = tmp_path / "alive"
    consumer, connection, _ = consumer_with(lambda *a: None, HEALTH_FILE=str(health), HEALTH_INTERVAL="7")
    consumer._beat()
    assert health.exists()
    assert [delay for delay, _ in connection.later] == [7]


def test_stop_is_requested_once():
    consumer, connection, channel = consumer_with(lambda *a: None)
    consumer.request_stop()
    consumer.request_stop()
    connection.process_data_events()
    assert channel.calls == [("stop_consuming",)]


def test_shutdown_waits_for_the_running_job_then_closes(tmp_path):
    release = threading.Event()

    def handler(ch, method, props, body):
        release.wait(2)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    consumer, connection, channel = consumer_with(handler, SHUTDOWN_TIMEOUT="5", HEALTH_FILE=str(tmp_path / "a"))
    consumer.on_message(channel, Method(), None, b"{}")
    threading.Timer(0.2, release.set).start()
    consumer.run()
    assert ("ack", 3) in channel.calls
    assert connection.closed


def test_shutdown_timeout_leaves_the_job_unacked_for_redelivery(tmp_path):
    def handler(ch, method, props, body):
        time.sleep(1)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    consumer, connection, channel = consumer_with(handler, SHUTDOWN_TIMEOUT="0", HEALTH_FILE=str(tmp_path / "a"))
    consumer.on_message(channel, Method(), None, b"{}")
    consumer.run()
    assert ("ack", 3) not in channel.calls
    assert connection.closed
