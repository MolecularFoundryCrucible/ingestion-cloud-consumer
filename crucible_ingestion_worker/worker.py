import functools
import json
import logging
import os
import signal
import threading
import time
from importlib.metadata import version
from pathlib import Path
from typing import Callable, Optional

import requests

from crucible_ingestion import data_ingestion
from crucible_ingestion.utils import get_ingestion_githash

from .config import Config
from .storage import file_exists, resolve_path

logger = logging.getLogger(__name__)

MAX_FILE_RETRIES = 5
MAX_INGEST_RETRIES = 5


def ingestion_version() -> str:
    """The crucible-ingestion commit for a git install, else its package version."""
    return get_ingestion_githash() or version("crucible-ingestion")


class JobHandler:
    """Handles one ingestion job message; used as the RabbitMQ consumer callback.

    Message fields: filename (storage path), reqid (ingestion request), dsid
    (dataset), ingestion_class, and optionally file_name and storage_generation.
    """

    def __init__(self, config: Config, client, sleep: Callable[[float], None] = time.sleep):
        self.config = config
        self.client = client
        self.sleep = sleep
        self.version = ingestion_version()

    def _fail(self, ch, message: dict, status: str, **fields) -> None:
        self.client.ingestions.update(message["reqid"], status, **fields)
        ch.basic_publish(exchange="", routing_key=self.config.failed_queue, body=json.dumps(message))

    def __call__(self, ch, method, props, body) -> None:
        try:
            message = json.loads(body.decode("utf-8").strip())
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.error(f"Unparseable message moved to {self.config.failed_queue}: {body[:200]!r}")
            ch.basic_publish(exchange="", routing_key=self.config.failed_queue, body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        reqid = message["reqid"]
        local_path, bucket = resolve_path(message["filename"], self.config.storage_mounts)
        if local_path is None:
            logger.error(f"Unexpected filename format, cannot resolve path: {message['filename']}")
            self.client.ingestions.update(reqid, status="file not found")
            ch.basic_publish(exchange="", routing_key=self.config.failed_queue, body=json.dumps(message))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(f"received message {message} .. starting processing")
        self.client.ingestions.update(reqid, status="started", ingestion_githash=self.version)

        for attempt in range(1, MAX_FILE_RETRIES + 1):
            if file_exists(local_path, bucket, self.config.storage_mounts, self.config.storage_exists_check):
                break
            if attempt < MAX_FILE_RETRIES:
                logger.warning(f"[x] File not found, retry {attempt}/{MAX_FILE_RETRIES} for {body}")
                self.sleep(2 ** attempt)
            else:
                logger.error(f"[x] Received {body} but file not found after {MAX_FILE_RETRIES} attempts")
                self.client.ingestions.update(reqid, status="file not found")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

        ingestion_class = None
        try:
            for attempt in range(1, MAX_INGEST_RETRIES + 1):
                try:
                    ds, ingestion_class, supported = data_ingestion(dataset_to_process=local_path,
                                                                    dsid=message["dsid"],
                                                                    ingestion_class=message["ingestion_class"],
                                                                    include_file=False,
                                                                    file_name=message.get("file_name"))
                    break
                except requests.exceptions.HTTPError as err:
                    not_found = err.response is not None and err.response.status_code == 404
                    if not_found and attempt < MAX_INGEST_RETRIES:
                        logger.warning(f"[x] Dataset not yet visible, retry {attempt}/{MAX_INGEST_RETRIES}: {err}")
                        self.sleep(2 ** attempt)
                        continue
                    raise

            if not supported:
                self.client.ingestions.update(reqid, status="not supported", ingestion_githash=self.version)
                if self.config.not_supported_queue:
                    ch.basic_publish(exchange="", routing_key=self.config.not_supported_queue, body=json.dumps(message))
                logger.warning(f"[x] Received {body} and was not a supported file type - skipping")
            else:
                self.client.ingestions.update(reqid, status="complete", ingestion_githash=self.version,
                                              ingestion_class=ingestion_class)
                logger.info(f"[x] Received {body} and ingested with id: {ds['unique_id']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as err:
            logger.error(f"[x] Received {body} but failed with error {err}")
            self._fail(ch, message, "failed", ingestion_githash=self.version, ingestion_class=ingestion_class)
            ch.basic_ack(delivery_tag=method.delivery_tag)


class ThreadSafeChannel:
    """Channel calls made from a job thread, run on the connection's thread in order."""

    def __init__(self, connection, channel):
        self._connection = connection
        self._channel = channel

    def basic_publish(self, exchange, routing_key, body) -> None:
        self._connection.add_callback_threadsafe(
            functools.partial(self._channel.basic_publish, exchange=exchange, routing_key=routing_key, body=body))

    def basic_ack(self, delivery_tag) -> None:
        self._connection.add_callback_threadsafe(functools.partial(self._channel.basic_ack, delivery_tag=delivery_tag))


class Consumer:
    """Runs each job in a thread so the connection keeps serving heartbeats, and
    drains the running job on shutdown."""

    def __init__(self, connection, channel, handler: JobHandler, config: Config,
                 clock: Callable[[], float] = time.monotonic):
        self.connection = connection
        self.channel = channel
        self.handler = handler
        self.config = config
        self.clock = clock
        self.job: Optional[threading.Thread] = None
        self.stopping = False

    def on_message(self, ch, method, props, body) -> None:
        proxy = ThreadSafeChannel(self.connection, ch)
        self.job = threading.Thread(target=self._run, args=(proxy, method, props, body), name="job", daemon=True)
        self.job.start()

    def _run(self, proxy, method, props, body) -> None:
        try:
            self.handler(proxy, method, props, body)
        except Exception:
            logger.exception(f"Unhandled error; message moved to {self.config.failed_queue}")
            proxy.basic_publish(exchange="", routing_key=self.config.failed_queue, body=body)
            proxy.basic_ack(delivery_tag=method.delivery_tag)

    def _beat(self) -> None:
        if self.config.health_file:
            Path(self.config.health_file).touch()
        self.connection.call_later(self.config.health_interval, self._beat)

    def request_stop(self, *_) -> None:
        if not self.stopping:
            self.stopping = True
            logger.info("Shutdown requested; no new jobs, finishing the current one")
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)

    def run(self) -> None:
        self._beat()
        self.channel.basic_consume(queue=self.config.queue, auto_ack=False, on_message_callback=self.on_message)
        self.channel.start_consuming()
        deadline = self.clock() + self.config.shutdown_timeout
        while self.job is not None and self.job.is_alive() and self.clock() < deadline:
            self.connection.process_data_events(time_limit=1)
        self.connection.process_data_events(time_limit=0)
        if self.job is not None and self.job.is_alive():
            logger.warning(f"Job still running after {self.config.shutdown_timeout}s; it will be redelivered")
        self.connection.close()


def connect(config: Config):
    import pika

    parameters = pika.ConnectionParameters(host=config.rabbitmq_host,
                                           port=config.rabbitmq_port,
                                           virtual_host=config.rabbitmq_vhost,
                                           credentials=pika.PlainCredentials(config.rabbitmq_user,
                                                                             config.rabbitmq_password),
                                           heartbeat=config.rabbitmq_heartbeat)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    for queue in (config.queue, config.failed_queue, config.not_supported_queue):
        if queue:
            channel.queue_declare(queue=queue)
    return connection, channel


def main(env=os.environ) -> None:
    from crucible import CrucibleClient
    from crucible_ingestion import set_client

    from .config import load_config

    config = load_config(env)
    logging.getLogger().setLevel(config.log_level)
    connection, channel = connect(config)
    client = CrucibleClient(api_url=config.api_url, api_key=config.api_key)
    set_client(client)
    handler = JobHandler(config, client)
    channel.basic_qos(prefetch_count=config.prefetch_count)
    consumer = Consumer(connection, channel, handler, config)
    signal.signal(signal.SIGTERM, consumer.request_stop)
    signal.signal(signal.SIGINT, consumer.request_stop)
    logger.info(f"[*] Waiting for messages on {config.queue} (crucible-ingestion {handler.version})")
    consumer.run()
